import os
import json
import time
import queue
import logging
from logging import Handler
import re
import tempfile
import threading
import traceback
from copy import copy
from collections import namedtuple
from datetime import datetime
from getpass import getuser
from socket import gethostname

import parsl
from parsl.app.app import python_app, bash_app
from parsl.dataflow.error import DependencyError
from parsl.app.errors import AppFailure, MissingOutputs, ParslError
from ipyparallel.error import RemoteError
import networkx as nx

from operon._util.logging import setup_logger
from operon._util.home import OperonState
from operon._util.configs import cycle_config_input_options, built_in_configs
from operon._util.apps import _DeferredApp, _ParslAppBlueprint
from operon._util.errors import MalformedPipelineError, NoParslConfigurationError
from operon.meta import Meta

SOURCE = 0
TARGET = 1
EXIT_ERROR = 1
PYTHON_APP = 0
BASH_APP = 1
logger = logging.getLogger('operon.main')

# import parsl
# parsl.set_stream_logger()


class CondaPackage(namedtuple('CondaPackage', 'tag config_key executable_path')):
    """
    Tuple provided to the ``packages`` key of a pipeline's ``conda()`` method.

    .. code-block:: python

        CondaPackage(tag, config_key, executable_path=None)

    """
    def __new__(cls, tag, config_key, executable_path=None):
        return super().__new__(cls, tag, config_key, executable_path)


class Software(_ParslAppBlueprint):
    """
    An abstraction of an executable program external to the pipeline.

    :param name: str Name of the Software
    :param path: str Path to executable for the Software
    :param subprogram: str Subprogram to be appended to the execution call
    :param success_on: list<str> List of exit codes as strings to be considered success
    """
    _id = 0
    _software_paths = set()
    _pipeline_config = None

    def __init__(self, name, path=None, subprogram='', success_on=None, meta=None):
        self.name = name
        if path is None:
            try:
                path = Software._pipeline_config[name]['path']
            except Exception:
                raise ValueError('Software path could not be inferred')
        self.path = ' '.join((path, str(subprogram))) if subprogram else path
        self.basename = os.path.basename(path).replace(' ', '_')
        self.success_on = success_on or ['0']
        self.default_meta = meta or dict()

        # Add path to class collection of software paths
        Software._software_paths.add(self.path)

    def register(self, *args, **kwargs):
        """
        Registers a run of this program in the Parsl workflow, using the given
        ``Parameter``, ``Redirect``, and ``Pipe`` components.

        In addition to an arbitrary number of positional arguments, takes the following keyword arguments:

        * ``extra_inputs`` - list of ``Data`` to be considered input, in addition to any provided
          in a ``Parameter`` or ``Redirect``
        * ``extra_outputs`` - list of ``Data`` to be considered output, in addition to any provided
          in a ``Parameter`` or ``Redirect``
        * ``wait_on`` - list of Apps to be considered input; should be a list of ``_DeferredApp`` objects,
          which are returned by ``Software.register()`` and ``CodeBlock.register()``
        * ``action`` - a string giving a short but more descriptive name to this run of the software, for use
          in the log file

        :return: ``_DeferredApp`` which can be passed to other Apps
        """
        blueprint = self.prep(*args, **kwargs)
        _ParslAppBlueprint._blueprints[blueprint['id']] = blueprint
        cmd = '{cmd}{stdout_redirect}{stderr_redirect}'.format(
            cmd=blueprint['cmd'],
            stdout_redirect=' > {}'.format(blueprint['stdout']) if blueprint['stdout'] else '',
            stderr_redirect=' 2> {}'.format(blueprint['stderr']) if blueprint['stderr'] else ''
        )
        logger.debug('Registered {} as {}\nCommand: {}'.format(blueprint['name'], blueprint['id'], cmd))
        return _DeferredApp(blueprint['id'])

    def run(self, *args, **kwargs):
        self.register(*args, **kwargs)

    def prep(self, *args, **kwargs):
        """
        Does most of the work for ``register()``, but this method should only be used directly
        inside of a ``Pipe`` object.
        """
        """
        The meta dictionary:
        {
            'resources': {
                'cpu': <Number of CPUs>,
                'mem': <Amount of memory>
            },
            'site': <Name of the executor to run this app, for backward compatibility>,
            'executor': <Name of the executor to run this app>
        }
        """
        app_blueprint = {
            'id': '{}_{}'.format(self.basename, _ParslAppBlueprint.get_id()),
            'type': 'bash',
            'name': kwargs.get('action', self.path),
            'cmd': '',
            'success_on': self.success_on,
            'meta': dict(),
            'inputs': list(),
            'outputs': list(),
            'wait_on': list(),
            'stdout': None,
            'stderr': None
        }
        cmd = [self.path]

        # Add software dependencies, will be _DeferredApp objects
        if kwargs.get('wait_on'):
            app_blueprint['wait_on'].extend(map(str, kwargs.get('wait_on')))

        # Add extra inputs and extra outputs
        if kwargs.get('extra_inputs'):
            app_blueprint['inputs'].extend(map(str, kwargs.get('extra_inputs')))
        if kwargs.get('extra_outputs'):
            app_blueprint['outputs'].extend(map(str, kwargs.get('extra_outputs')))

        # Get paramters, redirects, and pipes
        cmd_parts = {
            'Parameter': [para for para in args if isinstance(para, Parameter)],
            'Redirect': [redir for redir in args if isinstance(redir, Redirect)],
            'Pipe': [pipe for pipe in args if isinstance(pipe, Pipe)]
        }

        # If there is more than 2 redirects or 1 pipe, ignore extras
        if len(cmd_parts['Redirect']) > 2:
            cmd_parts['Redirect'] = cmd_parts['Redirect'][:2]
        if len(cmd_parts['Pipe']) >= 1:
            cmd_parts['Pipe'] = cmd_parts['Pipe'][0]

        # Deal with Parameters
        for parameter in cmd_parts['Parameter']:
            cmd.append(str(parameter))
            for data in parameter.data:
                # Default to data being OUTPUT if none specified
                if data.mode == Data.INPUT:
                    app_blueprint['inputs'].append(str(data))
                else:
                    app_blueprint['outputs'].append(str(data))

        # Deal with Redirects
        for redirect in cmd_parts['Redirect']:
            if redirect.stream in Redirect._BOTH_MODES:
                app_blueprint['stdout'] = app_blueprint['stderr'] = str(redirect.dest)
                break
            if redirect.stream in Redirect._STDOUT_MODES:
                app_blueprint['stdout'] = str(redirect.dest)
            elif redirect.stream in Redirect._STDERR_MODES:
                app_blueprint['stderr'] = str(redirect.dest)
            if isinstance(redirect.dest, Data):
                app_blueprint['outputs'].append(str(redirect.dest))

        # Deal with a Pipe, if it exists
        if cmd_parts['Pipe']:
            # Preserve the previous redirect to stderr, if it exists
            if app_blueprint['stderr'] is not None:
                cmd.append('2> {}'.format(app_blueprint['stderr']))

            # Append the piped software
            pipe_blueprint = cmd_parts['Pipe'].piped_software_blueprint
            cmd.extend(['|', pipe_blueprint['cmd']])
            app_blueprint['inputs'].extend(pipe_blueprint['inputs'])
            app_blueprint['outputs'].extend(pipe_blueprint['outputs'])
            app_blueprint['stdout'] = pipe_blueprint['stdout']
            app_blueprint['stderr'] = pipe_blueprint['stderr']

        app_blueprint['cmd'] = ' '.join(cmd)

        # If either of stdout or stderr were not explicitly set by a Redirect,
        # set it to go to a temporary file for later injection into the main logs
        if not app_blueprint['stdout']:
            app_blueprint['stdout'] = os.path.join(
                ParslPipeline._pipeline_run_temp_dir.name,
                '{}.stdout'.format(app_blueprint['id'])
            )
        if not app_blueprint['stderr']:
            app_blueprint['stderr'] = os.path.join(
                ParslPipeline._pipeline_run_temp_dir.name,
                '{}.stderr'.format(app_blueprint['id'])
            )

        # Store resource meta
        app_blueprint['meta'] = kwargs.get('meta') or self.default_meta

        # print('Created app blueprint: {}'.format(app_blueprint))
        return app_blueprint


class Data(object):
    """
    Representation of a file on the filesystem that should be considered input to or output
    from some program in the pipeline. Multiple instantiations of the same file
    path will return the same ``Data`` object.

    ``Data`` objects should be used in place of a string filepath when passing ``Parameter`` or
    ``Redirect`` to a ``Software``. Alternatively, a list of them can be passed to a ``Software`` or
    ``CodeBlock`` in their ``extra_inputs=`` or ``extra_outputs=`` keyword arguments.

    :param path: str Path to the file on the filesystem
    """
    _id = 0
    _data = dict()

    INPUT = 0
    OUTPUT = 1

    def __new__(cls, path):
        if not path:
            return ''
        if path in cls._data:
            return cls._data[path]
        return super(Data, cls).__new__(cls)

    def __init__(self, path):
        if path not in Data._data:
            # Initialize
            Data._data[path] = self
            self.path = path
            self.tmp = None
            self.mode = None
            self._initial_input = False
            self._terminal_output = False

    def as_input(self):
        """
        Marks this ``Data`` object as input
        """
        self.mode = Data.INPUT
        return self

    def as_output(self, tmp=False):
        """
        Marks this ``Data`` object as output

        :param tmp: bool If ``True``, this file will be deleted when the pipeline completes
        """
        self.mode = Data.OUTPUT
        self.tmp = tmp
        return self

    def __str__(self):
        return self.path

    def __unicode__(self):
        return self.__str__()


class DataBlob(object):
    """
    When the exact filename or filenames of program output is unknown,
    this can be used in its place to find file outputs using a Unix blob
    TODO This is a future feature
    """


class Parameter(object):
    """
    Abstraction of a command line parameter into a program.

    :param *args: str All string arguments are joined together with spaces when parsed
                      by a ``Software``
    :param sep: str Separator between parameter tokens
    """
    def __init__(self, *args, sep=' '):
        self.parameters = args
        self.sep = sep
        self.data = [d for d in args if isinstance(d, Data)]

    def __str__(self):
        return self.sep.join(map(str, self.parameters))


class Redirect(object):
    """
    Abstraction of a command line redirect to a file.

    :param stream: str|enum The stream to redirect
    :param dest: str The filepath to dump the redirection

    The following class constants exist for use with ``stream=``:

    * STDOUT
    * STDERR
    * BOTH
    * STDOUT_APPEND
    * STDERR_APPEND
    * BOTH_APPEND
    """
    STDOUT = 0
    STDERR = 1
    BOTH = 2
    # STDOUT_APPEND = 3
    # STDERR_APPEND = 4
    # BOTH_APPEND = 5
    NULL = os.devnull
    # Remove the ability to use append mode until Parsl supports it
    _STDOUT_MODES = {STDOUT}
    _STDERR_MODES = {STDERR}
    _BOTH_MODES = {BOTH}
    _APPEND_MODES = set()
    # _APPEND_MODES = {STDOUT_APPEND, STDERR_APPEND, BOTH_APPEND}
    # _STDOUT_MODES = {STDOUT, STDOUT_APPEND}
    # _STDERR_MODES = {STDERR, STDERR_APPEND}
    # _BOTH_MODES = {BOTH, BOTH_APPEND}

    _convert = {
        '>': STDOUT,
        '1>': STDOUT,
        # '>>': STDOUT_APPEND,
        # '1>>': STDOUT_APPEND,
        '2>': STDERR,
        # '2>>': STDERR_APPEND,
        '&>': BOTH,
        # '&>>': BOTH_APPEND
    }

    def __init__(self, stream=STDOUT, dest='out.txt'):
        if isinstance(stream, str):
            stream = Redirect.token_convert(str(stream).strip())

        self.stream = stream
        self.dest = dest
        self.mode = 'a' if stream in Redirect._APPEND_MODES else 'w'

    def __str__(self):
        return ''.join([Redirect.token_convert(self.stream), str(self.dest)])

    @staticmethod
    def token_convert(token):
        if type(token) == str:
            return Redirect._convert[token]
        elif type(token) == int:
            reverse_convert = {v: k for k, v in Redirect._convert.items()}
            return reverse_convert[token]
        return Redirect.STDOUT


class Pipe(object):
    """
    Abstracts piping the command line concept of using the output of one program as the
    input into another.

    :param piped_software_blueprint: the returned value of ``Software.prep()``
    """
    def __init__(self, piped_software_blueprint):
        self.piped_software_blueprint = piped_software_blueprint

    def __str__(self):
        return str(self.piped_software_blueprint)


class CodeBlock(_ParslAppBlueprint):
    """
    Represents a block of Python code to be run as a unit of execution in the workflow.
    """
    @staticmethod
    def register(func, args=None, kwargs=None, inputs=None,
                 outputs=None, wait_on=None, stdout=None, stderr=None, **kwargs_):
        """
        Registers a run of the function ``func`` in the Parsl workflow.

        :param func: function Reference to the function to be executed
        :param args: iterable The positional arguments into the function
        :param kwargs: dict The keyword arguments into the function
        :param inputs: list<``Data``> The input dependencies
        :param outputs: list<``Data``> The output files produced
        :param wait_on: list<``_DeferredApp``> Other software input dependencies
        :param stdout: str Path to file to store stdout stream
        :param stderr: str Path to file to store stderr stream
        :return: ``_DeferredApp`` representation of the value this function will eventually return
        """
        blueprint_id = '{}_{}'.format(func.__name__, _ParslAppBlueprint.get_id())
        _ParslAppBlueprint._blueprints[blueprint_id] = {
            'id': blueprint_id,
            'type': 'python',
            'func': func,
            'args': args if args else list(),
            'kwargs': kwargs if kwargs else dict(),
            'inputs': list(map(str, inputs)) if inputs else list(),
            'outputs': list(map(str, outputs)) if outputs else list(),
            'wait_on': list(map(str, wait_on)) if wait_on else list(),
            'stdout': stdout,
            'stderr': stderr,
            'meta': kwargs_.get('meta', dict())
        }
        logger.debug('Registered function {}\nArgs: {}\nKwargs: {}'.format(
            func.__name__,
            args,
            kwargs
        ))
        return _DeferredApp(blueprint_id)


class DataflowResponseHandler(Handler):
    def __init__(self, pipeline_futs, *args, **kwargs):
        self.task_map = {
            str(fut.tid): [name, fut]
            for name, fut in pipeline_futs
        }

        self.pending, self.running, self.finished = set(list(self.task_map.keys())), set(), set()

        super().__init__(*args, **kwargs)

    def handle(self, record):
        msg = record.getMessage()
        print('Got message: {}'.format(msg))

        task_started = re.match(r'Task (\d+) launched on executor', msg)
        if task_started is not None:
            task_id = task_started.group(1)
            # print(self.task_map[task_id][1].parent)
            parent_ = self.task_map['2'][1].parent
            if parent_ is not None:
                print('\t\t' + str(parent_.__dict__))
            else:
                print('\t\tNo Parent')


class ParslPipeline(object):
    """
    ParslPipeline forms the basis for a Pipeline class. This class sets up workflow digraph construction,
    stream capturing, and registration of apps and dependencies with Parsl.
    """
    # Temporary directory to send stream output of un-Redirected apps
    _pipeline_run_temp_dir = None

    def _run(self, pipeline_args, pipeline_config, original_command, run_args=None):
        """
        If run_args is not None, then this is a batch run because single runs won't
        populate run_args.

        :param pipeline_args:
        :param pipeline_config:
        :param original_command:
        :param run_args:
        :return:
        """
        # Ensure the pipeline() method is overridden
        if 'pipeline' not in vars(self.__class__):
            raise MalformedPipelineError('Pipeline has no method pipeline()')

        # Set up logging
        logs_dir = (run_args or pipeline_args).get('logs_dir')
        run_name = (run_args or pipeline_args).get('run_name')
        os.makedirs(logs_dir, exist_ok=True)
        setup_logger(logs_dir, run_name)

        # Set up temp dir
        ParslPipeline._pipeline_run_temp_dir = tempfile.TemporaryDirectory(
            dir=logs_dir,
            suffix='__operon'
        )

        # Log initial run conditions
        logger.info(f'Executing: operon {original_command}')
        logger.info(f'Who and where: {getuser()}@{gethostname()}:{os.getcwd()}')
        if run_name != 'run':
            logger.info(f'Run name: {run_name}')

        # Respond to events in the Dataflow logging
        # logging.getLogger('parsl.dataflow.dflow').addHandler(DataflowResponseHandler())

        # Give pipeline config to Software class
        Software._pipeline_config = copy(pipeline_config)

        # Run self.pipeline() to assemble workflow graph
        if run_args is None:
            self.pipeline(pipeline_args, pipeline_config)
        else:
            for single_pipeline_args in pipeline_args:
                self.pipeline(single_pipeline_args, pipeline_config)

        # Hand the run over to Parsl and monitor for completion
        ParslPipeline._start_and_monitor_run(
            workflow_graph=ParslPipeline._assemble_graph(_ParslAppBlueprint._blueprints.values()),
            parsl_config=ParslPipeline._choose_parsl_config(
                pipeline_args_parsl_config=(run_args or pipeline_args).get('parsl_config'),
                pipeline_config_parsl_config=pipeline_config.get('parsl_config'),
                pipeline_default_parsl_config=self.parsl_configuration()
            )
        )

    @staticmethod
    def _start_and_monitor_run(workflow_graph, parsl_config):
        # Register apps and data with Parsl, get all app futures and temporary files
        pipeline_futs, tmp_files = ParslPipeline._register_workflow(workflow_graph, parsl_config)

        state = {name: 'pending' for name, fut in pipeline_futs}

        # Record start time
        start_time = datetime.now()
        logger.info('Started pipeline run\n@operon_start {}'.format(str(start_time)))

        # Thread to listen for when apps start and stop
        def running_listener(q, pipeline_futs):
            fut_map = {fut: name for name, fut in pipeline_futs}
            pending, running, finished = set(list(fut_map.keys())), set(), set()
            while True:
                # The only thing that will be sent is 'kill'
                if not q.empty():
                    break
                time.sleep(0.01)

                # Copy running set, to see if anything changes
                precheck_running = copy(running)

                # Identify finished futures
                for running_fut in running:
                    # ready is for IPP, done is for threads (I don't know about other executors)
                    finished_func = 'ready' if hasattr(running_fut.parent, 'ready') else 'done'
                    if getattr(running_fut.parent, finished_func)():
                        logger.info('{} finished running'.format(fut_map[running_fut]))
                        finished.add(running_fut)
                running -= finished

                # Identify newly running futures
                for pending_fut in pending:
                    if pending_fut.parent is not None:
                        logger.info('{} staged to run'.format(fut_map[pending_fut]))
                        running.add(pending_fut)
                pending -= running

                # Check if anything changed this iteration
                if precheck_running != running and running:
                    logger.info('Staged or running: {}'.format(
                        '  '.join([fut_map[f] for f in running])
                    ))

        # Setup and start running listener thread
        running_listener_q = queue.Queue()
        running_listener_thread = threading.Thread(target=running_listener, args=(running_listener_q, pipeline_futs))
        running_listener_thread.start()

        # Wait for all apps to complete
        for name, fut in pipeline_futs:
            fut_errored = True
            try:
                fut.result()
            except AppFailure as e:
                logger.info('{} failed during execution'.format(name))
                logger.debug(e.reason)
                logger.info('Check run log for output from failed {}'.format(name))
            except DependencyError as e:
                logger.info('{} had a dependency fail'.format(name))
                logger.debug(str(e))
            except MissingOutputs as e:
                logger.info('{} did not produce expected outputs\n{}'.format(name, e))
            except ParslError as e:
                logger.info('{} produced a general Parsl error\n{}'.format(name, e))
            except KeyboardInterrupt:
                logger.info('User aborted run')
                break
            except RemoteError as e:
                logger.info('{} produced a RemoteError\n{}'.format(name, e.traceback))
            except Exception as e:
                logger.info('{} produced a general error\n{}'.format(name, traceback.format_exc()))
            else:
                fut_errored = False
            finally:
                state[name] = 'failed' if fut_errored else 'completed'

        # All apps are complete, so kill running listener thread
        running_listener_q.put('kill')
        running_listener_thread.join()

        # All apps are complete, so run cleanup
        if tmp_files and OperonState().setting('delete_temporary_files') == 'yes':
            for tmp_file_path in tmp_files:
                try:
                    os.remove(tmp_file_path)
                except Exception:
                    pass  # If a file can't be deleted, just leave it and move on

        # Record end time and elapsed time
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logger.info('Finished pipeline run\n@operon_end {}\n@operon_elapsed {}\n@operon_elapsed_seconds {}'.format(
            str(end_time),
            str(elapsed_time),
            str(elapsed_time.seconds)
        ))

        # Log any failures
        failures = [name for name, state_ in state.items() if state_ == 'failed']
        pendings = [name for name, state_ in state.items() if state_ == 'pending']
        logger.info('Failed apps: {}'.format(' '.join(failures) if failures else 'None'))
        logger.info('Apps never ran: {}'.format(' '.join(pendings) if pendings else 'None'))

        # Remove stream handler before outputting captured streams
        logger.handlers.pop(1)

        # Inject captured app stdout and stderr into logs
        for captured_output in os.listdir(ParslPipeline._pipeline_run_temp_dir.name):
            capture_output_path = os.path.join(ParslPipeline._pipeline_run_temp_dir.name, captured_output)
            app_name, stream = os.path.splitext(captured_output)
            try:
                captured_output_content = open(capture_output_path).read()
                if captured_output_content:
                    logger.debug('Output from {stream} stream of {app_name}:\n{msg}'.format(
                        stream=stream[1:],  # Get extension of captured stream, without period
                        app_name=app_name,
                        msg=captured_output_content
                    ))
            except FileNotFoundError:
                logger.debug('Output from {stream} of {app_name} could not be retrieved'.format(
                    stream=stream[1:],
                    app_name=app_name
                ))

    @staticmethod
    def _choose_parsl_config(pipeline_args_parsl_config, pipeline_config_parsl_config, pipeline_default_parsl_config):
        """
        Given the complete set of possible user inputs, this selects the first valid config in the
        Parsl config hierarchy:

            * Command line at runtime
            * Pipeline configuration
            * Installation default, in ``OPERON_HOME``
            * Pipeline default
            * Built-in 2 basic threads

        :param pipeline_args_parsl_config:
        :param pipeline_config_parsl_config:
        :param pipeline_default_parsl_config:
        :return: (str, str) first element is either 'builtin' or 'json', second is the configuration as the
                 builtin key or a raw json string
        """
        # 1) Config defined at runtime on the command line
        if pipeline_args_parsl_config is not None:
            logger.info(f'Attempting to load {pipeline_args_parsl_config}')
            # loaded_config is (str, str): (parsl config type, parl_config_value)
            loaded_config = cycle_config_input_options(pipeline_args_parsl_config)
            if loaded_config is not None:
                logger.info('Loaded Parsl config from command line arguments')
                return loaded_config

        # 2) Config defined for this pipeline in the pipeline configuration
        if pipeline_config_parsl_config:
            logger.info(f'Attempting to load {pipeline_config_parsl_config}')
            loaded_config = cycle_config_input_options(pipeline_config_parsl_config)
            if loaded_config is not None:
                logger.info('Loaded Parsl config from pipeline config')
                return loaded_config

        # 4) Config defined by the pipeline developer as a default, if no user config exists
        if pipeline_default_parsl_config:
            logger.info('Loaded Parsl config from pipeline default')
            return pipeline_default_parsl_config
            # except ValueError:
            #     pass  # Silently fail, move on to next option

        # 5) Config used if all above are absent, run as a Thread Pool with 2 workers
        if OperonState().setting('no_parsl_config_behavior') == 'use_package_default':
            logger.info('Loaded Parsl config using package default (2 basic threads)')
            return built_in_configs['basic-threads-2']()
        raise NoParslConfigurationError('Operon global settings requested immediate failure')


    @staticmethod
    def _generate_executor_app_factories(executor_name=None):
        executors_ = 'all' if executor_name is None else [executor_name]

        @python_app(executors=executors_, cache=True)
        def _pythonapp(func_, func_args, func_kwargs, **kwargs):
            return func_(*func_args, **func_kwargs)

        @bash_app(executors=executors_, cache=True)
        def _bashapp(cmd, success_on=None, **kwargs):
            return ('scodes=({exit_codes});{cmd};ecode=$?;for i in "${{{{scodes[@]}}}}";'
                    'do if [ "$i" = $ecode ];then exit 0;fi;done;exit 1').format(
                exit_codes=' '.join(map(str, success_on or ['0'])),
                cmd=cmd
            )

        return _pythonapp, _bashapp

    @staticmethod
    def _register_workflow(workflow_graph, parsl_config):
        """
        For right now we will keep track of all unique combinations of resource requirements and
        how many of each. The maxBlocks can then be set to the number of each resource requirement. In the
        future we can try to be smarter by examining the workflow graph and deciding how many could
        possibly ever be running concurrently.

        Pipeline is single, Config is single
            * Assign all Apps to null executor
            * @App('python', dfk)  <-- No executor=

        Pipeline is single, Config is multi
            * Assign all Apps to null executor (will be randomly assigned among configured executors)
            * Log warning of mismatch

        Pipeline is multi, Config is single
            * Assign all Apps to null executor
            * Log warning of mismatch

        Pipeline is multi, Config is multi, perfect match
            * Assign all Apps to their appropriate executor

        Pipeline is multi, Config is multi, some match
            * Assign apps that can to their appropriate executors
            * For the remaining, assign to first executor
            * Log warning of mismatch

        Pipeline is multi, Config is multi, no matches
            * Assign all apps to first executor
            * Log warning of mismatch

        :param workflow_graph:
        :param dfk:
        :return:
        """
        # Regiser config with Parsl
        parsl.load(parsl_config)

        is_single_parsl_config = len(parsl_config.executors) <= 1

        # Check to see if Pipeline is single or multi
        pipeline_executors = set(Meta._executors.keys())  # Start with anything defined in Meta
        is_single_pipeline_meta = len(pipeline_executors) <= 1

        logger.debug('Pipeline is {}-{}'.format(
            'single' if is_single_pipeline_meta else 'multi',
            'single' if is_single_parsl_config else 'multi',
        ))

        app_factories = dict()
        # At a minimum define the 'all' executor, which is an executor with no specific label
        app_factories['all'] = ParslPipeline._generate_executor_app_factories()

        # If we have multiple executors, define them
        if not any((is_single_parsl_config, is_single_pipeline_meta)):
            for executor in parsl_config.executors:
                app_factories[executor.label] = ParslPipeline._generate_executor_app_factories(executor_name=executor.label)

        # Some data containers
        app_futures, data_futures = list(), dict()
        app_nodes_registered = {
            node_id: False
            for node_id in workflow_graph
            if workflow_graph.node[node_id]['type'] == 'app'
        }
        data_node_in_degree = {
            node_id: in_degree
            for node_id, in_degree in workflow_graph.in_degree(workflow_graph.nodes())
            if workflow_graph.node[node_id]['type'] == 'data'
        }

        def register_app(app_node_id, workflow_graph):
            """
            Recursive algorithm to traverse the workflow graph and register apps
            :param app_node_id: str ID of the app node to try to register with parsl
            :param workflow_graph: nx.DiGraph Directed graph representation of the workflow
            :return: list<AppFuture> All app futures generated by the workflow
            """
            # Check if any input data nodes don't have data futures and have in-degree > 0
            for input_dependency_node in workflow_graph.predecessors(app_node_id):
                if workflow_graph.nodes[input_dependency_node]['type'] == 'data':
                    if input_dependency_node not in data_futures and data_node_in_degree[input_dependency_node] > 0:
                        register_app(list(workflow_graph.predecessors(input_dependency_node))[0], workflow_graph)
                elif workflow_graph.nodes[input_dependency_node]['type'] == 'app':
                    if not app_nodes_registered[input_dependency_node]:
                        register_app(input_dependency_node, workflow_graph)

            # Register this app
            _app_blueprint = workflow_graph.node[app_node_id]['blueprint']
            _app_inputs = [
                data_futures.get(input_data)
                for input_data in _app_blueprint['inputs']
                if data_futures.get(input_data)
            ]

            # If there are any app dependencies, add them
            if _app_blueprint['wait_on']:
                _app_inputs.extend([
                    app_nodes_registered.get(wait_on_app_id)
                    for wait_on_app_id in _app_blueprint['wait_on']
                    if app_nodes_registered.get(wait_on_app_id)
                ])

            # Select executor to run this app on
            executor_assignment = 'all'
            if not any((is_single_parsl_config, is_single_pipeline_meta)):
                # This is a multi-multi run, we might be able to assign to a executor
                # Giving a name defined in Meta takes precedence
                meta_executor = _app_blueprint.get('meta', {}).get('executor')

                # For backward compatibility with 'site' key
                if meta_executor is None and 'site' in _app_blueprint.get('meta', {}):
                    meta_executor = _app_blueprint.get('meta', {}).get('site')

                if meta_executor is not None and meta_executor in app_factories:
                    executor_assignment = meta_executor
                elif Meta._default_executor is not None and Meta._default_executor in app_factories:
                    executor_assignment = Meta._default_executor

            # Create the App future with a specific executor App factory
            if _app_blueprint['type'] == 'bash':
                _app_future = app_factories[executor_assignment][BASH_APP](
                    cmd=_app_blueprint['cmd'],
                    success_on=_app_blueprint['success_on'],
                    inputs=_app_inputs,
                    outputs=_app_blueprint['outputs'],
                    stdout=_app_blueprint['stdout'],
                    stderr=_app_blueprint['stderr']
                )
            else:
                _app_future = app_factories[executor_assignment][PYTHON_APP](
                    func_=_app_blueprint['func'],
                    func_args=_app_blueprint['args'],
                    func_kwargs=_app_blueprint['kwargs'],
                    inputs=_app_inputs,
                    outputs=_app_blueprint['outputs'],
                    stdout=_app_blueprint['stdout'],
                    stderr=_app_blueprint['stderr']
                )

            logger.info('{} assigned to executor {}, task id {}'.format(_app_blueprint['id'], executor_assignment, _app_future.tid))
            app_futures.append((_app_blueprint['id'], _app_future))

            # Set output data futures
            for data_fut in _app_future.outputs:
                if data_fut.filename not in data_futures:
                    data_futures[data_fut.filename] = data_fut

            app_nodes_registered[app_node_id] = _app_future

        # Register all apps
        for app_node in app_nodes_registered:
            if not app_nodes_registered[app_node]:
                register_app(app_node, workflow_graph)

        # Gather files marked as temporary, if any
        tmp_files = [d for d in data_futures if Data(d).tmp]

        return app_futures, tmp_files

    @staticmethod
    def _assemble_graph(blueprints):
        # Initialize a directed graph
        digraph = nx.DiGraph()

        # Iterate through edges, and add nodes as necessary
        for blueprint in blueprints:
            # Add software node
            app_id = blueprint['id']
            app_name = blueprint['name'] if blueprint['type'] == 'bash' else app_id
            digraph.add_node(app_id, name=app_name, type='app', blueprint=blueprint)

            # Register inputs, outputs, and wait_on
            for blp_input in blueprint['inputs']:
                digraph.add_node(blp_input, name=blp_input, type='data')
                digraph.add_edge(blp_input, app_id)

            for blp_output in blueprint['outputs']:
                digraph.add_node(blp_output, name=blp_output, type='data')
                digraph.add_edge(app_id, blp_output)

            for blp_wait_on in blueprint['wait_on']:
                digraph.add_edge(blp_wait_on, app_id)

        # Output graph in JSON format
        json_digraph = {'nodes': list(), 'edges': list()}
        for node, nodedata in digraph.nodes.items():
            json_digraph['nodes'].append(
                {'data': {'id': os.path.basename(node), 'type': nodedata['type'], 'haveblueprint': bool(nodedata.get('blueprint'))}}
            )
        for edge, edgedata in digraph.edges.items():
            json_digraph['edges'].append(
                {'data': {'source': os.path.basename(edge[SOURCE]), 'target': os.path.basename(edge[TARGET])}}
            )
        # print(json.dumps(json_digraph, indent=2))
        # TODO Find a way to output this to the user, maybe in the logs directory

        return digraph

    def sites(self):
        """
        Kept around for backward compatibility.
        """
        return self.executors()

    def executors(self):
        """
        Override this method.

        A dictionary which describes the executors this pipeline is intended to run on.

        Example:
        {
            'executor1': {
                'resources': {
                    'cpu': Meta.dynamic(),
                    'mem': '4G',
                    'storage': '400G'
                },
                'description': ''
            }
        }
        """
        return dict()

    def parsl_configuration(self):
        """
        Override this method.

        If provided, will be used as a low-precendence default to run pipelines. The user is given every
        opportunity to provide his/her own Parsl configuration, but if absolutely none is given this
        configuration will be used.

        This should be used with caution because it potentially reduces portability.

        :return: dict Parsl configuration as a low-precedence default for this pipeline
        """
        return None

    def description(self):
        """
        Override this method.

        A string describing this pipeline.

        :return: str A description of this pipeline
        """
        return ''

    def dependencies(self):
        """
        Override this method.

        A list of pip style dependency tags for this pipeline.

        :return: list A list of pip style dependencies
        """
        return list()

    def conda(self):
        """
        Override this method.

        Directives to download software from conda/bioconda.
        The returned dictionary should have a key ``packages`` and an
        optional key ``channels``.

        * ``packages`` should be a list of ``operon.components.CondaPackage`` tuples
        * ``channels`` should be list of conda channels to temporarily access for installation

        :return: dict A dict configuring conda packages
        """
        return dict()

    def arguments(self, parser):
        """
        Override this method.

        Adds arguments to this pipeline using the ``argparse.add_argument()`` method. The parser
        argument is an ``argparse.ArgumentParser()`` object.

        :param parser: ``argparse.ArgumentParser`` object
        """
        pass

    def configuration(self):
        """
        Override this method.

        Dictionary representation of the JSON object that will be used to configure this
        pipeline. Configuration variables should be values that will change from platform
        to platform but not run to run, ex. paths to software, but likely not parameters to
        that software.

        Keys will be returned as is, but terminal string values will become input values from
        the user when he/she calls ``operon configure``.

        In the pipeline, this dictionary will be used to populate ``pipeline_config``, with
        terminal string values replaced with user input values.

        :return: dict Dictionary representation of config values
        """
        return dict()

    def pipeline(self, pipeline_args, pipeline_config):
        """
        Override this method.

        The logic of the pipeline will be here. The arguments are automatically populated with the
        user arguments to the pipeline (those added with the method ``self.arguments()``) as well as
        the configuration for the pipeline (as a dictionary of the form returned by the method ``self.configuration()``,
        but with user input values in place of terminal strings.)

        :param pipeline_args: dict Populated dictionary of user arguments
        :param pipeline_config: dict Populated dictionary of pipeline configuration
        """
        raise NotImplementedError

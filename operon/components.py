import os
import sys
import json
import logging
import tempfile
from copy import copy
from collections import namedtuple

from parsl import App
import networkx as nx

from operon._util import setup_logger
from operon._util.home import get_operon_home
from operon._util.configs import dfk_with_config, direct_config, cycle_config_input_options
from operon._util.apps import _DeferredApp, _ParslAppBlueprint

SOURCE = 0
TARGET = 1
EXIT_ERROR = 1
logger = logging.getLogger('operon.main')


class CondaPackage(namedtuple('CondaPackage', 'tag config_key executable_path')):
    def __new__(cls, tag, config_key, executable_path=None):
        return super().__new__(cls, tag, config_key, executable_path)


class Software(_ParslAppBlueprint):
    _id = 0
    _software_paths = set()

    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.basename = os.path.basename(path).replace(' ', '_')

        # Add path to class collection of software paths
        Software._software_paths.add(self.path)

    def register(self, *args, **kwargs):
        """
        Registers a Software run as a parsl App
        :param args:
        :param kwargs:
        :return:
        """
        blueprint = self.prep(*args, **kwargs)
        _ParslAppBlueprint._blueprints[blueprint['id']] = blueprint
        return _DeferredApp(blueprint['id'])

    def run(self, *args, **kwargs):
        """
        Alias for self.register(), for inter-run-mode compatibility
        """
        self.register(*args, **kwargs)

    def prep(self, *args, **kwargs):
        app_blueprint = {
            'id': '{}_{}'.format(self.basename, _ParslAppBlueprint.get_id()),
            'type': 'bash',
            'name': kwargs.get('action', self.path),
            'cmd': '',
            'inputs': list(),
            'outputs': list(),
            'stdout': None,
            'stderr': None
        }
        cmd = [self.path]

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
            if redirect.stream in Redirect._STDOUT_MODES:
                app_blueprint['stdout'] = str(redirect.dest)
            elif redirect.stream in Redirect._STDERR_MODES:
                app_blueprint['stderr'] = str(redirect.dest)
            if isinstance(redirect.dest, Data):
                app_blueprint['outputs'].append(str(redirect.dest))

        # Deal with a Pipe, if it exists
        if cmd_parts['Pipe']:
            pipe_blueprint = cmd_parts['Pipe'].piped_software_blueprint
            cmd.extend(['|', pipe_blueprint['cmd']])
            app_blueprint['inputs'].extend(pipe_blueprint['inputs'])
            app_blueprint['outputs'].extend(pipe_blueprint['outputs'])
            # TODO stdout and stderr

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

        # print('Created app blueprint: {}'.format(app_blueprint))
        return app_blueprint


class Data(object):
    _id = 0
    _data = dict()

    INPUT = 0
    OUTPUT = 1

    def __new__(cls, path, tmp=False):
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
        self.mode = Data.INPUT
        return self

    def as_output(self, tmp=False):
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
    def __init__(self, *args, sep=' '):
        self.parameters = args
        self.sep = sep
        self.data = [d for d in args if isinstance(d, Data)]

    def __str__(self):
        return self.sep.join(map(str, self.parameters))


class Redirect(object):
    """
    The Redirect object abstracts out redirecting streams to files.
    """
    STDOUT = 0
    STDERR = 1
    BOTH = 2
    STDOUT_APPEND = 3
    STDERR_APPEND = 4
    BOTH_APPEND = 5
    NULL = os.devnull
    _APPEND_MODES = {STDOUT_APPEND, STDERR_APPEND, BOTH_APPEND}
    _STDOUT_MODES = {STDOUT, STDOUT_APPEND}
    _STDERR_MODES = {STDERR, STDERR_APPEND}
    _BOTH_MODES = {BOTH, BOTH_APPEND}

    _convert = {
        '>': STDOUT,
        '1>': STDOUT,
        '>>': STDOUT_APPEND,
        '1>>': STDOUT_APPEND,
        '2>': STDERR,
        '2>>': STDERR_APPEND,
        '&>': BOTH,
        '&>>': BOTH_APPEND
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
    def __init__(self, piped_software_blueprint):
        self.piped_software_blueprint = piped_software_blueprint

    def __str__(self):
        return str(self.piped_software_blueprint)


class CodeBlock(_ParslAppBlueprint):
    @staticmethod
    def register(func, args=None, kwargs=None, inputs=None, outputs=None, stdout=None, stderr=None, **_kwargs):
        blueprint_id = '{}_{}'.format(func.__name__, _ParslAppBlueprint.get_id())
        _ParslAppBlueprint._blueprints[blueprint_id] = {
            'id': blueprint_id,
            'type': 'python',
            'func': func,
            'args': args if args else list(),
            'kwargs': kwargs if kwargs else dict(),
            'inputs': list(map(str, inputs)),
            'outputs': list(map(str, outputs)),
            'stdout': stdout,
            'stderr': stderr
        }
        return _DeferredApp(blueprint_id)


class ParslPipeline(object):
    """
    ParslPipeline forms the basis for a Pipeline class. This class sets up workflow digraph construction,
    stream capturing, and registration of apps and dependencies with Parsl.
    """
    pipeline_args = None
    pipeline_config = None

    # Temporary directory to send stream output of un-Redirected apps
    _pipeline_run_temp_dir = tempfile.TemporaryDirectory(suffix='__operon')

    def _run_pipeline(self, pipeline_args, pipeline_config):
        # Set up logs dir
        os.makedirs(pipeline_args['logs_dir'], exist_ok=True)
        setup_logger(pipeline_args['logs_dir'])
        logger.info('Started pipeline run')

        # Run the pipeline to populate Software instances and construct the workflow graph
        self.pipeline(pipeline_args, pipeline_config)
        workflow_graph = self._assemble_graph(_ParslAppBlueprint._blueprints.values())

        # Register apps and data with Parsl, get all app futures
        pipeline_futs = self._register_workflow(workflow_graph, self._get_dfk(pipeline_args, pipeline_config))

        # Wait for all apps to complete
        for fut in pipeline_futs:
            fut.result()

        # Inject captured app stdout and stderr into logs
        for captured_output in os.listdir(ParslPipeline._pipeline_run_temp_dir.name):
            capture_output_path = os.path.join(ParslPipeline._pipeline_run_temp_dir.name, captured_output)
            app_name, stream = os.path.splitext(captured_output)
            captured_output_content = open(capture_output_path).read()
            if captured_output_content:
                logger.debug('Output from {stream} stream of {app_name}:\n{msg}'.format(
                    stream=stream[1:],  # Get extension of captured stream, without period
                    app_name=app_name,
                    msg=captured_output_content
                ))

    def _get_dfk(self, pipeline_args, pipeline_config):
        # 1) Config defined at runtime on the command line
        if pipeline_args['parsl_config'] is not None:
            loaded_config = cycle_config_input_options(pipeline_args['parsl_config'])
            if loaded_config is not None:
                logger.info('Loaded Parsl config from command line arguments')
                return loaded_config

        # 2) Config defined for this pipeline in the pipeline configuration
        if pipeline_config.get('parsl_config'):
            loaded_config = cycle_config_input_options(pipeline_config['parsl_config'])
            if loaded_config is not None:
                logger.info('Loaded Parsl config from pipeline config')
                return loaded_config

        # 3) Config defined as an installation default, if all above options are absent
        # A stub parsl configuration is provided by init, but the user must manually make changes
        # to the stub for this method to be activated, otherwise it will be ignored
        if os.path.isfile(os.path.join(get_operon_home(), 'parsl_config.json')):
            init_parsl_config_filepath = os.path.join(get_operon_home(), 'parsl_config.json')
            with open(init_parsl_config_filepath) as init_parsl_config_json:
                try:
                    init_parsl_config = json.load(init_parsl_config_json)
                    if 'use' not in init_parsl_config:
                        logger.info('Loaded Parsl config from installation default')
                        return direct_config(init_parsl_config)
                except json.JSONDecodeError:
                    logger.error('Malformed JSON when loading from installation default, trying next option')
                except ValueError:
                    logger.error('Bad Parsl config when loading from installation default, trying the next option')

        # 4) Config defined by the pipeline developer as a default, if no user config exists
        if self.parsl_configuration():
            logger.info('Loaded Parsl config from pipeline default')
            try:
                return direct_config(self.parsl_configuration())
            except ValueError:
                pass  # Silently fail, move on to next option

        # 5) Config used if all above are absent, always run as a Thread Pool with 8 workers
        logger.info('Loaded Parsl config using package default (8 basic threads)')
        return dfk_with_config['basic-threads-8']()

    def _register_workflow(self, workflow_graph, dfk):
        # Instantiate the App Factories
        @App('python', dfk)
        def _pythonapp(func_, func_args, func_kwargs, **kwargs):
            return func_(*func_args, **func_kwargs)

        @App('bash', dfk)
        def _bashapp(cmd, **kwargs):
            return cmd

        @App('bash', dfk)
        def _cleanup(*args, **kwargs):
            return 'rm {} 2>/dev/null || exit 0'.format(' '.join(args))

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
            for input_data_node in workflow_graph.predecessors(app_node_id):
                if input_data_node not in data_futures and data_node_in_degree[input_data_node] > 0:
                    register_app(list(workflow_graph.predecessors(input_data_node))[0], workflow_graph)

            # Register this app
            _app_blueprint = workflow_graph.node[app_node_id]['blueprint']
            _app_inputs = [
                data_futures.get(input_data)
                for input_data in _app_blueprint['inputs']
                if data_futures.get(input_data)
            ]
            if _app_blueprint['type'] == 'bash':
                _app_future = _bashapp(
                    cmd=_app_blueprint['cmd'],
                    inputs=_app_inputs,
                    outputs=_app_blueprint['outputs'],
                    stdout=_app_blueprint['stdout'],
                    stderr=_app_blueprint['stderr']
                )
            else:
                _app_future = _pythonapp(
                    func_=_app_blueprint['func'],
                    func_args=_app_blueprint['args'],
                    func_kwargs=_app_blueprint['kwargs'],
                    inputs=_app_inputs,
                    outputs=_app_blueprint['outputs'],
                    stdout=_app_blueprint['stdout'],
                    stderr=_app_blueprint['stderr']
                )

            app_futures.append(_app_future)

            # Set output data futures
            for data_fut in _app_future.outputs:
                if data_fut.filename not in data_futures:
                    data_futures[data_fut.filename] = data_fut

            app_nodes_registered[app_node_id] = True

        # Register all apps
        for app_node in app_nodes_registered:
            if not app_nodes_registered[app_node]:
                register_app(app_node, workflow_graph)

        # If any temporary files exist, register cleanup program at the end of the workflow
        tmp_files = [d for d in data_futures if Data(d).tmp]
        if tmp_files:
            app_futures.append(_cleanup(*tmp_files, inputs=copy(app_futures)))

        return app_futures

    def _assemble_graph(self, blueprints):
        # Initialize a directed graph
        digraph = nx.DiGraph()

        # Iterate through edges, and add nodes as necessary
        for blueprint in blueprints:
            if blueprint['type'] == 'bash':
                app_id = blueprint['id']
                digraph.add_node(blueprint['id'], name=blueprint['name'], type='app', blueprint=blueprint)
            else:
                app_id = blueprint['func'].__name__
                digraph.add_node(app_id, name=app_id, type='app', blueprint=blueprint)

            for blp_input in blueprint['inputs']:
                digraph.add_node(blp_input, name=blp_input, type='data')
                digraph.add_edge(blp_input, app_id)

            for blp_output in blueprint['outputs']:
                digraph.add_node(blp_output, name=blp_output, type='data')
                digraph.add_edge(app_id, blp_output)

        # Output graph in JSON format
        json_digraph = {'nodes': list(), 'edges': list()}
        for node, nodedata in digraph.nodes.items():
            json_digraph['nodes'].append(
                {'data': {'id': os.path.basename(node), 'type': nodedata['type']}}
            )
        for edge, edgedata in digraph.edges.items():
            json_digraph['edges'].append(
                {'data': {'source': os.path.basename(edge[SOURCE]), 'target': os.path.basename(edge[TARGET])}}
            )
        # print(json.dumps(json_digraph, indent=2))
        # TODO Find a way to output this to the user, maybe in the logs directory

        return digraph

    def _parse_config(self):
        try:
            with open(self.pipeline_args['pipeline_config']) as config:
                self.pipeline_config = json.loads(config.read())
        except IOError:
            sys.stdout.write('Fatal Error: Config file at {} does not exist.\n'.format(
                self.pipeline_args['config']
            ))
            sys.stdout.write('A config file location can be specified with the --config option.\n')
            sys.exit(EXIT_ERROR)
        except ValueError:
            sys.stdout.write('Fatal Error: Config file at {} is not in JSON format.\n'.format(
                self.pipeline_args['config']
            ))
            sys.exit(EXIT_ERROR)

    def _print_dependencies(self):
        sys.stdout.write('\n'.join(self.dependencies()) + '\n')

    def parsl_configuration(self):
        """
        Override this method.

        This should be used with caution because it potentially reduces portability.
        :return: dict Parsl configuration as a low-precedence default for this pipeline
        """
        return None

    def description(self):
        """
        Override this method.
        A single string describing this pipeline.
        :return: str A description of this pipeline.
        """
        return ''

    def dependencies(self):
        """
        Override this method.
        A list of pip style dependencies for this pipeline.
        :return: list A list of pip style dependencies.
        """
        return list()

    def conda(self):
        """
        Override this method.
        Directives to download software from conda/bioconda.

        The returned dictionary should have a key 'packages' and an
        optional key 'channels'.
            - 'packages' should be a list of operon.components.CondaPackage tuples
            - 'channels' should be list of conda channels to temporarily access for installation

        :return: dict A dict configuring conda packages
        """
        return dict()

    def arguments(self, parser):
        """
        Override this method.
        Adds arguments to this pipeline using the argparse.add_argument() method. The parser
        argument is an argparse.ArgumentParser() object.
        :param parser: argparse.ArgumentParser object
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
        the user when he/she calls operon configure.

        In the pipeline, this dictionary will become the class variable pipeline_config, with
        terminal string values replaced with user input values.
        :return: dict Dictionary representation of config values
        """
        return dict()

    def pipeline(self, pipeline_args, pipeline_config):
        """
        Override this method.
        The logic of the pipeline will be here. The arguments are automatically populated with the
        user arguments to the pipeline (those added with the method self.arguments()) as well as
        the configuration for the pipeline (as a dictionary of the form returned by the method config(),
        but with user input values in place of terminal strings.)
        :param pipeline_args: dict Populated dictionary of user arguments
        :param pipeline_config: dict Populated dictionary of pipeline configuration
        :return: None
        """
        raise NotImplementedError

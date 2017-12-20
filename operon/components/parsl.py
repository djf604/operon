import os
import json
from copy import copy

import six
from parsl import DataFlowKernel, ThreadPoolExecutor, App

from operon.components import BasePipeline

# import parsl
# parsl.set_file_logger('parsl.log')


class ParslPipeline(BasePipeline):
    def _run_pipeline(self, pipeline_args, pipeline_config):
        self.run_pipeline(pipeline_args, pipeline_config)
        pipeline_futs = self._assemble_workflow(Software._blueprints)

        # Wait for all apps to complete
        for fut in pipeline_futs:
            fut.result()

    def _assemble_workflow(self, blueprint):
        # TODO Obviously this needs to be configurable in the future
        workers = ThreadPoolExecutor(max_workers=8)
        dfk = DataFlowKernel(executors=[workers])
        app_futures, data_futures = list(), dict()

        @App('bash', dfk)
        def _app(cmd, **kwargs):
            return cmd

        @App('bash', dfk)
        def _cleanup(*args, **kwargs):
            return 'rm {}'.format(' '.join(args))

        for app_blueprint in blueprint:
            # Get data futures for input data, or pass string if it doesn't exist
            _app_inputs = [
                data_futures.get(input_data)
                for input_data in app_blueprint['inputs']
                if data_futures.get(input_data)
            ]
            _app_future = _app(
                cmd=app_blueprint['cmd'],
                inputs=_app_inputs,
                outputs=app_blueprint['outputs'],
                stdout=app_blueprint['stdout'],
                stderr=app_blueprint['stderr']
            )
            app_futures.append(_app_future)

            for data_fut in _app_future.outputs:
                if data_fut.filename not in data_futures:
                    data_futures[data_fut.filename] = data_fut

        # Delete all temporary files, as last step
        tmp_files = [d for d in data_futures if Data(d).tmp]
        if tmp_files:
            app_futures.append(_cleanup(*tmp_files, inputs=copy(app_futures)))

        return app_futures


class Software(object):
    _id = 0
    _blueprints = list()

    def __init__(self, name, path):
        self.name = name
        self.path = path

    def register(self, *args, **kwargs):
        """
        Registers a Software run as a parsl App
        :param args:
        :param kwargs:
        :return:
        """
        Software._blueprints.append(self.prep(*args, **kwargs))

    def run(self, *args, **kwargs):
        """
        Alias for self.register(), for inter-run-mode compatibility
        """
        self.register(*args, **kwargs)

    def prep(self, *args, **kwargs):
        app_blueprint = {
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
            Data._data[path] = self
            self.path = path
            self.tmp = None
            self.mode = None

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
    """


class Parameter(object):
    def __init__(self, *args, sep=' '):
        # self.parameters = [
        #     split_arg
        #     for arg in args
        #     for split_arg in str(arg).split(sep)
        # ]
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
        if isinstance(stream, six.string_types):
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
            reverse_convert = {v: k for k, v in six.iteritems(Redirect._convert)}
            return reverse_convert[token]
        return Redirect.STDOUT


class Pipe(object):
    def __init__(self, piped_software_blueprint):
        self.piped_software_blueprint = piped_software_blueprint

    def __str__(self):
        return str(self.piped_software_blueprint)
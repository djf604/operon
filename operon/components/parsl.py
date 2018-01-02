import os
import json
from copy import copy, deepcopy
from functools import wraps
import inspect
import uuid
import random

import six
from parsl import DataFlowKernel, ThreadPoolExecutor, App

from operon.components import BasePipeline

import networkx as nx

# import parsl
# parsl.set_file_logger('parsl.log')


class ParslPipeline(BasePipeline):
    def _run_pipeline(self, pipeline_args, pipeline_config):
        self.run_pipeline(pipeline_args, pipeline_config)
        workflow_graph = self._assemble_graph(ParslAppBlueprint._blueprints.values())
        pipeline_futs = self._register_workflow(workflow_graph)

        # Wait for all apps to complete
        for fut in pipeline_futs:
            fut.result()

    def _register_workflow(self, workflow_graph):
        # Set up parsl
        workers = ThreadPoolExecutor(max_workers=8)
        dfk = DataFlowKernel(executors=[workers])

        # Instantiate the App Factories
        @App('python', dfk)
        def _pythonapp(func_, func_args, func_kwargs, **kwargs):
            return func_(*func_args, **func_kwargs)

        @App('bash', dfk)
        def _bashapp(cmd, **kwargs):
            return cmd

        @App('bash', dfk)
        def _cleanup(*args, **kwargs):
            return 'rm {}'.format(' '.join(args))

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
        json_digraph = digraph.copy()
        for node in json_digraph.nodes():
            json_digraph.node[node]['blueprint'] = None
        print(json.dumps(nx.node_link_data(json_digraph), indent=2))

        return digraph


class ParslAppBlueprint(object):
    _id_counter = 0
    _blueprints = dict()
    _app_futures = dict()

    @classmethod
    def get_id(cls):
        cls._id_counter += 1
        return cls._id_counter


class Software(ParslAppBlueprint):
    _id = 0
    _blueprints = list()
    _software_paths = set()

    def __init__(self, name, path):
        self.name = name
        self.path = path

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
        # Software._blueprints.append(blueprint)
        ParslAppBlueprint._blueprints[blueprint['id']] = blueprint
        return DeferredApp(blueprint['id'])

    def run(self, *args, **kwargs):
        """
        Alias for self.register(), for inter-run-mode compatibility
        """
        self.register(*args, **kwargs)

    def prep(self, *args, **kwargs):
        app_blueprint = {
            'id': '__app__{}'.format(ParslAppBlueprint.get_id()),
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


class CodeBlock(ParslAppBlueprint):
    _blueprints = list()

    @staticmethod
    def register(func, args=None, kwargs=None, inputs=None, outputs=None, stdout=None, stderr=None, **_kwargs):
        blueprint_id = '__app__{}'.format(ParslAppBlueprint.get_id())
        ParslAppBlueprint._blueprints[blueprint_id] = {
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
        # CodeBlock._blueprints.append({
        #     'id': blueprint_id,
        #     'type': 'python',
        #     'func': func,
        #     'args': args if args else list(),
        #     'kwargs': kwargs if kwargs else dict(),
        #     'inputs': list(map(str, inputs)),
        #     'outputs': list(map(str, outputs)),
        #     'stdout': stdout,
        #     'stderr': stderr
        # })
        return DeferredApp(blueprint_id)


    # def register(*args, inputs=None, outputs=None, stdin=None, stderr=None):
    #     def _decorator(func):
    #         @wraps(func)
    #         def _wrapped(*args, **kwargs):
    #             print(args)
    #             print(kwargs)
    #             return func(*args, )


class DeferredApp(object):
    def __init__(self, id):
        self.id = id

from operon.components import (Software, Parameter, Redirect, Data, CodeBlock,
                               ParslPipeline)
from operon._util.apps import _ParslAppBlueprint
import tempfile
from parsl import ThreadPoolExecutor, DataFlowKernel
from parsl.executors.errors import ScalingFailed
import pytest
from operon._util.logging import setup_logger
import glob
import os

# Set temp dir
ParslPipeline._pipeline_run_temp_dir = tempfile.TemporaryDirectory(
    dir='/tmp',
    suffix='__operon'
)


def pipeline_components_for_tests():
    # Reset all components
    _ParslAppBlueprint._id_counter = 0
    _ParslAppBlueprint._blueprints = dict()
    Software._software_paths = set()
    Data._data = dict()

    # Instantiate software
    petrichor = Software('petrichor', '/home/dfitzgerald/workspace/PycharmProjects/Operon/tests/petrichor')
    bash_sleep = Software('bash_sleep', '/bin/sleep')

    # Define python app
    def notos(sleep=None, outs=None):
        import time
        import random
        id_ = random.randint(1, 1000)
        if sleep:
            time.sleep(sleep)
        if outs:
            for out in outs:
                with open(out, 'w') as outfile:
                    outfile.write('{}\n'.format(id_))

    # App a, start=0, end=2
    petrichor.register(
        Parameter('--sleep', '2'),
        Redirect(stream='>', dest=Data('a.out'))
    )

    # App b, start=0, end=3
    petrichor.register(
        Parameter('--sleep', '3'),
        Redirect(stream='>', dest=Data('b.out'))
    )

    # App c, start=0, end=5
    petrichor.register(
        Parameter('--sleep', '5'),
        Redirect(stream='>', dest=Data('c.out'))
    )

    # App d, start=5, end=8
    CodeBlock.register(
        func=notos,
        kwargs={
            'sleep': 3,
            'outs': ['d1.out', 'd2.out']
        },
        inputs=[Data('a.out'), Data('b.out'), Data('c.out')],
        outputs=[Data('d1.out'), Data('d2.out')]
    )

    # App e, start=0, end=10
    app_e = bash_sleep.register(
        Parameter('10')
    )

    # App g, start=10, end=12
    CodeBlock.register(
        func=notos,
        kwargs={
            'sleep': 2,
            'outs': ['g1.out', 'g2.out']
        },
        inputs=[Data('d2.out')],
        outputs=[Data('g1.out'), Data('g2.out')],
        wait_on=[app_e]
    )

    # App f, start=8, end=11
    petrichor.register(
        Parameter('--sleep', '3'),
        Parameter('--outfile', Data('f.out').as_output(tmp=True)),
        extra_inputs=[Data('d1.out')]
    )

    # App h, start=12, end=18
    app_h = bash_sleep.register(
        Parameter('6'),
        extra_inputs=[Data('g2.out')]
    )

    # App i, start=18, end=20
    petrichor.register(
        Parameter('--sleep', '2'),
        Parameter('--outfile', Data('i.final').as_output()),
        extra_inputs=[Data('g1.out'), Data('f.out')],
        wait_on=[app_h]
    )


def test_workflow_graph_generation():
    pipeline_components_for_tests()
    workflow_graph = ParslPipeline._assemble_graph(_ParslAppBlueprint._blueprints.values())

    # Check for app nodes
    app_nodes = (
        'petrichor_1',
        'petrichor_2',
        'petrichor_3',
        'notos_4',
        'sleep_5',
        'notos_6',
        'petrichor_7',
        'sleep_8',
        'petrichor_9'
    )
    for app_node_id in app_nodes:
        assert workflow_graph.nodes[app_node_id]['blueprint']
        assert workflow_graph.nodes[app_node_id]['type'] == 'app'

    # Check for data nodes
    data_nodes = (
        'a.out',
        'b.out',
        'c.out',
        'd1.out',
        'd2.out',
        'f.out',
        'g1.out',
        'g2.out',
        'i.final'
    )
    for data_node_id in data_nodes:
        assert len(workflow_graph.nodes[data_node_id]) == 2
        assert workflow_graph.nodes[data_node_id]['type'] == 'data'

    # Check for correct edges
    out_edges = {
        ('petrichor_1', 'a.out'),
        ('petrichor_2', 'b.out'),
        ('petrichor_3', 'c.out'),
        ('a.out', 'notos_4'),
        ('b.out', 'notos_4'),
        ('c.out', 'notos_4'),
        ('notos_4', 'd1.out'),
        ('notos_4', 'd2.out'),
        ('d2.out', 'notos_6'),
        ('sleep_5', 'notos_6'),
        ('notos_6', 'g1.out'),
        ('notos_6', 'g2.out'),
        ('g1.out', 'petrichor_9'),
        ('g2.out', 'sleep_8'),
        ('sleep_8', 'petrichor_9'),
        ('d1.out', 'petrichor_7'),
        ('petrichor_7', 'f.out'),
        ('f.out', 'petrichor_9'),
        ('petrichor_9', 'i.final')
    }
    assert out_edges == set(workflow_graph.edges)


def test_correct_dfk_cascade():
    direct_pass = """
    {
    "sites": [{
        "site": "four_jobs",
        "auth": {
            "channel": "ssh",
            "hostname": "192.170.228.90",
            "username": "dominic",
            "scriptDir": "/cephfs/users/dominic/.ippscripts"
        },
        "execution": {
            "executor": "ipp",
            "provider": "slurm",
            "block": {
                "nodes": 1,
                "taskBlocks": 1,
                "initBlocks": 1,
                "maxBlocks": 3,
                "minBlocks": 1,
                "walltime": "48:00:00"
            }
        }
    }],
    "globals": {"lazyErrors": true}
    }
    """

    with pytest.raises(ScalingFailed):
        # Argument level, built-in DFK
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config='basic-threads-4',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        ).executors[0], ThreadPoolExecutor)

        # Argument level, point to JSON file
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config='tiny_config.json',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        ), DataFlowKernel)

        # Argument level, point to JSON file, malformed
        assert ParslPipeline._get_dfk(
            pipeline_args_parsl_config='malformed_config.json',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        ) is None

        # Argument level, direct pass
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config=direct_pass,
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        ), DataFlowKernel)

        # Config level, built-in DFK
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='basic-threads-4',
            pipeline_default_parsl_config=None
        ).executors[0], ThreadPoolExecutor)

        # Config level, point to JSON file
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        ), DataFlowKernel)

        # Config level, point to JSON file, malformed
        assert ParslPipeline._get_dfk(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='malformed_config.json',
            pipeline_default_parsl_config=None
        ) is None

        # Config level, direct pass
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config=direct_pass,
            pipeline_default_parsl_config=None
        ), DataFlowKernel)

        # Package default, built-in DFK
        assert isinstance(ParslPipeline._get_dfk(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config=None,
            pipeline_default_parsl_config=None
        ).executors[0], ThreadPoolExecutor)


def test_pipeline_execution(tmpdir_factory):
    spoofed_logs_dir = str(tmpdir_factory.mktemp('logs'))
    # Run pipeline to register Software and assemble workflow graph
    setup_logger(spoofed_logs_dir)
    pipeline_components_for_tests()
    workflow_graph = ParslPipeline._assemble_graph(_ParslAppBlueprint._blueprints.values())

    # Register apps and data with Parsl, get all app futures and temporary files
    pipeline_futs, tmp_files = ParslPipeline._register_workflow(
        workflow_graph=workflow_graph,
        dfk=ParslPipeline._get_dfk(
            pipeline_args_parsl_config='basic-threads-4',
            pipeline_config_parsl_config=None,
            pipeline_default_parsl_config=None
        )
    )

    # Monitor the run to completion
    ParslPipeline._monitor_run(
        pipeline_futs=pipeline_futs,
        tmp_files=tmp_files
    )

    # Assert that the logs look as expected
    # Note: This is a deterministic pipeline
    pipeline_logfile = glob.glob(os.path.join(spoofed_logs_dir, '*.log'))[0]
    log_order = [
        'petrichor_1 started running',
        'sleep_5 started running',
        'petrichor_3 started running',
        'petrichor_2 started running',
        'Actively running: sleep_5  petrichor_1  petrichor_3  petrichor_2',
        'petrichor_1 finished running',
        'Actively running: sleep_5  petrichor_3  petrichor_2',
        'petrichor_2 finished running',
        'Actively running: petrichor_3  sleep_5',
        'petrichor_3 finished running',
        'notos_4 started running',
        'Actively running: sleep_5  notos_4',
        'notos_4 finished running',
        'petrichor_7 started running',
        'Actively running: petrichor_7  sleep_5',
        'sleep_5 finished running',
        'notos_6 started running',
        'Actively running: notos_6  petrichor_7',
        'petrichor_7 finished running',
        'Actively running: notos_6',
        'notos_6 finished running',
        'sleep_8 started running',
        'Actively running: sleep_8',
        'sleep_8 finished running',
        'petrichor_9 started running',
        'Actively running: petrichor_9',
        'petrichor_9 finished running',
        'Finished pipeline run'
    ]

    # Go through the log file
    with open(pipeline_logfile) as log:
        # Go through all lines before @operon_start
        while not next(log).startswith('@operon_start'):
            pass

        # First four log entries can be in any order
        assert set(log_order[:4]) == {next(log).split('>')[-1].strip() for _ in range(4)}

        # Go through line by line, checking for expected output
        for i, line in enumerate(log, start=4):
            log_entry = line.split('>')[-1].strip()

            # If we get to @operon_end break out
            if line.startswith('@operon_end'):
                break

            # Actively running could be in any order, but set should match
            if log_entry.startswith('Actively running'):
                n_running = set(log_entry.split()[:2])
                n_should_be_running = set(log_order[i].split()[:2])
                assert n_running == n_should_be_running
            else:
                # Otherwise, compare for exact match
                assert log_entry == log_order[i]

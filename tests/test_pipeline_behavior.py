from operon.components import (Software, Parameter, Redirect, Data, CodeBlock,
                               ParslPipeline)
from operon._util.apps import _ParslAppBlueprint
from operon.meta import Meta
import tempfile
from parsl import ThreadPoolExecutor, DataFlowKernel
from parsl.executors.errors import ScalingFailed
import pytest
from operon._util.logging import setup_logger
import glob
import os
import logging

logger = logging.getLogger('operon.main')

# Set temp dir
ParslPipeline._pipeline_run_temp_dir = tempfile.TemporaryDirectory(
    dir='/tmp',
    suffix='__operon'
)


def reset_componenets():
    # Reset all components
    _ParslAppBlueprint._id_counter = 0
    _ParslAppBlueprint._blueprints = dict()
    Software._software_paths = set()
    Data._data = dict()
    logger.handlers = list()
    Meta._sites = dict()


def pipeline_components_for_tests():
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


def multipipeline_components_for_tests():
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

    Meta.define_site(name='Jerry', resources={
        'cpu': '2',
        'mem': '2G'
    })

    # App a, start=0, end=2
    # petrichor_1
    petrichor.register(
        Parameter('--sleep', '2'),
        Redirect(stream='>', dest=Data('a.out')),
        meta={
            'resources': {
                'cpu': '1',
                'mem': '1G'
            }
        }
    )

    # App b, start=0, end=3
    # petrichor_2
    petrichor.register(
        Parameter('--sleep', '3'),
        Redirect(stream='>', dest=Data('b.out')),
        meta={
            'resources': {
                'cpu': '2',
                'mem': '2G'
            }
        }
    )

    # App c, start=0, end=5
    # petrichor_3
    petrichor.register(
        Parameter('--sleep', '5'),
        Redirect(stream='>', dest=Data('c.out')),
        meta={
            'resources': {
                'cpu': '3',
                'mem': '3G'
            }
        }
    )

    # App d, start=5, end=8
    # notos_4
    CodeBlock.register(
        func=notos,
        kwargs={
            'sleep': 3,
            'outs': ['d1.out', 'd2.out']
        },
        inputs=[Data('a.out'), Data('b.out'), Data('c.out')],
        outputs=[Data('d1.out'), Data('d2.out')],
        meta={
            'resources': {
                'cpu': '1',
                'mem': '1G'
            }
        }
    )

    # App e, start=0, end=10
    # sleep_5
    app_e = bash_sleep.register(
        Parameter('10'),
        meta={'site': 'Jerry'}
    )

    # App g, start=10, end=12
    # notos_6
    CodeBlock.register(
        func=notos,
        kwargs={
            'sleep': 2,
            'outs': ['g1.out', 'g2.out']
        },
        inputs=[Data('d2.out')],
        outputs=[Data('g1.out'), Data('g2.out')],
        wait_on=[app_e],
        meta={
            'resources': {
                'cpu': '1',
                'mem': '1G'
            }
        }
    )

    # App f, start=8, end=11
    # petrichor_7
    petrichor.register(
        Parameter('--sleep', '3'),
        Parameter('--outfile', Data('f.out').as_output(tmp=True)),
        extra_inputs=[Data('d1.out')],
        meta={
            'resources': {
                'cpu': '2',
                'mem': '2G'
            },
            'site': 'Jerry'
        }
    )

    # App h, start=12, end=18
    # sleep_8
    app_h = bash_sleep.register(
        Parameter('6'),
        extra_inputs=[Data('g2.out')],
        meta={
            'resources': {
                'cpu': '3',
                'mem': '3G'
            },
            'site': 'DoesNotExist'
        }
    )

    # App i, start=18, end=20
    # petrichor_9
    petrichor.register(
        Parameter('--sleep', '2'),
        Parameter('--outfile', Data('i.final').as_output()),
        extra_inputs=[Data('g1.out'), Data('f.out')],
        wait_on=[app_h],
        meta={
            'resources': {
                'cpu': '1',
                'mem': '1G'
            }
        }
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
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config='basic-threads-4',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        )).executors[0], ThreadPoolExecutor)

        # Argument level, point to JSON file
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config='tiny_config.json',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        )), DataFlowKernel)

        # Argument level, point to JSON file, malformed
        assert ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config='malformed_config.json',
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        )) is None

        # Argument level, direct pass
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=direct_pass,
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        )), DataFlowKernel)

        # Config level, built-in DFK
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='basic-threads-4',
            pipeline_default_parsl_config=None
        )).executors[0], ThreadPoolExecutor)

        # Config level, point to JSON file
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='tiny_config.json',
            pipeline_default_parsl_config=None
        )), DataFlowKernel)

        # Config level, point to JSON file, malformed
        assert ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config='malformed_config.json',
            pipeline_default_parsl_config=None
        )) is None

        # Config level, direct pass
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config=direct_pass,
            pipeline_default_parsl_config=None
        )), DataFlowKernel)

        # Package default, built-in DFK
        assert isinstance(ParslPipeline._get_dfk(*ParslPipeline._choose_parsl_config(
            pipeline_args_parsl_config=None,
            pipeline_config_parsl_config=None,
            pipeline_default_parsl_config=None
        )).executors[0], ThreadPoolExecutor)


def test_various_pipelines(tmpdir_factory):
    no_site_assignments = {
        'petrichor_1': '__all__',
        'petrichor_2': '__all__',
        'petrichor_3': '__all__',
        'notos_4': '__all__',
        'sleep_5': '__all__',
        'notos_6': '__all__',
        'petrichor_7': '__all__',
        'sleep_8': '__all__',
        'petrichor_9': '__all__',
    }
    # Test single-single on threads
    do_pipeline_execution(tmpdir_factory, 'basic-threads-4', pipeline_components_for_tests,
                          site_assignments=no_site_assignments)
    # Test single-single on ipp
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_config.json', pipeline_components_for_tests,
                          site_assignments=no_site_assignments)

    # Test single-multi on ipp
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_multiconfig.json', pipeline_components_for_tests,
                          site_assignments=no_site_assignments)

    # Test multi-single on ipp
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_config.json', multipipeline_components_for_tests,
                          site_assignments=no_site_assignments)

    # Test multi-multi-perfect on ipp
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_multiconfig.json', multipipeline_components_for_tests,
                          site_assignments={
                              'petrichor_1': 'resources_(1,1G)',
                              'petrichor_2': 'resources_(2,2G)',
                              'petrichor_3': 'resources_(3,3G)',
                              'notos_4': 'resources_(1,1G)',
                              'sleep_5': 'Jerry',
                              'notos_6': 'resources_(1,1G)',
                              'petrichor_7': 'Jerry',
                              'sleep_8': 'resources_(3,3G)',
                              'petrichor_9': 'resources_(1,1G)',
                          })

    # Test multi-multi-some on ipp, missing sites in config
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_multiconfig_some_missing.json', multipipeline_components_for_tests,
                          site_assignments={
                              'petrichor_1': 'resources_(1,1G)',
                              'petrichor_2': '__all__',
                              'petrichor_3': 'resources_(3,3G)',
                              'notos_4': 'resources_(1,1G)',
                              'sleep_5': '__all__',
                              'notos_6': 'resources_(1,1G)',
                              'petrichor_7': '__all__',
                              'sleep_8': 'resources_(3,3G)',
                              'petrichor_9': 'resources_(1,1G)',
                          })
    # Test multi-multi-some on ipp, extra sites in config
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_multiconfig_some_extra.json', multipipeline_components_for_tests,
                          site_assignments={
                              'petrichor_1': 'resources_(1,1G)',
                              'petrichor_2': 'resources_(2,2G)',
                              'petrichor_3': 'resources_(3,3G)',
                              'notos_4': 'resources_(1,1G)',
                              'sleep_5': 'Jerry',
                              'notos_6': 'resources_(1,1G)',
                              'petrichor_7': 'Jerry',
                              'sleep_8': 'resources_(3,3G)',
                              'petrichor_9': 'resources_(1,1G)',
                          })

    # Test multi-multi-mismatch on ipp
    do_pipeline_execution(tmpdir_factory, 'tiny_ipp_multiconfig_all_missing.json', multipipeline_components_for_tests,
                          site_assignments=no_site_assignments)


def do_pipeline_execution(tmpdir_factory, parsl_config, pipeline_components_func, site_assignments):
    spoofed_logs_dir = str(tmpdir_factory.mktemp('logs'))
    # Run pipeline to register Software and assemble workflow graph
    reset_componenets()
    setup_logger(spoofed_logs_dir)
    pipeline_components_func()
    workflow_graph = ParslPipeline._assemble_graph(_ParslAppBlueprint._blueprints.values())

    # Register apps and data with Parsl, get all app futures and temporary files
    config_type, parsl_config = ParslPipeline._choose_parsl_config(
        pipeline_args_parsl_config=parsl_config,
        pipeline_config_parsl_config=None,
        pipeline_default_parsl_config=None
    )
    pipeline_futs, tmp_files = ParslPipeline._register_workflow(
        workflow_graph=workflow_graph,
        dfk=ParslPipeline._get_dfk(
            config_type=config_type,
            parsl_config=parsl_config
        ),
        parsl_config=config_type if config_type == 'builtin' else parsl_config
    )

    # Monitor the run to completion
    ParslPipeline._monitor_run(
        pipeline_futs=pipeline_futs,
        tmp_files=tmp_files
    )

    # Get the log file from this run
    pipeline_logfile = glob.glob(os.path.join(spoofed_logs_dir, '*.log'))[0]

    # Ensure apps were sent to their correct sites
    log_site_assignments = dict()
    with open(pipeline_logfile) as log:
        for line in log:
            if 'assigned to site' in line:
                line = line.split('>')[-1].strip().split()
                log_site_assignments[line[0]] = line[-1]
    assert log_site_assignments == site_assignments

    # Assert that the logs reflect the correct workflow dependency graph
    # We have no guarantee about when apps will run, only that they won't start
    # until all dependencies are finished; as such, that's all we can assert
    # Get just the relevant parts of the log output
    cleaned_pipeline_log = list()
    with open(pipeline_logfile) as log:
        # Go through all lines before @operon_start
        while not next(log).startswith('@operon_start'):
            pass
        for line in log:
            # If we reach @operon_end we're done
            if line.startswith('@operon_end'):
                break
            cleaned_pipeline_log.append(line.split('>')[-1].strip())

    # Ensure the log show the dependency graph was honored for each app
    running_order = [
        l for l in cleaned_pipeline_log
        if l.endswith(' running')
    ]
    dependent_apps = {
        'notos_4': {'petrichor_1', 'petrichor_2', 'petrichor_3'},
        'notos_6': {'notos_4', 'sleep_5'},
        'petrichor_7': {'notos_4'},
        'sleep_8': {'notos_6'},
        'petrichor_9': {'notos_6', 'petrichor_7', 'sleep_8'}
    }

    for dependent_app, dependencies in dependent_apps.items():
        # Find line containing dependent app
        dependent_app_log_i = running_order.index(dependent_app + ' started running')
        previous_dependencies = set(running_order[:dependent_app_log_i])
        assert set([d + ' finished running' for d in dependencies]).issubset(previous_dependencies)

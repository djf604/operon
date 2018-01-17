import os
import json
from functools import partial
import logging

from parsl import ThreadPoolExecutor, DataFlowKernel
from parsl.execution_provider.errors import BadConfig

logger = logging.getLogger('operon.main')


def cycle_config_input_options(user_input):
    # Look for the name of an Operon pre-defined parsl config
    if user_input in dfk_with_config:
        return dfk_with_config[user_input]()

    # If this is a valid filepath, try to read the JSON as a config
    if os.path.isfile(user_input):
        with open(user_input) as config_json:
            try:
                return direct_config(json.load(config_json))
            except json.JSONDecodeError:
                logger.error('Malformed JSON when loading from command line arguments '
                             'or pipeline config, trying next option')
            except ValueError:
                logger.error('Bad Parsl configuration when loading from command line '
                             'arguments or pipeline config, trying next option')

    # Try to load the input directly as JSON
    try:
        return direct_config(json.loads(user_input))
    except json.JSONDecodeError:
        logger.error('Malformed JSON when loading from command line arguments '
                     'or pipeline config, trying next option')
    except ValueError:
        logger.error('Bad Parsl configuration when loading from command line '
                     'arguments or pipeline config, trying next option')

    # If all the above options failed, return None so other options will be tried
    return None


def direct_config(config):
    try:
        return DataFlowKernel(config=config)
    except BadConfig:
        raise ValueError


def basic_threads(workers=8):
    workers = ThreadPoolExecutor(max_workers=workers)
    return DataFlowKernel(executors=[workers])


dfk_with_config = {
    'basic-threads-8': partial(basic_threads, workers=8),
    'basic-threads-4': partial(basic_threads, workers=4),
    'basic-threads-2': partial(basic_threads, workers=2),
}

init_config_stub = {
    'use': 'Remove this key-value to activate this parsl config',
    'sites': [
        {
            'site': 'Name of the site being defined',
            'auth': {
                'channel': 'Channel type [local, ssh, ssh-il]'
            },
            'execution': {
                'executor': 'Mechanism that executes tasks on compute resources [ipp, threads, swift_t]',
                'provider': 'Scheduler or resources type of the site [slurm, torque, condor, aws, ...]',
                'block': {
                    'nodes': 'Nodes to request per block',
                    'taksBlocks': 'Workers to start per block',
                    'initBlocks': 'Number of blocks to provision at execution start',
                    'minBlocks': 'Min blocks to maintain during execution',
                    'maxBlocks': 'Max blocks that can be provisioned',
                    'walltime': 'Walltime allowed for the block in HH:MM:SS format',
                    'options': {
                        'attr': 'Provider specific attributes given to provider for execution'
                    }
                }
            }
        }
    ],
    'globals': {
        'lazyErrors': True
    },
    'controller': {
        'publicIp': 'Public IP address of the launching machine'
    }
}
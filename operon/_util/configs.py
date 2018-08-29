import sys
import os
import json
from functools import partial
import logging

from parsl import ThreadPoolExecutor, DataFlowKernel
# from parsl.execution_provider.errors import BadConfig

from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

from operon._util.home import load_parsl_config_file

logger = logging.getLogger('operon.main')


def parse_pipeline_config(pipeline_config_path):
    try:
        with open(pipeline_config_path) as config:
            return json.loads(config.read())
    except IOError:
        sys.stdout.write('Fatal Error: Config file at {} does not exist.\n'.format(
            pipeline_config_path
        ))
        sys.stdout.write('A config file location can be specified with the --config option.\n')
        sys.exit(1)
    except ValueError:
        sys.stdout.write('Fatal Error: Config file at {} is not in JSON format.\n'.format(
            pipeline_config_path
        ))
        sys.exit(1)


def cycle_config_input_options(user_input):
    """
    User is expected to either input a path to a file containing Python code
    which will yield a parsl Config, or the name of a built-in/previously created
    Config in internal storage, as a pickled file
    :param user_input:
    :return:
    """
    if os.path.isfile(user_input):
        try:
            return load_parsl_config_file(user_input)
        except AttributeError:
            logger.warning('Parsl config could not be loaded because variable \'config\' could not be found')
            return None
        except TypeError:
            logger.warning('Object loaded from variable \'config\' is not a parsl.config.Config')
            return None
        except Exception as e:
            logger.warning(f'Parsl config could not be loaded: {e}')
            return None
    elif user_input in built_in_configs:
        return built_in_configs[user_input]()
    # TODO elif a previously created config as a pickle file, re-inflated
    logger.warning('Not a file on disk or built in config')
    return None


def basic_threads(workers=8):
    return Config(
        executors=[ThreadPoolExecutor(max_threads=workers)],
        retries=3
    )


built_in_configs = {
    'basic-threads-8': partial(basic_threads, workers=8),
    'basic-threads-4': partial(basic_threads, workers=4),
    'basic-threads-2': partial(basic_threads, workers=2),
    'basic-threads-1': partial(basic_threads, workers=1),
    'sequential-local': partial(basic_threads, workers=1)
}

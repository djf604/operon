import os
import json
from importlib.util import spec_from_file_location, module_from_spec


class OperonState(object):
    def __init__(self):
        # Get current state
        operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
        self.state_json_path = os.path.join(operon_home_root, '.operon', 'operon_state.json')
        with open(self.state_json_path) as operon_state:
            self.state = json.load(operon_state)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        with open(self.state_json_path, 'w') as operon_state:
            json.dump(self.state, operon_state, indent=2)

    def insert_pipeline(self, name, values):
        self.state['pipelines'][name] = values

    def remove_pipeline(self, name):
        self.state['pipelines'].pop(name, None)


def get_operon_home(root=False):
    operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
    return operon_home_root if root else os.path.join(operon_home_root, '.operon')


def load_pipeline_file(pipeline_filepath):
    """
    This only works in Python 3.5+
    Taken from https://stackoverflow.com/a/67692/1539628
    :param pipeline_filepath:
    :return:
    """
    spec = spec_from_file_location('operon.pipeline', pipeline_filepath)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

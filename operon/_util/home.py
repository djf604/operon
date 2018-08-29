import os
from importlib.util import spec_from_file_location, module_from_spec

import tinydb
import parsl.config

FILENAME_BASE = 0


class OperonState(object):
    """
    Schema:
    {
        'type': 'operon_record'
        'version': version of operon
        'init_date': date of operon directory initialization
        'home_root': root location of the operon directory
    }
    {
        'type': 'pipeline_record'
        'name': name of the pipeline
        'installed_date': date of pipeline installation
        'configured': True/False whether the pipeline has a corresponding configuration
    }
    """
    _init = False
    _operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
    db = None
    query = tinydb.Query()

    def __new__(cls, operon_home_root=None, *args, **kwargs):
        if not cls._init:
            db_path = os.path.join(operon_home_root or cls._operon_home_root,
                                   '.operon', '.operon_state_db.json')
            cls.db = tinydb.TinyDB(db_path)
        return super().__new__(cls, *args, **kwargs)

    # def __init__(self, *args, **kwargs):
    #     pass

    @classmethod
    def pipelines_installed(cls):
        return {
            record['name']
            for record in cls.db.search(cls.query.type == 'pipeline_record')
        }

    @classmethod
    def pipelines_configured(cls):
        return [
            (record['name'], record['configured'])
            for record in cls.db.search(cls.query.type == 'pipeline_record')
        ]

    @classmethod
    def setting(cls, *args):
        if not args:
            return None
        _settings_doc = cls.db.get(doc_id=1)['settings']
        if args[0] in _settings_doc:
            if len(args) == 1:
                return _settings_doc.get(args[0])
            _settings_doc[args[0]] = args[1]
            cls.db.update({'settings': _settings_doc}, cls.query.type == 'operon_record')


def get_operon_home(root=False):
    operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
    return operon_home_root if root else os.path.join(operon_home_root, '.operon')


def pipeline_is_installed(pipeline_name, force_state_installation=False):
    pipeline_file_exists = os.path.isfile(
        os.path.join(get_operon_home(), 'pipelines', '{}.py'.format(pipeline_name))
    )
    if force_state_installation:
        return pipeline_file_exists and OperonState().db.search(OperonState().query.name == pipeline_name)
    return pipeline_file_exists


def load_module_from_file(module_filepath, package_name):
    """
    This only works in Python 3.5+
    Taken from https://stackoverflow.com/a/67692/1539628
    """
    spec = spec_from_file_location(package_name, module_filepath)
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_pipeline_file(pipeline_filepath):
    return load_module_from_file(pipeline_filepath, '__operon.pipeline')


def load_parsl_config_file(parsl_config_filepath):
    try:
        config = getattr(load_module_from_file(parsl_config_filepath, '__operon.parsl_config'), 'config')
    except:
        raise
    if not isinstance(config, parsl.config.Config):
        raise TypeError
    return config


def file_appears_installed(filepath):
    if not os.path.isfile(filepath):
        return False
    pipeline_name_base = os.path.splitext(os.path.basename(filepath))[FILENAME_BASE]
    return pipeline_name_base in OperonState().pipelines_installed()

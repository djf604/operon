import os
import pkgutil
from importlib import import_module

from operon._util.home import get_operon_home, load_pipeline_file


def get_operon_subcommands(classes=False):
    operon_subcommands = [
        operon_subcommand
        for operon_subcommand in [
            name for _, name, _
            in pkgutil.iter_modules(__path__)
        ]
    ]
    if not classes:
        return operon_subcommands

    return {
        operon_subcommand: fetch_subcommand_class(operon_subcommand)
        for operon_subcommand in operon_subcommands
    }


def fetch_subcommand_class(subcommand):
    module = import_module('operon._cli.subcommands.{}'.format(subcommand))
    return module.Subcommand()


class BaseSubcommand(object):
    home_pipelines = os.path.join(get_operon_home(), 'pipelines')
    home_configs = os.path.join(get_operon_home(), 'configs')

    def get_pipeline_class(self, pipeline_name):
        pipeline_filepath = os.path.join(self.home_pipelines,
                                         '{}.py'.format(pipeline_name))

        # Look for pipeline in installed directory first
        if os.path.isfile(pipeline_filepath):
            return load_pipeline_file(pipeline_filepath).Pipeline()
        # Check to see if pipeline name is a full path to pipeline
        elif os.path.isfile(pipeline_name):
            return load_pipeline_file(pipeline_name).Pipeline()
        # If none of the above, return None
        return None

    def help_text(self):
        return ''

    def run(self, subcommand_args):
        return NotImplementedError


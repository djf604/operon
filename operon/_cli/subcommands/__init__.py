import os
import sys
import pkgutil
from importlib import import_module

from operon._util.home import get_operon_home, load_pipeline_file
from operon._util.errors import MalformedPipelineError
from operon.components import ParslPipeline


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

    def get_pipeline_instance(self, pipeline_name):
        pipeline_filepath = os.path.join(self.home_pipelines,
                                         '{}.py'.format(pipeline_name))

        # Look for pipeline in installed directory first
        if os.path.isfile(pipeline_filepath):
            pipeline_mod = load_pipeline_file(pipeline_filepath)
        # Check to see if pipeline name is a full path to pipeline
        elif os.path.isfile(pipeline_name):
            pipeline_mod = load_pipeline_file(pipeline_name)
        # If none of the above, return None
        else:
            return None

        # Return pipeline instance
        try:
            # Ensure Pipeline subclasses ParslPipeline
            if not issubclass(pipeline_mod.Pipeline, ParslPipeline):
                raise MalformedPipelineError(
                    'Pipeline class does not subclass ParslPipeline\n'
                    'Try the form:\n\n'
                    '\tclass Pipeline(ParslPipeline):\n'
                )
            return pipeline_mod.Pipeline()
        except AttributeError:
            # Ensure the pipeline file contains a class called Pipeline
            raise MalformedPipelineError(
                'No Pipeline class could be found for {}\n'
                'Try the form:\n\n'
                '\tclass Pipeline(ParslPipeline):\n'.format(pipeline_name)
            )

    def help_text(self):
        return ''

    def run(self, subcommand_args):
        return NotImplementedError


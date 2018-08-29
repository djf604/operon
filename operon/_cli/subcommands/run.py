import os
import sys
import argparse

from operon._cli.subcommands import BaseSubcommand
from operon._util.configs import parse_pipeline_config

ARGV_FIRST_ARGUMENT = 0
ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


def usage():
    return 'operon run <pipeline-name> [-h] [pipeline-options]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Run a pipeline with pipeline-specific parameters.'

    def run(self, subcommand_args):
        # Get pipeline name or output help
        parser = argparse.ArgumentParser(prog='operon run', usage=usage(), description=self.help_text())
        if not subcommand_args or subcommand_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        # Get the pipeline class based on the name
        pipeline_name = subcommand_args[ARGV_PIPELINE_NAME]
        pipeline_instance = self.get_pipeline_instance(pipeline_name)

        if pipeline_instance is not None:
            # Parse the pipeline arguments and inject them into the pipeline class
            pipeline_args_parser = argparse.ArgumentParser(prog='operon run {}'.format(pipeline_name))
            pipeline_args_parser.add_argument('--pipeline-config',
                                              default=os.path.join(self.home_configs, '{}.json'.format(pipeline_name)),
                                              help='Path to a config file to use for this run')
            pipeline_args_parser.add_argument('--parsl-config',
                                              help='Path to a JSON file containing a Parsl config')
            pipeline_args_parser.add_argument('--logs-dir', default='.', help='Path to a directory to store log files')
            pipeline_args_parser.add_argument('--run-name', default='run', help='Name of this run for the log file')

            # Get custom arguments from the Pipeline
            pipeline_instance.arguments(pipeline_args_parser)
            pipeline_args = vars(pipeline_args_parser.parse_args(subcommand_args[1:]))

            pipeline_instance._run(
                pipeline_args=pipeline_args,
                pipeline_config=parse_pipeline_config(pipeline_args['pipeline_config']),
                original_command='run ' + ' '.join(subcommand_args)
            )
        else:
            # If pipeline class doesn't exist, exit immediately
            sys.stderr.write('Pipeline {name} does not exist in {home}\n'.format(
                name=pipeline_name,
                home=self.home_pipelines + '/'
            ))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

import argparse
import os
import sys
from operon.util.commands import BaseCommand

ARGV_FIRST_ARGUMENT = 0
ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


class Command(BaseCommand):
    @staticmethod
    def usage():
        return 'operon run <pipeline-name> [-h] [pipeline-options]'

    def help_text(self):
        return 'Run a pipeline with pipeline-specific parameters.'

    def run(self, command_args):
        # Get pipeline name or output help
        parser = argparse.ArgumentParser(prog='operon run', usage=self.usage(), description=self.help_text())
        if not command_args or command_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        # Get the pipeline class based on the name
        pipeline_name = command_args[ARGV_PIPELINE_NAME]
        pipeline_class = self.get_pipeline_class(pipeline_name)

        if pipeline_class is not None:
            # Parse the pipeline arguments and inject them into the pipeline class
            pipeline_args_parser = argparse.ArgumentParser(prog='operon run {}'.format(pipeline_name))
            pipeline_args_parser.add_argument('--pipeline-config',
                                              default=os.path.join(self.home_configs, '{}.json'.format(pipeline_name)),
                                              help='Path to a config file to use for this run.')
            pipeline_args_parser.add_argument('--parsl-config',
                                              help='Parsl config TODO')
            pipeline_class.add_pipeline_args(pipeline_args_parser)
            pipeline_class.pipeline_args = vars(pipeline_args_parser.parse_args(command_args[1:]))

            # Parse pipeline config and run pipeline
            pipeline_class._parse_config()
            pipeline_class._run_pipeline(
                pipeline_args=pipeline_class.pipeline_args,
                pipeline_config=pipeline_class.pipeline_config
            )
        else:
            # If pipeline class doesn't exist, exit immediately
            sys.stderr.write('Pipeline {name} does not exist in {home}\n'.format(
                name=pipeline_name,
                home=self.home_pipelines + '/'
            ))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

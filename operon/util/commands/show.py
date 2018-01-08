import sys
import os
import argparse
import json
from operon.util.commands import BaseCommand

ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


class Command(BaseCommand):
    @staticmethod
    def usage():
        return 'operon show <pipeline-line> [-h]'

    def help_text(self):
        return ('Show information about a Operon installed pipeline, including parameters, the ' +
                'configuration dictionary, and the current default configuration, if it exists.')

    def run(self, command_args):
        parser = argparse.ArgumentParser(prog='operon show', usage=self.usage(), description=self.help_text())
        parser.add_argument('pipeline-name', help='Operon pipeline to show.')
        pipeline_name = vars(parser.parse_args(command_args))['pipeline-name']
        if pipeline_name.lower() == 'help':
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        pipeline_class = self.get_pipeline_class(pipeline_name)
        config_dictionary = pipeline_class.configure()

        # Start show subcommand output
        sys.stderr.write('Operon pipeline: {}\n\n'.format(pipeline_name))

        # Show pipeline arguments
        show_parser = argparse.ArgumentParser(prog='operon run {}'.format(pipeline_name),
                                              description=pipeline_class.description())
        pipeline_class.add_pipeline_args(show_parser)
        show_parser.print_help()

        # Show pipeline dependencies
        sys.stderr.write('\nPipeline Dependencies:\n')
        if pipeline_class.dependencies():
            pipeline_class._print_dependencies()
        else:
            sys.stderr.write('None\n')

        # Show current platform configuration, if it exists
        config_json_filepath = os.path.join(self.home_configs, '{}.json'.format(pipeline_name))
        if os.path.isfile(config_json_filepath):
            sys.stderr.write('\nCurrent Configuration:\n')
            sys.stderr.write(open(config_json_filepath).read())

        # Show configuration dictionary, with prompts
        sys.stderr.write('\nConfiguration Dictionary:\n')
        sys.stderr.write(json.dumps(config_dictionary, indent=4) + '\n')
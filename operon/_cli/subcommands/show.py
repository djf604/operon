import sys
import os
import argparse
import json

from operon._cli.subcommands import BaseSubcommand

ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


def usage():
    return 'operon show <pipeline-line> [-h]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return ('Show information about a Operon installed pipeline, including parameters, the ' +
                'configuration dictionary, and the current default configuration, if it exists.')

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon show', usage=usage(), description=self.help_text())
        parser.add_argument('pipeline-name', help='Operon pipeline to show.')
        pipeline_name = vars(parser.parse_args(subcommand_args))['pipeline-name']
        if pipeline_name.lower() == 'help':
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        pipeline_instance = self.get_pipeline_instance(pipeline_name)
        config_dictionary = pipeline_instance.configuration()

        # Start show subcommand output
        sys.stderr.write('Operon pipeline: {}\n\n'.format(pipeline_name))

        # Show pipeline arguments
        show_parser = argparse.ArgumentParser(prog='operon run {}'.format(pipeline_name),
                                              description=pipeline_instance.description())
        pipeline_instance.arguments(show_parser)
        show_parser.print_help()

        # Show pipeline dependencies
        sys.stderr.write('\nPython Dependencies:\n')
        if pipeline_instance.dependencies():
            sys.stdout.write('\n'.join(pipeline_instance.dependencies()) + '\n')
        else:
            sys.stderr.write('None\n')

        # Show conda dependencies
        sys.stderr.write('\nConda Packages:\n')
        conda_packages = pipeline_instance.conda().get('packages')
        if conda_packages:
            sys.stderr.write('\n'.join(sorted(set([c.tag for c in conda_packages]))) + '\n')

        # Show current Parsl configuration, if it exists
        if pipeline_instance.parsl_configuration():
            sys.stderr.write('\nPipeline Default Parsl Configuration:\n')
            sys.stderr.write(json.dumps(pipeline_instance.parsl_configuration(), indent=4) + '\n')

        # Show current platform configuration, if it exists
        config_json_filepath = os.path.join(self.home_configs, '{}.json'.format(pipeline_name))
        if os.path.isfile(config_json_filepath):
            sys.stderr.write('\nCurrent Configuration:\n')
            sys.stderr.write(open(config_json_filepath).read())

        # Show configuration dictionary, with prompts
        sys.stderr.write('\nConfiguration Dictionary:\n')
        sys.stderr.write(json.dumps(config_dictionary, indent=4) + '\n')

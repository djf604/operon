import sys
import os
import argparse
import json

from operon._cli.subcommands import BaseSubcommand
from operon.meta import _MetaSiteDynamic

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
        sys.stdout.write('Operon pipeline: {}\n\n'.format(pipeline_name))

        # Show pipeline arguments
        show_parser = argparse.ArgumentParser(prog='operon run {}'.format(pipeline_name),
                                              description=pipeline_instance.description())
        pipeline_instance.arguments(show_parser)
        show_parser.print_help()

        # Show pipeline dependencies
        sys.stdout.write('\nPython Dependencies:\n')
        if pipeline_instance.dependencies():
            sys.stdout.write('\n'.join(pipeline_instance.dependencies()) + '\n')
        else:
            sys.stdout.write('None\n')

        # Show conda dependencies
        sys.stdout.write('\nConda Packages:\n')
        conda_packages = pipeline_instance.conda().get('packages')
        if conda_packages:
            sys.stdout.write('\n'.join(sorted(set([c.tag for c in conda_packages]))) + '\n')

        # Show current Parsl configuration, if it exists
        if pipeline_instance.parsl_configuration():
            sys.stdout.write('\nPipeline Default Parsl Configuration:\n')
            sys.stdout.write(json.dumps(pipeline_instance.parsl_configuration(), indent=4) + '\n')

        # Show what site names this pipeline expects and resources associated with each
        pipeline_sites = pipeline_instance.sites()
        if pipeline_sites:
            sys.stdout.write('\nPipeline Execution Sites:\n')
            for site_name, site_description in pipeline_sites.items():
                sys.stdout.write('{site_name}: {site_description}\n'.format(
                    site_name=site_name,
                    site_description=site_description['description']
                ))
                for resource, val in site_description['resources'].items():
                    if isinstance(val, _MetaSiteDynamic):
                        val = '[dynamic] {}'.format(val.description or '')
                    sys.stdout.write('\t{}: {}\n'.format(resource.capitalize(), val))
        """
        The problem is I can't really get the site names, including the implicitly defined sites, without 
        running the pipeline logic code, and I can't really do that because I don't have the pipeline args 
        defined. On top of that, some of the sites might be dynamically configured, so I can only really 
        report statically defined sites anyway.
        
        I think the way to go is to have a some place where the names of the expected sites can be defined 
        statically. It's on the developers honor that these actually match up with the sites defined in the 
        pipeline logic and will hold true at runtime.
        """

        # Show current platform configuration, if it exists
        config_json_filepath = os.path.join(self.home_configs, '{}.json'.format(pipeline_name))
        if os.path.isfile(config_json_filepath):
            sys.stdout.write('\nCurrent Configuration:\n')
            sys.stdout.write(open(config_json_filepath).read())

        # Show configuration dictionary, with prompts
        sys.stdout.write('\nConfiguration Dictionary:\n')
        sys.stdout.write(json.dumps(config_dictionary, indent=4) + '\n')

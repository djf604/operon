import os
import sys
import argparse

from operon._cli.subcommands import BaseSubcommand

BASENAME = 0
EXTENTION = 1


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Lists installed pipelines in the default Operon home directory.'

    def run(self, subcommand_args):
        # Get argparse help funtionality
        argparse.ArgumentParser(prog='operon list', description=self.help_text()).parse_args(subcommand_args)

        sys.stderr.write('Installed pipelines (in {}):\n\n'.format(self.home_pipelines))

        # Grab the installed files from both directories
        installed_pipelines = {os.path.splitext(pipeline)[BASENAME] for pipeline
                               in os.listdir(self.home_pipelines)
                               if pipeline != '__init__.py'}

        installed_configs = [config for config in os.listdir(self.home_configs)]

        for pipeline_name in installed_pipelines:
            if '{}.json'.format(pipeline_name) in installed_configs:
                sys.stderr.write('{} is configured\n'.format(pipeline_name))
            else:
                sys.stderr.write('{} is NOT configured\n'.format(pipeline_name))

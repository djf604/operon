import sys
import os
import shutil
import argparse
from datetime import datetime

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState

try:
    from pip import main as pip
except ImportError:
    pass

ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_ERROR = 1
EXIT_CMD_SYNTAX_ERROR = 2


def usage():
    return 'operon install <pipeline-path> [-h]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Install a Operon formatted pipeline.'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon install', usage=usage(), description=self.help_text())
        parser.add_argument('pipeline-path', help='Full path to a Operon formatted pipeline to install.')
        parser.add_argument('-y', action='store_true', help='Install all dependency packages without asking.')
        args = vars(parser.parse_args(subcommand_args))
        pipeline_path = args['pipeline-path']
        pipeline_name = os.path.splitext(os.path.basename(pipeline_path))[0]
        if pipeline_path.strip().lower() == 'help':
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        # Check if provided filepath actually exists
        if not os.path.isfile(pipeline_path):
            sys.stderr.write('Pipeline not {} found.\n'.format(pipeline_path))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

        # Check if pipeline already exists
        if os.path.isfile(os.path.join(self.home_pipelines, os.path.basename(pipeline_path))):
            overwrite = input('Pipeline {} is already installed, overwrite? [y/n] '.format(
                    os.path.basename(pipeline_path)
            ))
            if overwrite.lower() in {'no', 'n'}:
                sys.exit(EXIT_CMD_SUCCESS)

        # Copy pipeline file
        try:
            shutil.copy2(pipeline_path, self.home_pipelines)

            # Add pipeline to Operon state
            with OperonState() as operon_state:
                operon_state.insert_pipeline(
                    name=pipeline_name,
                    values={
                        'installed_date': datetime.now().strftime('%Y%b%d %H:%M:%S'),
                        'configured': False
                    }
                )
            sys.stderr.write('Pipeline {} successfully installed.\n'.format(os.path.basename(pipeline_path)))
        except (IOError, OSError, shutil.Error):
            sys.stderr.write('Pipeline at {} could not be installed into {}.\n'.format(pipeline_path,
                                                                                       self.home_pipelines))
            # TODO Why couldn't it be installed?
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

        # Attempt to install dependencies through pip
        if 'pip' not in globals():
            sys.stderr.write('Your platform or virtual environment does not appear to have pip installed.\n')
            sys.stderr.write('Dependencies cannot be installed, skipping this step.\n')
            sys.exit(EXIT_CMD_ERROR)

        pipeline_class = self.get_pipeline_class(pipeline_path)
        pipeline_dependencies = pipeline_class.dependencies()
        if pipeline_dependencies:
            sys.stderr.write('\nAttempting to install the following dependencies:\n')
            pipeline_class._print_dependencies()

            if not args['y']:
                install_depencencies = input('\nProceed with dependency installation? [y/n] ')
                if install_depencencies.lower() in {'no', 'n', 'nope', 'nada', 'nah'}:
                    sys.exit(EXIT_CMD_SUCCESS)
            for package in pipeline_dependencies:
                pip(['install', '--upgrade', package])

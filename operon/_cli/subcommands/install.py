import sys
import os
import shutil
import argparse
from datetime import datetime

import inquirer

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState

# For pip before 31 Aug 2017
try:
    from pip import main as pip
except ImportError:
    pass

# For pip on or after 31 Aug 2017
try:
    from pip._internal import main as pip
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
        return 'Install an Operon formatted pipeline.'

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
            sys.stderr.write('    Pipeline {} is already installed\n'.format(os.path.basename(pipeline_path)))
            overwrite = inquirer.prompt([inquirer.Confirm(
                'overwrite',
                message='Overwrite?'
            )]) or dict()

            # If user responds no, exit immediately
            if not overwrite.get('overwrite'):
                sys.exit(EXIT_CMD_SUCCESS)

        # Copy pipeline file
        try:
            shutil.copy2(pipeline_path, self.home_pipelines)

            # Store pipeline record in DB
            OperonState().db.insert({
                'type': 'pipeline_record',
                'name': pipeline_name,
                'installed_date': datetime.now().strftime('%Y%b%d %H:%M:%S'),
                'configured': False
            })

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

        pipeline_instance = self.get_pipeline_instance(pipeline_path)
        pipeline_dependencies = pipeline_instance.dependencies()
        if pipeline_dependencies:
            sys.stderr.write('\nAttempting to install the following dependencies:\n')
            sys.stdout.write('\n'.join(pipeline_instance.dependencies()) + '\n')

            if not args['y']:
                proceed = inquirer.prompt([inquirer.Confirm(
                    'proceed',
                    message='Proceed with dependency installation?',
                    default=True
                )]) or dict()

                # If user responds no, exit immediately
                if not proceed.get('proceed'):
                    sys.exit(EXIT_CMD_SUCCESS)

            for package in pipeline_dependencies:
                pip(['install', '--upgrade', package])

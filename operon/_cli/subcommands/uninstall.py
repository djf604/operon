import os
import sys
import argparse
import subprocess

import inquirer

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import pipeline_is_installed, get_operon_home, OperonState


def usage():
    return 'operon uninstall <pipeline-name> [-h]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Uninstall an Operon formatted pipeline.'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon uninstall', usage=usage())
        parser.add_argument('pipeline-name', help='Name of a currently installed Operon pipeline.')
        parser.add_argument('--all', help='Will uninstall all files associated with install, '
                                          'including pipeline configuration.')
        args = vars(parser.parse_args(subcommand_args))
        pipeline_name = args['pipeline-name']

        if not pipeline_is_installed(pipeline_name):
            sys.stderr.write('Pipeline {} is not installed.\n'.format(pipeline_name))
            sys.exit(0)

        try:
            confirm = inquirer.prompt([inquirer.Confirm(
                'confirm',
                message='Are you sure you want to uninstall {}?'.format(pipeline_name)
            )], raise_keyboard_interrupt=True) or dict()
            if not confirm.get('confirm'):
                sys.stderr.write('User aborted uninstall.\n')
                sys.exit(0)
        except KeyboardInterrupt:
            sys.stderr.write('User aborted uninstall.\n')

        operon_home = get_operon_home()

        # Remove from pipelines
        pipeline_file = os.path.join(operon_home, 'pipelines', '{}.py'.format(pipeline_name))
        subprocess.call(['rm', pipeline_file])

        # Remove pipeline configuration, if it exists
        pipeline_config = os.path.join(operon_home, 'configs', '{}.json'.format(pipeline_name))
        if os.path.isfile(pipeline_config) and args['all']:
            subprocess.call(['rm', pipeline_config])

        # Remove from Operon state
        OperonState().db.remove(OperonState().query.name == pipeline_name)

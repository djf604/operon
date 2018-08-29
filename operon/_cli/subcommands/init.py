import os
import sys
import argparse
import inspect
import re
import subprocess
from datetime import datetime

import operon
from operon._cli import _completer
from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState

ARGV_OPERON_HOME_ROOT = 0


def init_operon_record(operon_home_root):
    return {
        'type': 'operon_record',
        'version': operon.__version__,
        'init_date': datetime.now().strftime('%Y%b%d %H:%M:%S'),
        'home_root': operon_home_root,
        'settings': {
            'no_parsl_config_behavior': 'use_package_default',
            'delete_temporary_files': 'yes'
        }
    }


def create_or_append_to_file(comment, payload, filepath):
    payload_exists, write_mode = False, 'w'
    if os.path.isfile(filepath):
        write_mode = 'a'
        payload_exists = re.search(
            r'\n\s*' + payload.replace('-', '\-'),
            open(filepath).read()
        )
    if not payload_exists:
        with open(filepath, write_mode) as filehandle:
            filehandle.write(comment + '\n')
            filehandle.write(payload + '\n')


def make_operon_home(operon_home_root):
    operon_home = os.path.join(operon_home_root, '.operon')

    # If this location has already been init, confirm overwrite
    if os.path.isdir(operon_home):
        abort = False
        overwrite = input('Operon has already been initialized at {}\n'
                          'Would you like to remove the existing files and initialize a new '
                          'Operon structure? [y/n] '.format(operon_home_root))
        if overwrite.lower() not in {'yes', 'y'}:
            abort = True
        else:
            confirm_overwrite = input('Are you sure? This will delete all your pipelines and '
                                      'configurations [y/n] ')
            if confirm_overwrite.lower() not in {'yes', 'y'}:
                abort = True

        if abort:
            sys.stderr.write('\nAborted initialization\n')
            sys.exit(0)
        else:
            try:
                subprocess.call(['rm', '-r', '-f', operon_home])
            except subprocess.CalledProcessError:
                sys.stderr.write('Could not delete the existing Operon structure, please delete '
                                 'manually if you wish to install to {}'.format(operon_home))
                sys.exit(1)

    # If the location doesn't already exist, or user wants to overwrite, proceed
    operon_root_pipelines = os.path.join(operon_home_root, '.operon', 'pipelines')
    operon_root_configs = os.path.join(operon_home_root, '.operon', 'configs')
    try:
        # Make directory tree
        sys.stderr.write('Creating directory tree\n')
        os.makedirs(operon_root_pipelines, exist_ok=True)
        os.makedirs(operon_root_configs, exist_ok=True)

        # Add __init__.py to make modules
        sys.stderr.write('Initializing python modules\n')
        os.mknod(os.path.join(operon_root_pipelines, '__init__.py'), 0o644)
        os.mknod(os.path.join(operon_root_configs, '__init__.py'), 0o644)

        # Install completion script
        sys.stderr.write('Installing auto-completer script\n')
        operon_completer_path = os.path.join(operon_home_root, '.operon', '.operon_completer')
        with open(operon_completer_path, 'w') as operon_completer:
            operon_completer.write(inspect.getsource(_completer))
        os.chmod(operon_completer_path, 0o755)

        # Register completion program in ~/.bash_completion
        bash_completion_file = os.path.join(os.path.expanduser('~'), '.bash_completion')
        bash_completion_payload = ('if [ -e {completer_path} ]; then\n'
                                   '    complete -o bashdefault -o default -C {completer_path} operon\n'
                                   'fi').format(
            completer_path=operon_completer_path
        )
        create_or_append_to_file(
            comment='# Added by Operon pipeline development package',
            payload=bash_completion_payload,
            filepath=bash_completion_file
        )

        # Write out an empty Operon State JSON
        sys.stderr.write('Writing out empty state file\n')
        OperonState(operon_home_root=operon_home_root).db.insert(init_operon_record(operon_home_root))

        # Set OPERON_HOME to the root location in the user's .bashrc, .bash_profile, or .profile
        if operon_home_root != os.path.expanduser('~') and not os.environ.get('OPERON_HOME'):
            for preload_file in ('.bashrc', '.bash_profile'):
                if os.path.isfile(os.path.join(os.path.expanduser('~'), preload_file)):
                    break
            else:
                # If none of the above files were found, default to .profile
                preload_file = '.profile'

            create_or_append_to_file(
                comment='# Added by Operon pipeline development package',
                payload='export OPERON_HOME="{}"'.format(operon_home_root),
                filepath=os.path.join(os.path.expanduser('~'), preload_file)
            )
            sys.stderr.write('OPERON_HOME has been exported as an environment variable in {}\n'.format(
                os.path.join(os.path.expanduser('~'), preload_file)
            ))

        # Report success to the user
        sys.stderr.write('\nOperon successfully initialized at {}\n'.format(operon_home_root))
        sys.stderr.write('Please start a new shell session to complete initialization\n')
    except OSError as e:
        sys.stderr.write('An error occurred initializing Operon at {}.\n{}\n'.format(
            operon_home_root,
            str(e)
        ))


def usage():
    return 'operon init [operon_home_root]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return ('Initializes Operon at the given location.'
                'If no path is given, the user home directory is used. For any location other than '
                'the user home directory, the user '
                'needs to set a OPERON_HOME environment variable manually for Operon '
                'to use the newly created directory.')

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon init', usage=usage(), description=self.help_text())
        parser.add_argument('operon-home-root', default='', nargs='?',
                            help=('Operon will initialize in this directory. '
                                  'Defaults to the user home directory.'))
        args = vars(parser.parse_args(subcommand_args))

        # If input is help, display help message
        if args['operon-home-root'].strip().lower() == 'help':
            parser.print_help()
            sys.exit(0)

        # Default to environment var OPERON_HOME if set, otherwise user defined location
        make_operon_home(args['operon-home-root'] or os.environ.get('OPERON_HOME') or os.path.expanduser('~'))

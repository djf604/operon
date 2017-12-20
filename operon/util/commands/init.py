import os
import sys
import errno
import argparse
from operon.util.commands import BaseCommand

ARGV_OPERON_HOME_ROOT = 0


# Based off https://stackoverflow.com/a/600612/1539628
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as os_error:
        if os_error.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def make_operon_home(operon_home_root):
    operon_root_pipelines = os.path.join(operon_home_root, '.operon', 'pipelines')
    operon_root_configs = os.path.join(operon_home_root, '.operon', 'configs')
    try:
        # Make directory tree
        mkdir_p(operon_root_pipelines)
        mkdir_p(operon_root_configs)

        # Add __init__.py to make modules
        os.mknod(os.path.join(operon_root_pipelines, '__init__.py'), 0o644)
        os.mknod(os.path.join(operon_root_configs, '__init__.py'), 0o644)

        # Write out user messages
        sys.stderr.write('Operon successfully initialized at {}\n'.format(operon_home_root))
        if operon_home_root != os.path.expanduser('~') and not os.environ.get('OPERON_HOME'):
            sys.stderr.write('Please set a OPERON_HOME environment variable to {}\n'.format(operon_home_root))
    except OSError as e:
        sys.stderr.write('An error occurred initializing Operon at {}.\n{}\n'.format(
            operon_home_root,
            str(e)
        ))


def usage():
    return 'operon init [operon_home_root]'


class Command(BaseCommand):
    def help_text(self):
        return ('Initializes Operon at the given location.'
                'If no path is given, the user home directory is used. For any location other than '
                'the user home directory, the user '
                'needs to set a OPERON_HOME environment variable manually for Operon '
                'to use the newly created directory.')

    def run(self, command_args):
        parser = argparse.ArgumentParser(prog='operon init', usage=usage(), description=self.help_text())
        parser.add_argument('operon-home-root', default='', nargs='?',
                            help=('Operon will initialize in this directory. '
                                  'Defaults to the user home directory.'))
        args = vars(parser.parse_args(command_args))

        # If input is help, display help message
        if args['operon-home-root'].strip().lower() == 'help':
            parser.print_help()
            sys.exit(0)

        # Default to environment var OPERON_HOME if set, otherwise user defined location
        make_operon_home(args['operon-home-root'] or os.environ.get('OPERON_HOME') or os.path.expanduser('~'))

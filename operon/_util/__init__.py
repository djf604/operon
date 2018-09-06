import os
import sys
import traceback
import argparse
import logging as pylogging

from operon import __version__ as operon_version
from operon._cli import get_operon_subcommands


def print_no_init():
    sys.stderr.write('Operon cannot find an init directory at the user ' +
                     'home or in the OPERON_HOME environment variable. Please ' +
                     'run \'operon init\' before using Operon.\n')


def execute_from_command_line(argv=None):
    argv = argv or sys.argv[:]

    # Get subcommand classes
    operon_subcommand_classes = get_operon_subcommands(classes=True)

    # Create subparsers
    parser = argparse.ArgumentParser(prog='operon')
    parser.add_argument('--version', action='version', version=operon_version)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('subcommand')
    parser.add_argument('subcommandargs', nargs=argparse.REMAINDER)

    if len(sys.argv) == 1:
        # If no arguments were given, print help
        parser.print_help()
    else:
        # Otherwise, run subcommand Command class, passing in all arguments after subcommand
        args = vars(parser.parse_args())
        if args['debug']:
            pylogging.getLogger('operon.main').setLevel(pylogging.DEBUG)

        if args['subcommand'].lower() == 'help':
            parser.print_help()
            sys.exit(0)
        try:
            operon_subcommand_classes[args['subcommand'].replace('-', '_')].run(args['subcommandargs'])
        except Exception as e:
            sys.stderr.write('Operon encountered an error when trying to execute {}:\n'.format(args['subcommand']))
            sys.stderr.write(str(e) + '\n')
            traceback.print_exc()

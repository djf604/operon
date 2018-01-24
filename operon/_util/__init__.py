import os
import sys
import traceback
import argparse
import logging
from datetime import datetime

import operon._cli.subcommands
from operon._cli.subcommands import fetch_subcommand_class, get_operon_subcommands


def setup_logger(logs_dir=None):
    logger = logging.getLogger('operon.main')
    logger.setLevel(logging.DEBUG)
    logformat = logging.Formatter(fmt='[{asctime}|{name}] > {message}', datefmt='%d%b%Y %H:%M:%S', style='{')

    if logs_dir is not None:
        operon_log_filename = 'run_{}.operon.log'.format(datetime.now().strftime('%d%b%Y_%H%M%S'))
        logfilehandler = logging.FileHandler(os.path.join(logs_dir, operon_log_filename))
        logfilehandler.setFormatter(logformat)
        logger.addHandler(logfilehandler)

    ch = logging.StreamHandler()
    ch.setFormatter(logformat)
    logger.addHandler(ch)


def print_no_init():
    sys.stderr.write('Operon cannot find an init directory at the user ' +
                     'home or in the OPERON_HOME environment variable. Please ' +
                     'run \'operon init\' before using Operon.\n')


def print_help_text():
    fetch_subcommand_class('help').run_from_argv()


def print_unrecognized_command(subcommand):
    sys.stderr.write('Unrecognized command: {}\n\n'.format(subcommand))
    sys.stderr.write('Use one of the following:\n')
    print_help_text()


def execute_from_command_line(argv=None):
    argv = argv or sys.argv[:]

    # Get subcommand classes
    operon_subcommand_classes = get_operon_subcommands(classes=True)

    # Create subparsers
    parser = argparse.ArgumentParser(prog='operon')
    parser.add_argument('--version', action='version', version=operon.__version__)
    subparsers = parser.add_subparsers(dest='subcommand',
                                       metavar='[{}]'.format(', '.join(operon_subcommand_classes.keys())))

    # Add a subparser for each subcommand
    for operon_subcommand, operon_subcommand_class in operon_subcommand_classes.items():
        subparsers.add_parser(operon_subcommand.replace('_', '-'), help=operon_subcommand_class.help_text())

    if len(sys.argv) == 1:
        # If no arguments were given, print help
        parser.print_help()
    else:
        # Otherwise, run subcommand Command class, passing in all arguments after subcommand
        subcommand = vars(parser.parse_args(argv[1:2])).get('subcommand', 'help')
        if subcommand.lower() == 'help':
            parser.print_help()
            sys.exit(0)
        try:
            operon_subcommand_classes[subcommand].run(argv[2:])
        except Exception as e:
            sys.stderr.write('Operon encountered an error when trying to execute {}:\n'.format(subcommand))
            sys.stderr.write(str(e) + '\n')
            traceback.print_exc()

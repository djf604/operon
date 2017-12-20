import sys
import os
import traceback
import argparse
from importlib import import_module

import pkgutil
import operon.util.commands
from operon import __version__
import six


def fetch_command_class(subcommand):
    module = import_module('operon.util.commands.{}'.format(subcommand))
    return module.Command()


def print_no_init():
    sys.stderr.write('Operon cannot find an init directory at the user ' +
                     'home or in the OPERON_HOME environment variable. Please ' +
                     'run \'operon init\' before using Operon.\n')


def print_help_text():
    fetch_command_class('help').run_from_argv()


def print_unrecognized_command(subcommand):
    sys.stderr.write('Unrecognized command: {}\n\n'.format(subcommand))
    sys.stderr.write('Use one of the following:\n')
    print_help_text()


def add_common_pipeline_args(parser):
    parser.add_argument('--reads', required=True, action='append',
                        help=('Raw reads to process with this pipeline. Paired-end reads ' +
                              'can be joined together with a colon (:). Specify this option ' +
                              'multiple times to process multiple raw reads files.\nEx ' +
                              'paired-end: --reads read1.fastq:read2.fastq\nEx single-end: ' +
                              '--reads sample1.fastq sample1.extra.fastq'))
    parser.add_argument('--output', required=True,
                        help='Directory to store all results of this pipeline in.')
    parser.add_argument('--log')


def execute_from_command_line(argv=None):
    argv = argv or sys.argv[:]

    # Get command classes
    operon_command_classes = {operon_command: fetch_command_class(operon_command)
                              for operon_command in [
                                  name for _, name, _
                                  in pkgutil.iter_modules(operon.util.commands.__path__)
                                  ]
                              }

    # Create subparsers
    parser = argparse.ArgumentParser(prog='operon')
    parser.add_argument('--version', action='version', version=__version__)
    subparsers = parser.add_subparsers(dest='subcommand',
                                       metavar='[{}]'.format(', '.join(operon_command_classes.keys())))

    # Add a subparser for each subcommand
    for operon_command, operon_command_class in six.iteritems(operon_command_classes):
        subparsers.add_parser(operon_command.replace('_', '-'), help=operon_command_class.help_text())

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
            operon_command_classes[subcommand].run(argv[2:])
        except Exception as e:
            sys.stderr.write('Operon encountered an error when trying to execute {}:\n'.format(subcommand))
            sys.stderr.write(str(e) + '\n')
            traceback.print_exc()

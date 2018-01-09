import sys
import os
import traceback
import argparse
import json
from importlib import import_module
import importlib.util

import pkgutil
import operon.util.commands


def load_pipeline_file(pipeline_filepath):
    """
    This only works in Python 3.5+
    Taken from https://stackoverflow.com/a/67692/1539628
    :param pipeline_filepath:
    :return:
    """
    spec = importlib.util.spec_from_file_location('operon.pipeline', pipeline_filepath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class OperonState(object):
    def __init__(self):
        # Get current state
        operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
        self.state_json_path = os.path.join(operon_home_root, '.operon', 'operon_state.json')
        with open(self.state_json_path) as operon_state:
            self.state = json.load(operon_state)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        with open(self.state_json_path, 'w') as operon_state:
            json.dump(self.state, operon_state, indent=2)

    def insert_pipeline(self, name, values):
        self.state['pipelines'][name] = values

    def remove_pipeline(self, name):
        self.state['pipelines'].pop(name, None)



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


def get_operon_subcommands(classes=False):
    operon_subcommands = [
        operon_subcommand
        for operon_subcommand in [
            name for _, name, _
            in pkgutil.iter_modules(operon.util.commands.__path__)
        ]
    ]
    if not classes:
        return operon_subcommands

    return {
        operon_subcommand: fetch_command_class(operon_subcommand)
        for operon_subcommand in operon_subcommands
    }


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

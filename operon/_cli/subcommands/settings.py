import sys
import argparse

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState
from operon._util import settings

BASENAME = 0
EXTENSION = 1


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'View or modify global Operon settings'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon settings')
        parser.add_argument('--interactive', action='store_true', help='Start interactive settings session.')
        parser.add_argument('key', nargs='?', help='The settings key to manipulate.')
        parser.add_argument('value', nargs='?', help='The value to set on the key.')
        args = vars(parser.parse_args(subcommand_args))

        # If no arguments are given, display all settings and possible values
        if not (args['key'] and args['value']):
            sys.stdout.write('Operon settings and possible options:\n')
            for key, definitions in settings.schema.items():
                sys.stdout.write('\n{}\n'.format(key))
                for definition in definitions:
                    sys.stdout.write('\t{}: {}\n'.format(definition['option'], definition['description']))
            return

        # If arguments are given, assert whether they're valid
        key_exists = args['key'] in settings.schema.keys()
        value_valid = None
        if key_exists:
            value_valid = args['value'] in {_op['option'] for _op in settings.schema[args['key']]}

        # Set the key, or if the key or value isn't valid, inform the user
        if key_exists and value_valid:
            OperonState().setting(args['key'], args['value'])
            sys.stdout.write('Successfully set key {} to {}\n'.format(args['key'], args['value']))
        elif not key_exists:
            sys.stdout.write('Key {} is not a valid setting\n'.format(args['key']))
        else:
            sys.stdout.write('{} is not a valid value for key {}\n'.format(args['value'], args['key']))

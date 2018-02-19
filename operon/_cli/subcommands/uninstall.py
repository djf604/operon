import argparse

from operon._cli.subcommands import BaseSubcommand


def usage():
    return 'operon uninstall <pipeline-name> [-h]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Uninstall an Operon formatted pipeline.'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon uninstall', usage=usage())
        parser.add_argument('pipeline-name', help='Name of a currently installed Operon pipeline.')
        args = vars(parser.parse_args(subcommand_args))
        pipeline_name = args['pipeline-name']

        # Remove from pipelines

        # Remove pipeline configuration, if it exists

        # Remove from Operon state
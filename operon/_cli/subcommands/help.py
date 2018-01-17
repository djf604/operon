from operon._cli.subcommands import BaseSubcommand


class Subcommand(BaseSubcommand):
    """
    This class only exists so argparse will display help as
    a subcommand.
    """
    def help_text(self):
        return 'Show this help message and exit.'

    def run(self, subcommand_args):
        return

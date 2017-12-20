from operon.util.commands import BaseCommand


class Command(BaseCommand):
    """
    This class only exists so argparse will display help as
    a subcommand.
    """
    def help_text(self):
        return 'Show this help message and exit.'

    def run(self, command_args):
        return

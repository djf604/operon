import os
import sys
import argparse

import blessings

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState

BASENAME = 0
EXTENSION = 1


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Lists installed pipelines in the default Operon home directory.'

    def run(self, subcommand_args):
        # Get argparse help functionality
        argparse.ArgumentParser(prog='operon list', description=self.help_text()).parse_args(subcommand_args)
        term = blessings.Terminal()

        sys.stderr.write('Installed pipelines (in {}):\n\n'.format(self.home_pipelines))

        # TODO If ever a third column is added, this won't work
        table_headers = ['Pipeline Name', 'Configured?']
        conversion = [
            None,
            lambda c: 'Yes' if c else term.red('No')
        ]

        pipelines_state = OperonState().pipelines_configured()
        rows = [''] * (len(pipelines_state) + 1)

        for col_i, col_contents in enumerate(zip(*pipelines_state)):
            col_header = table_headers[col_i]
            if conversion[col_i]:
                col_contents = tuple(map(conversion[col_i], col_contents))
            longest = max([len(name) for name in col_contents + (col_header,)]) + 2

            # Output underlined header
            rows[0] += '{content}{padding}'.format(
                content=term.underline(col_header),
                padding=' ' * (longest - len(col_header))
            )

            # Output content
            for row_i, content in enumerate(col_contents, start=1):
                rows[row_i] += '{content}{padding}'.format(
                    content=content,
                    padding=' ' * (longest - len(content))
                )

        # Output list table
        for row in rows:
            print(row)

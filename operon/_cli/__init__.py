import os
import pkgutil
from importlib import import_module


def get_operon_subcommands(classes=False):
    operon_subcommands = [
        operon_subcommand
        for operon_subcommand in [
            name for _, name, _
            in pkgutil.iter_modules([os.path.join(__path__[0], 'subcommands')])
        ]
    ]
    if not classes:
        return [o.replace('_', '-') for o in operon_subcommands]

    return {
        operon_subcommand: fetch_subcommand_class(operon_subcommand)
        for operon_subcommand in operon_subcommands
    }


def fetch_subcommand_class(subcommand):
    module = import_module('operon._cli.subcommands.{}'.format(subcommand))
    return module.Subcommand()
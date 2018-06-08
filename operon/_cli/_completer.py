#!/usr/bin/env python
import os
import sys
import subprocess
try:
    from operon._cli import get_operon_subcommands
    from operon._util.home import OperonState
except ImportError:
    sys.exit()

COMPGEN = 'compgen -W "{options}" -- "{stub}"'


def get_pipeline_options():
    return ' '.join([_p['name'] for _p in OperonState().db.search(OperonState().query.type == 'pipeline_record')])


def get_completion_options(options, stub):
    if not options:
        return ''
    try:
        return subprocess.check_output(COMPGEN.format(
            options=options,
            stub=stub
        ), shell=True, executable=os.environ['SHELL']).decode()
    except subprocess.CalledProcessError:
        return ''


def completer():
    phrase = os.environ['COMP_LINE'].split(' ')
    stub_token = phrase[-1].strip()
    num_completed_tokens = len(phrase[:-1])

    completion_options = ''
    if num_completed_tokens == 1:
        completion_options = get_completion_options(
            options=' '.join([_sub.replace('_', '-') for _sub in get_operon_subcommands()]),
            stub=stub_token
        )
    elif num_completed_tokens == 2:
        if phrase[-2] in {'run', 'batch-run', 'configure', 'show', 'uninstall'}:
            completion_options = get_completion_options(
                options=get_pipeline_options(),
                stub=stub_token
            )

    # Output completion options to the shell completer
    print(completion_options)


if __name__ == '__main__':
    completer()

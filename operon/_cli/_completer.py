#!/usr/bin/env python
import os
import sys
import subprocess
try:
    from operon._cli import get_operon_subcommands
    from operon._util.home import OperonState
    from operon import COMPLETER_VERSION
except ImportError:
    sys.exit()

COMPGEN = 'compgen -W "{options}" -- "{stub}"'
VERSION = 1
SEMANTIC_VERSION = '0.1.8'


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
    # Run version check, self-update if necessary
    if COMPLETER_VERSION > VERSION:
        try:
            import inspect
            from operon._cli import _completer
            completer_path = os.path.abspath(__file__)
            with open(completer_path, 'w') as operon_completer:
                operon_completer.write(inspect.getsource(_completer))
            os.chmod(completer_path, 0o755)
        except:
            pass

    completer()

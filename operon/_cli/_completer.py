#!/usr/bin/env python
import os
import sys
import subprocess
import json
try:
    from operon._cli.subcommands import get_operon_subcommands
except ImportError:
    sys.exit()

COMPGEN = 'compgen -W "{options}" -- "{stub}"'


def get_pipeline_options():
    operon_home_root = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
    operon_state_json_path = os.path.join(operon_home_root, '.operon', 'operon_state.json')
    try:
        with open(operon_state_json_path) as operon_state_json:
            operon_state = json.load(operon_state_json)
            return ' '.join(operon_state['pipelines'].keys())
    except OSError:
        return ''


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
            options=' '.join(get_operon_subcommands()),
            stub=stub_token
        )
    elif num_completed_tokens == 2:
        if phrase[-2] in {'run', 'configure', 'show'}:
            completion_options = get_completion_options(
                options=get_pipeline_options(),
                stub=stub_token
            )

    # Output completion options to the shell completer
    print(completion_options)


if __name__ == '__main__':
    completer()

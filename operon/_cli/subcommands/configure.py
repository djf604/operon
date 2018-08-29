import sys
import os
import argparse
import json
import readline
import subprocess
import re
from collections import defaultdict

import inquirer

from operon._cli.subcommands import BaseSubcommand
from operon._util.home import OperonState, file_appears_installed
from operon._util.errors import MalformedPipelineConfigError

ARGV_PIPELINE_NAME = 0
ARGV_FIRST_ARGUMENT = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2
FILENAME_BASE = 0
INQUIRER_TYPES = {'text', 'path', 'confirm', 'list', 'checkbox', 'password'}

readline.set_completer_delims(' \t\n;')
readline.parse_and_bind('tab: complete')


def configure(config_dict, current_config=None, breadcrumb=None, questions=None):
    # Initialize if this is the outermost recursion
    if current_config is None:
        current_config = dict()
    if breadcrumb is None:
        questions = list()
        breadcrumb = 'root'

    # Go through all keys
    for key in config_dict:
        current_breadcrumb = '__'.join((breadcrumb, key))
        if isinstance(config_dict[key], str):
            # This is a string-type leaf
            # Add either a Path or Text question, depending on the key
            q_type = 'Path' if 'path' in key.lower() else 'Text'
            questions.append(getattr(inquirer, q_type)(
                name=current_breadcrumb,
                message=config_dict[key],
                default=current_config.get(key)
            ))
        elif isinstance(config_dict[key], dict) and 'q_type' in config_dict[key]:
            # This is a dict type leaf
            q_config = config_dict[key]

            # Ensure this is a valid question type
            if q_config['q_type'].strip().lower() not in INQUIRER_TYPES:
                raise ValueError('q_type must be one of {{{}}}'.format(', '.join(INQUIRER_TYPES)))

            # Get and remove q_type from question config
            q_type = q_config.pop('q_type').strip().capitalize()

            # Inject default, depending on question config
            if 'always_default' in q_config:
                q_config.pop('always_default')
            elif current_config.get(key):
                q_config['default'] = current_config.get(key)

            # Add question
            questions.append(getattr(inquirer, q_type)(
                name=current_breadcrumb,
                **q_config
            ))
        elif isinstance(config_dict[key], dict):
            # This is an inner configuration dictionary, so recurse
            configure(
                config_dict[key],
                current_config.get(key, dict()),
                breadcrumb=current_breadcrumb,
                questions=questions
            )
        else:
            raise MalformedPipelineConfigError('Encountered unknown object: {}'.format(config_dict[key]))

    # If this is the outermost recursion, ask questions and return results
    if breadcrumb == 'root':
        user_input = inquirer.prompt(questions, raise_keyboard_interrupt=True)

        # Inflate answers into original config dictionary
        tree = lambda: defaultdict(tree)
        root = tree()
        for key, value in user_input.items():
            # Get breadcrumbs to leaf with out root node
            breadcrumbs = key.split('__')[1:]

            # Get the inner dictionary needed
            if len(breadcrumbs) > 1:
                inner = root[breadcrumbs[0]]
                for _ in breadcrumbs[1:-1]:
                    inner = inner[_]

                # Set value of inner dictionary
                inner[breadcrumbs[-1]] = value
            else:
                root[breadcrumbs[0]] = value

        # Convert whole thing to regular dict, return
        return json.loads(json.dumps(root))


def create_conda_environment(name, config, conda_path, reinstall=False):
    if reinstall:
        subprocess.call([conda_path, 'env', 'remove', '--yes', '--name', name])

    # Install proper channels
    new_channels = config.get('channels') or ['r', 'defaults', 'conda-forge', 'bioconda']
    packages = [pkg.tag for pkg in config.get('packages', list())]

    # Save old channels configuration
    show_sources = subprocess.check_output([conda_path, 'config', '--show-sources']).decode()
    old_channels = re.findall(r'\s+- (\S+)\n', show_sources)

    # Install new channels temporarily
    for channel in new_channels:
        subprocess.call([conda_path, 'config', '--add', 'channels', channel])

    # Create new conda environment and install packages
    subprocess.call([conda_path, 'create', '--yes', '--name', name] + packages)

    # Uninstall new channels
    for channel in new_channels:
        subprocess.call([conda_path, 'config', '--remove', 'channels', channel])

    # Reinstate old order of old channels
    for channel in old_channels[::-1]:
        subprocess.call([conda_path, 'config', '--add', 'channels', channel])


def conda_env_exists(conda_path, conda_env_name):
    conda_list_cmd = '{} env list'.format(conda_path)
    return conda_env_name in {
        env_name.split()[0]
        for env_name in subprocess.check_output(conda_list_cmd, shell=True).decode().split('\n')
        if '__operon__' in env_name
    }


def get_conda_env_location(conda_path, conda_env_name):
    conda_list_cmd = '{} env list'.format(conda_path)
    return [
        env_name.split()[1]
        for env_name in subprocess.check_output(conda_list_cmd, shell=True).decode().split('\n')
        if conda_env_name in env_name
    ][0]


class Subcommand(BaseSubcommand):
    @staticmethod
    def usage():
        return 'operon configure <pipeline-name> [-h] [--location LOCATION]'

    def help_text(self):
        return 'Create a configuration file for a pipeline.'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon configure', usage=self.usage(), description=self.help_text())
        parser.add_argument('--location', help=('Path to which to save this config file. ' +
                                                'Defaults to install directory.'))

        if not subcommand_args or subcommand_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        pipeline_name = subcommand_args[ARGV_PIPELINE_NAME]
        # Check if pipeline_name is a path, if so ask for confirmation
        if file_appears_installed(pipeline_name):
            sys.stderr.write(
                'There appears to be an installed pipeline called {}, are you sure '
                'you mean to generate a configuration for the file {}?\n'.format(
                    os.path.splitext(os.path.basename(pipeline_name))[FILENAME_BASE],
                    pipeline_name
                )
            )
            confirmed_configure_file = inquirer.prompt([inquirer.Confirm(
                'confirmed_configure_file',
                message='Confirm?'
            )]) or dict()

            if not confirmed_configure_file.get('confirmed_configure_file'):
                sys.stderr.write('User aborted configuration.\n')
                sys.exit(EXIT_CMD_SUCCESS)

        if os.path.isfile(pipeline_name):
            pipeline_name_base = os.path.splitext(os.path.basename(pipeline_name))[FILENAME_BASE]
            if pipeline_name_base in OperonState().pipelines_installed():
                sys.stderr.write(
                    'There appears to be an installed pipeline called {}, are you sure '
                    'you mean to generate a configuration for the file {}?\n'.format(pipeline_name_base, pipeline_name)
                )
                confirmed_configure_file = inquirer.prompt([inquirer.Confirm(
                    'confirmed_configure_file',
                    message='Confirm?'
                )]) or dict()

                if not confirmed_configure_file.get('confirmed_configure_file'):
                    sys.stderr.write('User aborted configuration.\n')
                    sys.exit(EXIT_CMD_SUCCESS)

        # If pipeline is in the system, or user confirmed yes, continue
        pipeline_instance = self.get_pipeline_instance(pipeline_name)

        if pipeline_instance is not None:
            # Parse configure options
            config_args_parser = argparse.ArgumentParser(prog='operon configure {}'.format(pipeline_name))
            config_args_parser.add_argument('--location',
                                            default=os.path.join(self.home_configs,
                                                                 '{}.json'.format(pipeline_name)),
                                            help=('Path to which to save this config file. '
                                                  'Defaults to install directory.'))
            configure_args = vars(config_args_parser.parse_args(subcommand_args[1:]))

            save_location = configure_args['location']

            # If this config already exists, prompt user before overwrite
            if os.path.isfile(save_location):
                sys.stderr.write('    Configuration for {} already exists at {}\n'.format(pipeline_name, save_location))
                overwrite = inquirer.prompt([inquirer.Confirm(
                    'overwrite',
                    message='Overwrite?'
                )]) or dict()

                # If user responds no, exit immediately
                if not overwrite.get('overwrite'):
                    sys.stderr.write('User aborted configuration.\n')
                    sys.exit(EXIT_CMD_SUCCESS)

            # If the pipeline contains a listing of conda packages, and the user has conda installed and in
            # PATH, then ask if the user wants to use conda to install packages
            use_conda_paths, conda_path = False, None
            conda_env_name = '__operon__{}'.format(pipeline_name)
            pipeline_conda_config = pipeline_instance.conda()
            if pipeline_conda_config.get('packages'):
                try:
                    conda_path = subprocess.check_output('which conda', shell=True).strip().decode()
                    if conda_env_exists(conda_path, conda_env_name):
                        # If conda env already exists, give the user choices on how to proceed
                        sys.stderr.write('    A conda environment for this pipeline already exists.\n')
                        env_exists_answer = inquirer.prompt([inquirer.List(
                            'conda_env_exists',
                            message='What would you like to do?',
                            choices=[
                                'Use the installed environment to populate software paths',
                                'Ignore the installed environment',
                                'Reinstall the environment'
                            ]
                        )], raise_keyboard_interrupt=True).get('conda_env_exists')

                        if env_exists_answer != 'Ignore the installed environment':
                            use_conda_paths = True

                        if env_exists_answer == 'Reinstall the environment':
                            create_conda_environment(
                                conda_env_name,
                                pipeline_conda_config,
                                conda_path,
                                reinstall=True
                            )
                    else:
                        # If conda env doesn't yet exist, ask if user wants to create it
                        sys.stderr.write(
                            'Conda is installed, but no environment has been created for this '
                            'pipeline.\nOperon can use conda to download the software this '
                            'pipeline uses and inject those into your configuration.\n'
                        )
                        use_conda_paths = inquirer.prompt([inquirer.Confirm(
                            'use_conda_paths',
                            message='Would you like to download the software now?',
                            default=True
                        )], raise_keyboard_interrupt=True).get('use_conda_paths')

                        if use_conda_paths:
                            create_conda_environment(conda_env_name, pipeline_conda_config, conda_path)
                except subprocess.CalledProcessError:
                    pass  # If user doesn't have conda installed, do nothing and continue
                except KeyboardInterrupt:
                    sys.stderr.write('User aborted configuration.\n')
                    sys.exit(EXIT_CMD_SUCCESS)

            # Get configuration from pipeline, recursively prompt user to fill in info
            config_dict = pipeline_instance.configuration()
            if not isinstance(config_dict, dict):
                raise MalformedPipelineConfigError('Outermost object is not a dictionary')
            config_dict['parsl_config_path'] = 'Path to a parsl configuration to use (leave blank to skip)'
            try:
                current_config = json.loads(open(os.path.join(self.home_configs,
                                                              '{}.json'.format(pipeline_name))).read())
            except (json.JSONDecodeError, FileNotFoundError):
                current_config = dict()

            # If user wants to use conda, inject paths into config
            if use_conda_paths and conda_path:
                conda_env_location = get_conda_env_location(conda_path, conda_env_name)
                for conda_package in pipeline_conda_config.get('packages', list()):
                    software_path = (
                        os.path.join(conda_env_location, conda_package.executable_path)
                        if conda_package.executable_path
                        else os.path.join(conda_env_location, 'bin', conda_package.tag.split('=')[0])
                    )
                    if conda_package.config_key not in current_config:
                        current_config[conda_package.config_key] = dict()
                    current_config[conda_package.config_key]['path'] = software_path

            try:
                populated_config_dict = configure(config_dict, current_config)
            except (KeyboardInterrupt, EOFError):
                sys.stderr.write('\nUser aborted configuration.\n')
                sys.exit(EXIT_CMD_SUCCESS)
            except AttributeError as e:
                raise MalformedPipelineConfigError('Something about the configuration is malformed: {}'.format(e))

            # Write config out to file
            try:
                with open(save_location, 'w') as config_output:
                    config_output.write(json.dumps(populated_config_dict, indent=2) + '\n')

                # Set pipeline to configured in Operon state
                OperonState().db.update({'configured': True}, OperonState().query.name == pipeline_name)

                sys.stderr.write('Configuration file successfully written.\n')
            except IOError:
                sys.stderr.write('Could not open file for writing.\n')
                sys.exit(1)
        else:
            # If pipeline class doesn't exist, exit immediately
            sys.stderr.write('Pipeline {name} does not exist in {home}\n'.format(
                name=pipeline_name,
                home=self.home_pipelines + '/'
            ))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

import sys
import os
import argparse
import json
import readline
import subprocess
import re

from operon._cli.subcommands import BaseSubcommand

ARGV_PIPELINE_NAME = 0
ARGV_FIRST_ARGUMENT = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2

readline.set_completer_delims(' \t\n;')
readline.parse_and_bind('tab: complete')


def create_conda_environment(name, config, conda_path):
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


def configure(config_dict, current_config, breadcrumbs, blank=False):
    for key in config_dict:
        if isinstance(config_dict[key], dict):
            configure(config_dict[key], current_config.get(key, {}), '|'.join((breadcrumbs, key)), blank)
        else:
            if not blank:
                prompt = '|'.join((breadcrumbs, config_dict[key])).strip().strip(':')
                config_dict[key] = (input(prompt + ' [{}]: '.format(current_config.get(key, ''))) or
                                    current_config.get(key, ''))
            else:
                config_dict[key] = ''


class Subcommand(BaseSubcommand):
    @staticmethod
    def usage():
        return 'operon configure <pipeline-name> [-h] [--location LOCATION] [--blank]'

    def help_text(self):
        return 'Create a configuration file for a pipeline.'

    def run(self, subcommand_args):
        parser = argparse.ArgumentParser(prog='operon configure', usage=self.usage(), description=self.help_text())
        parser.add_argument('--location', help=('Path to which to save this config file. ' +
                                                'Defaults to install directory.'))
        parser.add_argument('--blank', action='store_true',
                            help='Skip configuration and create a blank configuration file.')

        if not subcommand_args or subcommand_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        pipeline_name = subcommand_args[ARGV_PIPELINE_NAME]
        pipeline_class = self.get_pipeline_class(pipeline_name)

        if pipeline_class is not None:
            # Parse configure options
            config_args_parser = argparse.ArgumentParser(prog='operon configure {}'.format(pipeline_name))
            config_args_parser.add_argument('--location',
                                            default=os.path.join(self.home_configs,
                                                                 '{}.json'.format(pipeline_name)),
                                            help=('Path to which to save this config file. '
                                                  'Defaults to install directory.'))
            config_args_parser.add_argument('--blank', action='store_true',
                                            help='Skip configuration and create a blank configuration file.')
            configure_args = vars(config_args_parser.parse_args(subcommand_args[1:]))

            save_location = configure_args['location']
            is_blank = configure_args['blank']

            # If this config already exists, prompt user before overwrite
            if os.path.isfile(save_location):
                overwrite = input('Configuration for {} already exists at {}, overwrite? [y/n] '.format(
                    pipeline_name,
                    save_location
                ))

                # If user responds no, exit immediately
                if overwrite.lower() in {'no', 'n'}:
                    sys.stderr.write('\nUser aborted configuration.\n')
                    sys.exit(EXIT_CMD_SUCCESS)

            # If the pipeline contains a listing of conda packages, and the user has conda installed and in
            # PATH, then ask if the user wants to use conda to install packages
            use_conda_paths = False
            conda_env_name = '__operon__{}'.format(pipeline_name)
            conda_envs_location = None
            pipeline_conda_config = pipeline_class.conda()
            if pipeline_conda_config.get('packages'):
                try:
                    conda_path = subprocess.check_output('which conda', shell=True).strip().decode()
                    conda_envs_location = os.path.join(os.path.split(os.path.split(conda_path)[0])[0], 'envs')
                    if os.path.isdir(os.path.join(conda_envs_location, conda_env_name)):
                        # If conda env already exists
                        ask_use_conda = input('A conda environment for this pipeline already exists, would you '
                                              'like to use it to populate software paths? [y/n] ')
                        if ask_use_conda.lower().strip() not in {'no', 'n'}:
                            use_conda_paths = True
                    else:
                        # If conda env doesn't yet exist, ask if user wants to create it
                        ask_create_conda = input('Conda is installed, but no environment has been created for this '
                                                 'pipline.\nOperon can use conda to download the software this '
                                                 'pipeline uses and inject those into your configuration.\nWould you '
                                                 'like to download the software now? [y/n] ')
                        if ask_create_conda.lower().strip() not in {'no', 'n'}:
                            use_conda_paths = True
                            # Create new conda environment
                            create_conda_environment(conda_env_name, pipeline_conda_config, conda_path)
                except subprocess.CalledProcessError:
                    pass  # If user doesn't have conda installed, do nothing and continue

            # Get configuration from pipeline, recursively prompt user to fill in info
            config_dict = pipeline_class.configure()
            try:
                current_config = json.loads(open(os.path.join(self.home_configs,
                                                              '{}.json'.format(pipeline_name))).read())
            except:
                current_config = {}

            # If user wants to use conda, inject paths into config
            if use_conda_paths:
                conda_env_location = os.path.join(conda_envs_location, conda_env_name)
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
                configure(config_dict, current_config, '', is_blank)
                if is_blank:
                    sys.stderr.write('Blank configuration generated.\n')
            except (KeyboardInterrupt, EOFError):
                sys.stderr.write('\nUser aborted configuration.\n')
                sys.exit(EXIT_CMD_SUCCESS)

            # Write config out to file
            try:
                with open(save_location, 'w') as config_output:
                    config_output.write(json.dumps(config_dict, indent=4) + '\n')
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

import sys
import os
import argparse
import json
import readline
from operon.util.commands import BaseCommand

import six.moves

ARGV_PIPELINE_NAME = 0
ARGV_FIRST_ARGUMENT = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2

readline.set_completer_delims(' \t\n;')
readline.parse_and_bind('tab: complete')


class Command(BaseCommand):
    @staticmethod
    def usage():
        return 'operon configure <pipeline-name> [-h] [--location LOCATION] [--blank]'

    def configure(self, config_dict, current_config, blank=False):
        for key in config_dict:
            if type(config_dict[key]) == dict:
                self.configure(config_dict[key], current_config.get(key, {}), blank)
            else:
                if not blank:
                    prompt = config_dict[key].strip().strip(':')
                    config_dict[key] = (six.moves.input(prompt + ' [{}]: '.format(current_config.get(key, ''))) or
                                        current_config.get(key, ''))
                else:
                    config_dict[key] = ''

    def help_text(self):
        return 'Create a configuration file for a pipeline.'

    def run(self, command_args):
        parser = argparse.ArgumentParser(prog='operon configure', usage=self.usage(), description=self.help_text())
        parser.add_argument('--location', help=('Path to which to save this config file. ' +
                                                'Defaults to install directory.'))
        parser.add_argument('--blank', action='store_true',
                            help='Skip configuration and create a blank configuration file.')

        if not command_args or command_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        pipeline_name = command_args[ARGV_PIPELINE_NAME]
        pipeline_class = self.get_pipeline_class(pipeline_name)

        if pipeline_class is not None:
            # Parse configure options
            config_args_parser = argparse.ArgumentParser(prog='operon configure {}'.format(pipeline_name))
            config_args_parser.add_argument('--location',
                                            default=os.path.join(self.home_configs,
                                                                 '{}.json'.format(pipeline_name)),
                                            help=('Path to which to save this config file. ' +
                                                  'Defaults to install directory.'))
            config_args_parser.add_argument('--blank', action='store_true',
                                            help='Skip configuration and create a blank configuration file.')
            configure_args = vars(config_args_parser.parse_args(command_args[1:]))

            save_location = configure_args['location']
            is_blank = configure_args['blank']

            # If this config already exists, prompt user before overwrite
            if os.path.isfile(save_location):
                overwrite = six.moves.input('Config for {} already exists at {}, overwrite? [y/n] '.format(
                    pipeline_name,
                    save_location
                ))

                # If user responds no, exit immediately
                if overwrite.lower() in {'no', 'n'}:
                    sys.stderr.write('\nUser aborted configuration.\n')
                    sys.exit(EXIT_CMD_SUCCESS)

            # Get configuration from pipeline, recursively prompt user to fill in info
            config_dict = pipeline_class.configure()
            try:
                current_config = json.loads(open(os.path.join(self.home_configs,
                                                              '{}.json'.format(pipeline_name))).read())
            except:
                current_config = {}
            try:
                self.configure(config_dict, current_config, is_blank)
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

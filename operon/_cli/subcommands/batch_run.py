import os
import sys
import argparse

from operon._cli.subcommands import BaseSubcommand
from operon._util.configs import parse_pipeline_config

ARGV_FIRST_ARGUMENT = 0
ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


"""
The format for an input matrix will be as follows:

If a header line is provided, the values should match those of the pipeline arguments. If all arguments are found 
as headers, the corresponding values will be matched to their aguments at runtime. If less than all arguments are 
found, then those that are found will match while those that are omitted will be left to their defaults. If more than 
all arguments are found, extra arguments will be ignored.

If a header line is not provided, each row will be treated as a separate pipeline run. The values will be applied to 
arguments in the order of their declaration in the pipeline script. If less than all arguments are found, then the 
missing arguments will be left to their defaults. Extra arguments will be ignored.

For singleton arguments, any value given will result in that singleton argument being included in the call, except in 
the case of the empty string, 'f', 'false', 'null', or 'none'.
"""

def usage():
    return 'operon batch-run <pipeline-name> [-h] --input-matrix <input_matrix> [--separate-pools]'


class Subcommand(BaseSubcommand):
    def help_text(self):
        return 'Run a pipeline in batch with a shared resource pool.'

    def run(self, subcommand_args):
        # Get pipeline name or output help
        parser = argparse.ArgumentParser(prog='operon batch-run', usage=usage(), description=self.help_text())
        if not subcommand_args or subcommand_args[ARGV_FIRST_ARGUMENT].lower() in ['-h', '--help', 'help']:
            parser.print_help()
            sys.exit(EXIT_CMD_SUCCESS)

        # Get the pipeline class based on the name
        pipeline_name = subcommand_args[ARGV_PIPELINE_NAME]
        pipeline_instance = self.get_pipeline_instance(pipeline_name)

        if pipeline_instance is not None:
            # Parse the pipeline arguments and inject them into the pipeline class
            pipeline_args_parser = argparse.ArgumentParser(prog='operon batch-run {}'.format(pipeline_name))
            pipeline_args_parser.add_argument('--pipeline-config',
                                              default=os.path.join(self.home_configs, '{}.json'.format(pipeline_name)),
                                              help='Path to a config file to use for this run')
            pipeline_args_parser.add_argument('--parsl-config',
                                              help='Path to a JSON file containing a Parsl config')
            pipeline_args_parser.add_argument('--logs-dir', default='.', help='Path to a directory to store log files')
            pipeline_args_parser.add_argument('--input-matrix', required=True,
                                              help=('Tab-separated file with a header and a row of arguments for each '
                                                    'sample or unit to be run. Consult the documentation for details '
                                                    'on the expected format.'))
            pipeline_args_parser.add_argument('--separate-pools', action='store_true',
                                              help=('If provided, Operon will run each sample or unit with its own '
                                                    'pool of resources, essentially like calling a separate Operon '
                                                    'instance for each sample or unit.'))

            # Get custom arguments from the Pipeline
            # pipeline_instance.arguments(pipeline_args_parser)
            # pipeline_args = vars(pipeline_args_parser.parse_args(subcommand_args[1:]))



            pipeline_instance._run_pipeline(
                pipeline_args=pipeline_args,
                pipeline_config=parse_pipeline_config(pipeline_args['pipeline_config'])
            )
        else:
            # If pipeline class doesn't exist, exit immediately
            sys.stderr.write('Pipeline {name} does not exist in {home}\n'.format(
                name=pipeline_name,
                home=self.home_pipelines + '/'
            ))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

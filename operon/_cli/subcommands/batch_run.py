import os
import sys
import argparse

from operon._cli.subcommands import BaseSubcommand
from operon._util.configs import parse_pipeline_config

ARGV_FIRST_ARGUMENT = 0
ARGV_PIPELINE_NAME = 0
EXIT_CMD_SUCCESS = 0
EXIT_CMD_SYNTAX_ERROR = 2


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
            run_args_parser = argparse.ArgumentParser(prog='operon batch-run {}'.format(pipeline_name), add_help=False)
            run_args_parser.add_argument('--pipeline-config',
                                              default=os.path.join(self.home_configs, '{}.json'.format(pipeline_name)),
                                              help='Path to a config file to use for this run')
            run_args_parser.add_argument('--parsl-config',
                                              help='Path to a JSON file containing a Parsl config')
            run_args_parser.add_argument('--logs-dir', default='.', help='Path to a directory to store log files')
            run_args_parser.add_argument('--input-matrix',
                                              help=('Tab-separated file with a header and a row of arguments for each '
                                                    'sample or unit to be run. Consult the documentation for details '
                                                    'on the expected format.'))
            run_args_parser.add_argument('--literal-input', action='store_true',
                                         help=('If provided, each line of the input matrix will be interpreted as '
                                               'if typed directly into the command line.'))
            # run_args_parser.add_argument('--separate-pools', action='store_true',
            #                                   help=('If provided, Operon will run each sample or unit with its own '
            #                                         'pool of resources, essentially like calling a separate Operon '
            #                                         'instance for each sample or unit.'))
            run_args_parser.add_argument('--run-name', default='run', help='Name of this run for the log file')
            run_args_parser.add_argument('-h', '--help', action='store_true', default=argparse.SUPPRESS,
                                         help='Show help message for run args and pipeline args.')

            # Gather arguments for this batch run overall
            run_args = vars(run_args_parser.parse_args(subcommand_args[1:]))

            # Create a parser for the pipeline args
            pipeline_args_parser = argparse.ArgumentParser(add_help=False)
            pipeline_instance.arguments(pipeline_args_parser)
            batch_pipeline_args = list()

            # If -h given to run args, print help message from run and pipeline args and quit
            if run_args.get('help'):
                sys.stderr.write('For the batch run:\n')
                run_args_parser.print_help()
                sys.stderr.write('\nFor the pipeline {}:\n'.format(pipeline_name))
                pipeline_args_parser.print_help()
                sys.exit()

            # Make --input-matrix a required argument
            if not run_args['input_matrix']:
                run_args_parser.print_help()
                sys.stderr.write('\nerror: the following arguments are required: --input-matrix\n')
                sys.exit()

            # Parse the input matrix
            with open(run_args['input_matrix']) as input_matrix:
                if not run_args['literal_input']:
                    headers = next(input_matrix).strip().split('\t')
                    # For each run in this batch run
                    for line in input_matrix:
                        positionals, optionals = list(), list()
                        record = line.strip().split('\t')

                        # For each argument in this run add to either optional or positional
                        for i, record_item in enumerate(record):
                            record_header = headers[i]
                            if record_header.startswith('positional_'):
                                positionals.append((int(record_header.split('_')[-1]), record_item))
                            else:
                                # Determine whether this is a singleton argument
                                if record_item.strip().lower() == 'true':
                                    # Include singleton
                                    optionals.append(record_header)
                                elif record_item.strip().lower() in {'#true', '#false'}:
                                    # Include optional with literal 'true' or 'false' value
                                    optionals.extend([record_header, record_item.strip().strip('#')])
                                elif record_item.strip().lower() != 'false':
                                    # Include normal optional
                                    optionals.extend([record_header] + record_item.split())
                                # Note: If value is 'false' then none of these will match, so the optional
                                #       won't be included

                        # Put positional arguments into positional order
                        positionals = [p[1] for p in sorted(positionals, key=lambda r: r[0])]

                        # Parse arguments with pipeline parser
                        pipeline_args = vars(pipeline_args_parser.parse_args(optionals + positionals))
                        if 'logs_dir' not in pipeline_args:
                            pipeline_args['logs_dir'] = run_args['logs_dir']

                        # Add this run to the batch run
                        batch_pipeline_args.append(pipeline_args)
                else:
                    for literal_line in input_matrix:
                        # Parse arguments with pipeline parser
                        pipeline_args = vars(pipeline_args_parser.parse_args(literal_line.strip().split()))
                        if 'logs_dir' not in pipeline_args:
                            pipeline_args['logs_dir'] = run_args['logs_dir']

                        # Add this run to the batch run
                        batch_pipeline_args.append(pipeline_args)

            # Run the pipeline in batch
            pipeline_instance._run(
                pipeline_args=batch_pipeline_args,
                pipeline_config=parse_pipeline_config(run_args['pipeline_config']),
                original_command='batch-run ' + ' '.join(subcommand_args),
                run_args=run_args,
            )
        else:
            # If pipeline class doesn't exist, exit immediately
            sys.stderr.write('Pipeline {name} does not exist in {home}\n'.format(
                name=pipeline_name,
                home=self.home_pipelines + '/'
            ))
            sys.exit(EXIT_CMD_SYNTAX_ERROR)

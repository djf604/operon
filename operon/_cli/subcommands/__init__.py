import os
import inspect

from operon._util.home import get_operon_home, load_pipeline_file
from operon._util.errors import MalformedPipelineError
from operon.components import ParslPipeline

FLATTEN = 0
MODULE_NAME = 0
MODULE_INSTANCE = 1


class BaseSubcommand(object):
    home_pipelines = os.path.join(get_operon_home(), 'pipelines')
    home_configs = os.path.join(get_operon_home(), 'configs')

    def get_pipeline_instance(self, pipeline_name):
        pipeline_filepath = os.path.join(self.home_pipelines,
                                         '{}.py'.format(pipeline_name))

        # Look for pipeline in installed directory first
        if os.path.isfile(pipeline_filepath):
            pipeline_mod = load_pipeline_file(pipeline_filepath)
        # Check to see if pipeline name is a full path to pipeline
        elif os.path.isfile(pipeline_name):
            pipeline_mod = load_pipeline_file(pipeline_name)
        # If none of the above, return None
        else:
            return None

        # Get all classes in the pipeline file
        classes_in_pipeline_mod = [
            c for c in
            inspect.getmembers(pipeline_mod, inspect.isclass)
            if '__operon.pipeline' in str(c[MODULE_INSTANCE])
        ]
        # If there is only one class in the pipeline file, use that class
        if len(classes_in_pipeline_mod) == 1:
            pipeline_class = classes_in_pipeline_mod[FLATTEN][MODULE_INSTANCE]
        # If there are multiple classes, attempt to find one called Pipeline
        elif len(classes_in_pipeline_mod) > 1:
            try:
                pipeline_class = [
                    c[MODULE_INSTANCE]
                    for c in classes_in_pipeline_mod
                    if c[MODULE_NAME] == 'Pipeline'
                ][FLATTEN]
            except:
                # If Pipeline does not exist, send back None
                raise MalformedPipelineError(
                    'Pipeline file contained multiple classes, none of '
                    'which were called \'Pipeline\'\n'
                    'Try the form:\n\n'
                    '\tclass Pipeline(ParslPipeline):\n')
        else:
            # If there are zero classes found, send back None
            raise MalformedPipelineError('Pipeline file has no classes')

        # Return pipeline instance
        try:
            # Ensure Pipeline subclasses ParslPipeline
            if not issubclass(pipeline_class, ParslPipeline):
                raise MalformedPipelineError(
                    'Pipeline class does not subclass ParslPipeline\n'
                    'Try the form:\n\n'
                    '\tclass Pipeline(ParslPipeline):\n'
                )
            return pipeline_class()
        except AttributeError:
            # Ensure the pipeline file contains a class called Pipeline
            raise MalformedPipelineError(
                'No Pipeline class could be found for {}\n'
                'Try the form:\n\n'
                '\tclass Pipeline(ParslPipeline):\n'.format(pipeline_name)
            )

    def help_text(self):
        return ''

    def run(self, subcommand_args):
        return NotImplementedError


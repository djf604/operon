import os
import imp


class BaseCommand(object):
    operon_home = os.environ.get('OPERON_HOME') or os.path.expanduser('~')
    home_pipelines = os.path.join(operon_home, '.operon', 'pipelines')
    home_configs = os.path.join(operon_home, '.operon', 'configs')

    def get_pipeline_class(self, pipeline_name):
        pipeline_filepath = os.path.join(self.home_pipelines,
                                         '{}.py'.format(pipeline_name))

        # Look for pipeline in installed directory first
        if os.path.isfile(pipeline_filepath):
            return imp.load_source(pipeline_name,
                                   pipeline_filepath.format(pipeline_name)).Pipeline()
        # Check to see if pipeline name is a full path to pipeline
        elif os.path.isfile(pipeline_name):
            return imp.load_source('', pipeline_name).Pipeline()
        # If none of the above, return None
        return None

    def help_text(self):
        return ''

    def run(self, command_args):
        return NotImplementedError


import os
import tempfile

from operon.components import ParslPipeline, CondaPackage, Software, Parameter, Data


class Pipeline(ParslPipeline):
    def parsl_configuration(self):
        super().parsl_configuration()

    def description(self):
        return 'Pipeline for testing'

    def dependencies(self):
        return ['six']

    def conda(self):
        return {
            'packages': [CondaPackage(tag='STAR=2.4.2a', config_key='STAR')]
        }

    def arguments(self, parser):
        parser.add_argument('--test')

    def configuration(self):
        return {
            'STAR': {
                'path': 'Full path to STAR',
                'sjdb_overhang': {
                    'q_type': 'list',
                    'message': 'What should the overhang be?',
                    'choices': ['4', '8', '95']
                }
            },
            'wget': {
                'path': 'Full path to wget'
            },
            'sleep': {
                'path': 'Full path to sleep'
            },
            'ersatz': {
                'path': {
                    'q_type': 'path',
                    'message': 'Full path to ersatz'
                }
            }
        }

    def pipeline(self, pipeline_args, pipeline_config):
        tmpdir = tempfile.mkdtemp()
        os.chdir(tmpdir)
        wget = Software('wget')
        ersatz = Software('ersatz')
        star = Software('STAR', subprogram='twopass')

        wget.register(
            Parameter('https://lh3.googleusercontent.com/'
                      '8gaEOU2p30N4Up-KMUl4MQBtnn0F5DyH5bqKKr0QqptnQgPk4lxXaWLJhi8Dcu9i8qE=w170'),
            Parameter('-O', Data(os.path.join(tmpdir, 'image.png')).as_output())
        )

        # Ersatz2
        ersatz.register(
            Parameter('--inputs', Data(os.path.join(tmpdir, 'image.png')).as_input()),
            Parameter('--outputs', Data('21.out').as_output(), Data('22.out').as_output())
        )

        # Ersatz1
        ersatz.register(
            Parameter('--outputs', Data('11.out').as_output())
        )

        # Ersatz3
        ersatz.register(
            Parameter('--outputs', Data('31.out').as_output())
        )

        # Ersatz4
        ersatz4 = ersatz.register(
            Parameter('--inputs', Data('11.out').as_input(), Data('21.put').as_input()),
            Parameter('--outputs', Data('41.out').as_output(), Data('42.out').as_output())
        )

        # Ersatz5
        ersatz.register(
            Parameter('--inputs', Data('22.out').as_input(), Data('31.out').as_input()),
            Parameter('--outputs', Data('51.out').as_output()),
            #extra_inputs=[ersatz4]  # TODO Software as dependencies
        )

        # Ersatz6
        ersatz.register(
            Parameter('--inputs', Data('41.out').as_input(), Data('42.out').as_input(), Data('51.out').as_input()),
            Parameter('--outputs', Data('61.out').as_output())
        )


from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

from libsubmit.providers import LocalProvider

config = Config(
    executors=[
        IPyParallelExecutor(
            label='malformed_config',
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=4,
                walltime='00:03:00'
            )
        )
    ],
    doesnotexist='willthrowerror'
)
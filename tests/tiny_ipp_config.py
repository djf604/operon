from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor
from parsl.utils import get_last_checkpoint

from libsubmit.providers import LocalProvider, SlurmProvider

config = Config(
    executors=[IPyParallelExecutor(
        label='four_jobs',
        provider=LocalProvider(
            init_blocks=1,
            min_blocks=0,
            max_blocks=4,
            walltime='00:03:00'
        ),
    )],
    retries=3
)

from parsl import ThreadPoolExecutor, DataFlowKernel


def basic_threads():
    workers = ThreadPoolExecutor(max_workers=8)
    return DataFlowKernel(executors=[workers])


configs_map = {
    'basic-threads': basic_threads
}
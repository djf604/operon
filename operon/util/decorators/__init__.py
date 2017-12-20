from time import time
from datetime import timedelta
import functools


def timed(func):
    @functools.wraps(func)
    def timer(*args, **kwargs):
        start_time = time()
        result = func(*args, **kwargs)
        elapsed_time = str(timedelta(seconds=int(time() - start_time)))
        print('Elapsed time is {}'.format(elapsed_time))
        return result
    return timer

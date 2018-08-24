import os
import logging
from datetime import datetime


def setup_logger(logs_dir=None, run_name='run'):
    logger = logging.getLogger('operon.main')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logformat = logging.Formatter(fmt='[{asctime}|{name}] > {message}', datefmt='%d%b%Y %H:%M:%S', style='{')

    if logs_dir is not None:
        operon_log_filename = '{}_{}.operon.log'.format(run_name, datetime.now().strftime('%d%b%Y_%H%M%S'))
        logfilehandler = logging.FileHandler(os.path.join(logs_dir, operon_log_filename))
        logfilehandler.setFormatter(logformat)
        logger.addHandler(logfilehandler)

    ch = logging.StreamHandler()
    ch.setFormatter(logformat)
    logger.addHandler(ch)

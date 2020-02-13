"""
Contains core constants, datatypes etc. used application wise
"""
import logging
import os
from enum import Enum
from functools import wraps
import time
import decimal
import json

BASE_API_URL: str = 'https://cloud.iexapis.com/v1/'
API_TOKEN = f"?token={os.getenv('API_TOKEN')}"
MAX_RETRIEVAL_THREADS = 16
MAX_PERSISTENCE_THREADS = 16
DYNAMO_URI = os.getenv('DYNAMO_URI', None)

if os.getenv('TEST_ENVIRONMENT') == 'True':
    BASE_API_URL: str = 'https://sandbox.iexapis.com/stable/'

REGION = os.getenv('REGION')
TABLE = os.getenv('TABLE')


class ActionStatus(Enum):
    SUCCESS = 0
    ERROR = -1


class Results:
    def __init__(self):
        self.ActionStatus: ActionStatus = ActionStatus.ERROR
        self.Results = []

class AppException(Exception):
    def __init__(self, ex, message="See exception for detailed message."):
        self.Exception = ex
        self.Message = message

def get_logger(module_name: str, level: str = logging.INFO):
    logging.basicConfig(format='%(asctime)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                        level=level)
    logger = logging.getLogger(module_name)
    filename = os.getenv('LOG_FILE')
    if filename:
        handler = logging.FileHandler(filename)
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s',
                                       datefmt='%d-%b-%y %H:%M:%S')
        handler.setFormatter(log_format)
        logger.addHandler(handler)
    return logger

def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = f'{e}, Retrying in {mdelay} seconds...'
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry
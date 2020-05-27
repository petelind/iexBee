"""
Contains core constants, datatypes etc. used application wise
"""
import logging
from pythonjsonlogger import jsonlogger
import os
from enum import Enum
from functools import wraps
import time
import decimal
import json
import boto3
from collections.abc import MutableMapping
from itertools import islice
from concurrent.futures import ThreadPoolExecutor

secret_mngr = boto3.client('secretsmanager')
API_TOKEN = os.getenv('API_TOKEN') or secret_mngr.get_secret_value(SecretId=f'iextoken-{os.getenv("ENV")}')['SecretString']
BASE_API_URL: str = 'https://cloud.iexapis.com/v1/'
MAX_RETRIEVAL_THREADS = 16
MAX_PERSISTENCE_THREADS = 16
DYNAMO_URI = os.getenv('DYNAMO_URI', None)
STOCKS = {}
ENVIRONMENT = os.getenv('ENV')

if os.getenv('TEST_ENVIRONMENT') == 'true':
    BASE_API_URL: str = 'https://sandbox.iexapis.com/stable/'

if os.getenv('TEST_STOCKS', 'false') == 'true':
    DATASET = "Test data set"
    STOCKS = {
        'ALTM': {'symbol': 'ALTM', 'date': '2020-03-10'},
        'AVTR-A': {'symbol': 'AVTR-A', 'date': '2020-03-10'},
        'RNR-C*': {'symbol': 'RNR-C*', 'date': '2020-03-10'},
        'STT-C*': {'symbol': 'STT-C*', 'date': '2020-03-10'},
        'SFB': {'symbol': 'SFB', 'date': '2020-03-10'},
        'CTRN': {'symbol': 'CTRN', 'date': '2020-03-10'},
        'CTR': {'symbol': 'CTR', 'date': '2020-03-10'},
        'CBO': {'symbol': 'CBO', 'date': '2020-03-10'},
        'CBX': {'symbol': 'CBX', 'date': '2020-03-10'},
        'BFYT': {'symbol': 'BFYT', 'date': '2020-03-10'},
        'DFNS=': {'symbol': 'DFNS=', 'date': '2020-03-10'},
        'NTEST.A': {'symbol': 'NTEST.A', 'date': '2020-03-10'},
        'NTEST.B': {'symbol': 'NTEST.B', 'date': '2020-03-10'},
        'NONE': {'symbol': 'NONE', 'date': '2020-03-10'},
        'ARNC#': {'symbol': 'ARNC#', 'date': '2020-03-10'}
    }
else:
    DATASET = "Whole data set"

REGION = os.getenv('REGION')
TABLE = os.getenv('TABLE', f'IexSnapshot-{os.getenv("ENV")}')


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


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['Environment'] = ENVIRONMENT
        log_record['Dataset'] = DATASET
        log_record['LambdaId'] = os.getenv('AWS_RECORD_ID', 'Unknown')


def get_logger(module_name: str, level: str = logging.INFO):

    # This part deletes predefined AWS logging handler:
    root_hndlr = logging.getLogger()
    if root_hndlr.handlers:
        for handler in root_hndlr.handlers:
            root_hndlr.removeHandler(handler)

    logger = logging.getLogger(module_name)

    if not logger.handlers:
        logger.setLevel(level)
        logs_handler = logging.StreamHandler()
        if os.getenv('JSON_LOGS', 'false') == "true":
            formatter = CustomJsonFormatter(
                fmt='%(asctime)s - %(Environment)s - %(Dataset)s - %(LambdaId)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s',
                datefmt='%d-%b-%y %H:%M:%S'
            )
        else:
            formatter = logging.Formatter(
                fmt='%(asctime)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s',
                datefmt='%d-%b-%y %H:%M:%S'
            )
        logs_handler.setFormatter(formatter)
        logger.addHandler(logs_handler)

    filename = os.getenv('LOG_FILE')
    if filename:
        handler = logging.FileHandler(filename)
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(process)d - [%(levelname)s] - %(message)s',
                                       datefmt='%d-%b-%y %H:%M:%S')
        handler.setFormatter(log_format)
        logger.addHandler(handler)

    return logger


def check_func(val):
    if val in [[], {}]:
        return False
    elif remove_empty_strings(val) not in [None, [], {}]:
        return True


# Function to search in nested dict:
def remove_empty_strings(dictionary):
    if type(dictionary) == list:
        return [
            remove_empty_strings(val)
            for val in dictionary
            if check_func(val)]
    elif type(dictionary) == dict:
        return {
            key: remove_empty_strings(val)
            for key, val in dictionary.items()
            if check_func(val)}
    elif dictionary or dictionary is False or dictionary == 0:
        return dictionary

def dict_cleanup(f):
    #@wraps(f)
    def f_dict_cleanup(*args, **kwargs):
        return remove_empty_strings(f(*args, **kwargs))
    return f_dict_cleanup

def batchify(
        param_to_slice: str, size: int,
        multiprocess: bool = False,
        workers: int = os.cpu_count()
    ):
    def split(data, size: int):
        if type(data) == dict:
            it = iter(data)
            for i in range(0, len(data), size):
                yield {k: data[k] for k in islice(it, size)}
        elif type(data) == list:
            for i in range(0, len(data), size):
                yield data[i:i+size]
        else:
            message = f'Can not slice over {type(data)}'
            raise AppException(TypeError, message)

    def deco_batchify(f):
        #@wraps(f)
        def f_batchify(*args, **kwargs):
            if param_to_slice not in kwargs:
                message = f"Can not find param {param_to_slice} in kwargs"
                raise AppException(Exception, message)
            data = kwargs[param_to_slice]
            max_workes = workers if multiprocess else 1
            with ThreadPoolExecutor(max_workers=max_workes) as executor:
                for d in split(data,size):
                    kwargs[param_to_slice] = d
                    executor.submit(f ,*args, **kwargs)
        return f_batchify
    return deco_batchify

def retry(exceptions, tries: int = 4, 
        delay: int = 3, backoff: int = 2, logger=None):
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
        def f_retry(self, *args, **kwargs):
            self.Logger.setLevel(self.log_level)
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(self, *args, **kwargs)
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


def func_time(logger=None):
    """
    Decorator. Measures function execution time.
    logger: Logger to use. If None, print.
    """

    def deco_func_time(func):
        @wraps(func)
        def time_measure(*args, **kwargs):
            start = int(round(time.time() * 1000))
            try:
                return func(*args, **kwargs)
            finally:
                end = int(round(time.time() * 1000)) - start
                logger.info(f"{func.__name__}: Total execution time: {end if end > 0 else 0} ms",
                            extra={"message_info": {"Type": "Time measure", "Function": func.__name__, "Execution time, ms": (end if end > 0 else 0)}})
        return time_measure

    return deco_func_time

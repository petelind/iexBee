"""
Contains core constants, datatypes etc. used application wise
"""
import logging
import os
from enum import Enum
import decimal
import json

BASE_API_URL: str = 'https://cloud.iexapis.com/v1/'
API_TOKEN = '?token=' + os.getenv('API_TOKEN')
MAX_RETRIEVAL_THREADS = 16
MAX_PERSISTENCE_THREADS = 16

if os.getenv('TEST_ENVIRONMENT') == 'True':
    BASE_API_URL: str = 'https://sandbox.iexapis.com/stable/'
    API_TOKEN = '?token=Tsk_044123025bab4e5eb4p0e2daf0307824a'

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

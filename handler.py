import logging
import os
from datetime import datetime

import app
from datawell.iex import Iex
from persistence.dynamostore import DynamoStore


def lambda_handler(event=None, context=None):
    log_level = logging.INFO
    logger = app.get_logger(module_name=__name__, level=log_level)
    try:
        start_time = datetime.now()

        datasource = Iex(app.STOCKS, log_level=log_level)
        dynamostore = DynamoStore(app.TABLE, log_level=log_level)
        dynamostore.store_documents(datasource.get_symbols())

        end_time = datetime.now()
        run_time = end_time - start_time
        logger.info(f'Timing: It took {run_time} to finish this run')
    except app.AppException as e:
        logger.error(e.Message, exc_info=True)
        os._exit(-1)  # please note: python has no encapsulation - you can call private methods! doesnt mean you should

lambda_handler()

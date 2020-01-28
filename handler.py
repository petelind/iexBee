import logging
import os
from datetime import datetime

import app
from datawell.iex import Iex


def lambda_handler(event=None, context=None):
    logger = app.get_logger(module_name=__name__, level=logging.INFO)
    try:
        start_time = datetime.now()

        datasource = Iex()
        # This funny thing is called list comprehension and is a damn fast iterator...
        # Here is how it works: https://nyu-cds.github.io/python-performance-tips/08-loops/
        [print(stock) for stock in datasource.Symbols]
        # ^ operand       ^ subject  ^iterable (collection or whatever is able to __iter()__)

        # Ok, lets time our run...
        end_time = datetime.now()
        run_time = end_time - start_time
        logger.info('Timing: It took ' + str(run_time) + ' to finish this run')
    except app.AppException as e:
        logger.error(e.Message, exc_info=True)
        os._exit(-1)  # please note: python has no encapsulation - you can call private methods! doesnt mean you should

lambda_handler()

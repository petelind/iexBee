import logging
import os
import secrets

import app
from datawell.iex import Iex
from persistence.dynamostore import DynamoStore


@app.func_time(logger=app.get_logger(module_name='handler.lambda_handler'))
def lambda_handler(event=None, context=None):
    os.environ["AWS_RECORD_ID"] = f"CONSOLE_{secrets.token_hex(nbytes=8)}"
    if context:
        os.environ["AWS_RECORD_ID"] = context.aws_request_id
    log_level = logging.INFO
    try:

        datasource = Iex(app.STOCKS, log_level=log_level)
        dynamostore = DynamoStore(app.TABLE, log_level=log_level)
        dynamostore.store_documents(documents=datasource.get_symbols())

    except app.AppException as e:
        logger.error(e.Message, exc_info=True)
        os._exit(-1)  # please note: python has no encapsulation - you can call private methods! doesnt mean you should


if __name__ == "__main__":
    lambda_handler()

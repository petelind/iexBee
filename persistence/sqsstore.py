import boto3
import logging
import app
from persistence.basestore import BaseStore

class sqsStore(BaseStore):

    def __init__(self, name: str, log_level=logging.INFO):
        self.log_level = log_level
        self.Logger = app.get_logger(__name__, level=self.log_level)
        self.sqs_resource = boto3.resource(
            'sqs',
            region_name=app.REGION,
            endpoint_url=app.SQS_URI
        )
        self.sqs_queue = self.sqs_resource.get_queue_by_name(
            QueueName=name
        )

    def store_documents():
        """
        Persists list of dict()
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully,
            ERROR if failed, AppException if AWS Error: No access etc
        """
        pass

    def get_filtered_documents():
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to
            get for all available dates
        :return: a list of dicts() each containing data available for a stock
            for a given period of time
        """
        pass

    def clean_table():
        """
        Use this one to either clean specific stocks from the db or delete the table if symbols_to_remove is empty.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        """
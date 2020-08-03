import boto3
import logging
import app
import json
from uuid import uuid1
from persistence.basestore import BaseStore

class sqsStore(BaseStore):

    def __init__(self, name: str='sqsStore', log_level=logging.INFO):
        self.log_level = log_level
        self.Logger = app.get_logger(__name__, level=self.log_level)
        self.sqs_client = boto3.client(
            'sqs',
            region_name=app.REGION,
            endpoint_url=app.SQS_URI
        )
        self.sqs_queue_url = self.sqs_client.get_queue_url(
            QueueName=name
        )['QueueUrl']

    def store_documents(self, documents: list):
        """
        Persists list of dict()
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully,
            ERROR if failed, AppException if AWS Error: No access etc
        """
        results = app.Results()
        entries = [
            { 
                'Id': str(uuid1()),
                'MessageBody': json.dumps(doc)
            }
            for doc in documents
        ]
        ids = [ e['Id'] for e in entries ]
        self.Logger.info(f'Store {ids} in sqs')
        self.Logger.debug(f'Saving {entries} in sqs {self.sqs_queue_url}')
        self.sqs_client.send_message_batch(
            QueueUrl=self.sqs_queue_url,
            Entries=entries
        )
        results.ActionStatus = 0
        results.Results = ids
        return results

    def get_filtered_documents(self, numberOfMessages: int):
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to
            get for all available dates
        :return: a list of dicts() each containing data available for a stock
            for a given period of time
        """
        sqs_messages = []
        results = app.Results()
        try:
            self.Logger.info(f'Get {numberOfMessages} of messages from sqs')
            self.Logger.debug(f'Getting {numberOfMessages} of messages from sqs {self.sqs_queue_url}')
            get_documents = self.sqs_client.receive_message(
                QueueUrl=self.sqs_queue_url,
                MaxNumberOfMessages=numberOfMessages
            )
            [sqs_messages.append(json.loads(message['Body'])) for message in get_documents['Messages']]
            results.Results = sqs_messages
            results.ActionStatus = 0
        except Exception as e:
            results.ActionStatus = -1
            self.Logger.info(f"An issue occured during the process of getting messages from sqs.")
            self.Logger.debug(f'An issue occured during the process of getting messages from sqs. Message {e}')
        return results
        

    def clean_table(self):
        """
        Use this one to either clean specific stocks from the db or delete the table if symbols_to_remove is empty.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        """
        return False

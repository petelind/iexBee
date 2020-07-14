import decimal
import json
import pytest
from unittest import TestCase
from unittest.mock import patch
from persistence.sqsstore import sqsStore
import boto3
import app


name = 'CompaniesSQSIntegrationTesting'
sqs_client = boto3.client('sqs', endpoint_url=app.SQS_URI)
sqs_queue = sqs_client.create_queue(QueueName=name)
sqs_store = sqsStore(name=name)

@pytest.mark.sqs
class TestSQSStore(TestCase):

    @classmethod
    def tearDownClass(cls):
        sqs_queue.delete()

    def tearDown(self):
        sqs_client
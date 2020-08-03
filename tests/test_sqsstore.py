import decimal
import json
import pytest
from unittest import TestCase
from unittest.mock import patch
from persistence.sqsstore import sqsStore
import boto3
import app
from uuid import uuid1

name = 'CompaniesSQSIntegrationTesting'
sqs_client = boto3.client('sqs', endpoint_url=app.SQS_URI)
sqs_queue = sqs_client.create_queue(QueueName=name)
sqs_queue_url = sqs_client.get_queue_url(QueueName=name)['QueueUrl']
sqs_store = sqsStore(name=name)

@pytest.mark.sqs
class TestSQSStore(TestCase):

    @classmethod
    def tearDownClass(cls):
        sqs_client.delete_queue(QueueUrl=sqs_queue_url)

    def tearDown(self):
        sqs_client.purge_queue(QueueUrl=sqs_queue_url)

    def test_store_AEBfixture(self):
        # ARRANGE
        symbol_to_load = 'AEB'
        date = '2020-02-11'
        serialized_doc = app.remove_empty_strings(
            self.read_fixture(
                f'tests/fixtures/{symbol_to_load}.response.json'
            )
        )

        # ACT:
        store_object = sqs_store.store_documents(documents=[serialized_doc])
        get_it_back = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=1
        )

        # ASSERT:
        self.assertIsInstance(
            store_object, app.Results, 
            'Store function should return appResults object'
        )
        self.assertEqual(store_object.ActionStatus, 0,
            'Store function should return Success ActionStatus'
        )
        self.assertIn('Messages', get_it_back, 'Queue does not have message')
        self.assertEqual(
            len(get_it_back['Messages']), 1,
            'Queue has more than 1 message'
        )
        self.assertDictEqual(
            json.loads(get_it_back['Messages'][0]['Body']), 
            serialized_doc, 'Stored document not equal'
        )

    def test_get_AEBfixture(self):
        # ARRANGE
        symbol_to_load = 'AEB'
        date = '2020-02-11'
        serialized_doc = app.remove_empty_strings(
            self.read_fixture(
                f'tests/fixtures/{symbol_to_load}.response.json'
            )
        )

        # ACT:
        test_message_body = json.dumps(serialized_doc)
        store_object = sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=test_message_body
        )
        get_object = sqs_store.get_filtered_documents(numberOfMessages=1)

        # ASSERT:
        self.assertIsInstance(
            get_object, app.Results, 
            'Store function should return appResults object'
        )
        self.assertEqual(get_object.ActionStatus, 0,
            'Store function should return Success ActionStatus'
        )
        self.assertDictEqual(
            get_object.Results[0], 
            serialized_doc, 'Stored document not equal'
        )

    def read_fixture(self, file: str):
        with open(file, mode='r') as companies_file:
            return json.load(companies_file, parse_float=decimal.Decimal)
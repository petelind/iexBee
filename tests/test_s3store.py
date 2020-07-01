import decimal
import json
from unittest import TestCase
from unittest.mock import patch
from persistence.s3store import S3Store
from botocore.exceptions import ClientError
from collections.abc import MutableMapping
import boto3
import app
from pickle import dumps, loads

bucket_name = 'companies-integration-testing'
s3_resource = boto3.resource('s3', endpoint_url=app.S3_URI)
s3_client = boto3.client('s3', endpoint_url=app.S3_URI)
bucket = s3_resource.Bucket(bucket_name)
bucket.create()
s3store = S3Store(bucket_name)

class TestS3Store(TestCase):

    @classmethod
    def setUpClass(cls):
        waiter = s3_client.get_waiter('bucket_exists')
        waiter.wait(
            Bucket=bucket_name,
            WaiterConfig={
                'Delay': 2,
                'MaxAttempts': 10
            }
        )

    @classmethod
    def tearDownClass(cls):
        bucket.delete()

    def tearDown(self) -> None:
        bucket.objects.delete()

    def test_store_documents_PassValidDocs_ExpectThemAppearInBucket(self):
        # ARRANGE
        symbol_to_load = 'AEB'
        date = '2020-02-11'
        serialized_doc = app.remove_empty_strings(
            self.read_fixture(
                f'tests/fixtures/{symbol_to_load}.response.json'
            )
        )
        self.assertFalse(self.item_exists(symbol_to_load, date), 'Item should exist before the deletion')

        # ACT:
        s3store.store_documents(documents=[serialized_doc])
        get_it_back_raw = s3_resource.Object(bucket_name, f'{date}/{symbol_to_load}').get()['Body'].read()
        get_it_back = loads(get_it_back_raw)

        # ASSERT:
        self.assertDictEqual(get_it_back, serialized_doc, 'Stored document not equal')

    def test_get_documents_WriteDocWithBoto3_ExpectReadedWithStore(self):
        # ARRANGE
        symbol_to_load = 'AAME'
        date = '2020-02-11'
        serialized_doc = app.remove_empty_strings(
            self.read_fixture(
                f'tests/fixtures/{symbol_to_load}.response.json'
            )
        )
        self.assertFalse(self.item_exists(symbol_to_load, date),
                         'Item should exist before the deletion')

        # ACT:
        object = s3_resource.Object(
            bucket_name, f'{date}/{symbol_to_load}'
        )
        object.put(Body=dumps(serialized_doc))
        get_it_back = s3store.get_filtered_documents(
            symbol_to_load,
            date
        )

        # ASSERT:
        self.assertIsInstance(get_it_back, app.Results)
        self.assertEqual(len(get_it_back.Results), 1)
        self.assertDictEqual(get_it_back.Results[0], serialized_doc,
                             'Stored document not equal')

    def item_exists(self, date: str, symbol: str):
        try:
            return loads(s3_resource.Object(bucket_name, f'{date}/{symbol}').get().read())
        except s3_resource.meta.client.exceptions.NoSuchKey:
            return False

    def read_fixture(self, file: str):
        with open(file, mode='r') as companies_file:
            return json.load(companies_file, parse_float=decimal.Decimal)

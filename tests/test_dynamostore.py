import decimal
import json
from unittest import TestCase
from persistence.dynamostore import DynamoStore
from boto3.dynamodb.conditions import Key
import boto3
import app

table_name = 'CompaniesIntegrationTesting'
dynamo_db_client = boto3.client('dynamodb', endpoint_url=app.DYNAMO_URI)
dynamo_db_resource = boto3.resource('dynamodb', endpoint_url=app.DYNAMO_URI)
dynamo_db_table = dynamo_db_resource.Table(table_name)
dynamo_store = DynamoStore(table_name=table_name)

class TestDynamoStore(TestCase):

    @classmethod
    def setUpClass(cls):
        waiter = dynamo_db_client.get_waiter('table_exists')
        waiter.wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 2,
                'MaxAttempts': 10
            }
        )

    @classmethod
    def tearDownClass(cls):
        dynamo_db_table.delete()

    def tearDown(self) -> None:
        all_companies: dict = dynamo_db_table.scan().get("Items", [])
        for company in all_companies:
            dynamo_db_table.delete_item(
                Key={
                    "symbol": company["symbol"]
                }
            )

    def test_store_documents_PassValidDocs_ExpectThemAppearInDB(self):
        # ARRANGE
        symbol_to_load = 'AEB'
        serialized_doc = dynamo_store.remove_empty_strings(
            self.read_fixture(
                f'tests/fixtures/{symbol_to_load}.response.json'
            )
        )
        self.assertFalse(self.item_exists(symbol_to_load),
                         'Item should exist before the deletion')

        # ACT:
        dynamo_store.store_documents([serialized_doc])
        get_it_back = dynamo_db_table.query(
            KeyConditionExpression=Key('symbol').eq(symbol_to_load)
        )['Items'][0]

        # ASSERT:
        self.assertDictEqual(get_it_back, serialized_doc,
                             'Stored document not equal')

    def test_store_documents_PassWithNullDocs_ExpectStoredWhithoutNull(self):
        # ARRANGE
        symbol_to_load = 'AAME'
        serialized_doc = self.read_fixture(
            f'tests/fixtures/{symbol_to_load}.response.json'
        )
        self.assertFalse(self.item_exists(symbol_to_load),
                         'Item should exist before the deletion')

        # ACT:
        dynamo_store.store_documents([serialized_doc])
        get_it_back = dynamo_db_table.query(
            KeyConditionExpression=Key('symbol').eq(symbol_to_load)
        )['Items'][0]

        # ASSERT:
        self.assertNotEqual(get_it_back, serialized_doc,
                            'Stored document need to be not equal')

    def test_clean_table_PassListWithOneExistingSymbol_ExpectSymbolDeletedFromDB(self):
        # ARRANGE:
        self.load_companies('tests/fixtures/companies_dump.json')
        initial_count = self.get_number_of_items_in_table()
        symbol_to_be_deleted = "A"
        assert self.item_exists(symbol_to_be_deleted), 'Item should exist before the deletion'

        # ACT
        dynamo_store.clean_table(symbols_to_remove=[symbol_to_be_deleted])

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count + 1, initial_count,
                         'Items count should be decremented by 1 after the clean up')
        self.assertFalse(self.item_exists(symbol_to_be_deleted), 'Item should be deleted')

    def test_clean_table_PassListWithNumberOfExistingSymbols_ExpectSymbolsDeletedFromDB(self):
        # ARRANGE:
        self.load_companies('tests/fixtures/companies_dump.json')
        initial_count = self.get_number_of_items_in_table()
        symbols_to_be_deleted = ["AA", "AACG", "AAMC"]
        for symbol in symbols_to_be_deleted:
            assert self.item_exists(symbol), f'Item {symbol} should exist before the deletion'
        # ACT
        dynamo_store.clean_table(symbols_to_remove=symbols_to_be_deleted)

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count + len(symbols_to_be_deleted), initial_count,
                         f'Items count should be decremented by {len(symbols_to_be_deleted)} after the clean up')
        for symbol_to_be_deleted in symbols_to_be_deleted:
            self.assertFalse(self.item_exists(symbol_to_be_deleted), f'Item {symbol_to_be_deleted} should be deleted')

    def test_clean_table_PassEmptyListOfSymbols_ExpectAllSymbolsFromDB(self):
        # ARRANGE:
        self.load_companies('tests/fixtures/companies_dump.json')

        # ACT
        dynamo_store.clean_table(symbols_to_remove=[])

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count, 0, f'Add items should be deleted')

    def read_fixture(self, file: str):
        with open(file, mode='r') as companies_file:
            return json.load(companies_file, parse_float=decimal.Decimal)

    def load_companies(self, file: str):
        companies = self.read_fixture(file)
        for company in companies.values():
            dynamo_db_table.put_item(Item=company)

    def get_number_of_items_in_table(self):
        return len(dynamo_db_table.scan()['Items'])

    def item_exists(self, symbol: str):
        return 'Item' in dynamo_db_table.get_item(Key={"symbol": symbol})

import decimal
import json
from unittest import TestCase
from persistence import *

from persistence.dynamostore import DynamoStore


class TestDynamoStore(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestDynamoStore, self).__init__(*args, **kwargs)
        self.dynamo_store = DynamoStore(table_name=table_name)

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

    def setUp(self) -> None:
        """
        This is is your plain old tearUp()
        :return: Nothing, but all the objects created here will be accessible by all other methods
        """
        with open('AEB.response.json') as file:
            self.serialized_doc = json.load(file)

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
        with open('AEB.response.json', mode='r') as doc:
            serialized_doc = json.load(doc)

        # ACT:
        self.dynamo_store.store_documents([serialized_doc])
        get_it_back = dynamo_db_table.query(
            KeyConditionExpression=Key('symbol').eq('AEB')
        )['Items'][0]

        # ASSERT:
        self.assertDictEqual(get_it_back, serialized_doc)


    def test_clean_table_PassListWithOneExistingSymbol_ExpectSymbolDeletedFromDB(self):
        # ARRANGE:
        self.load_companies('companies_dump.json')
        initial_count = self.get_number_of_items_in_table()
        symbol_to_be_deleted = "A"
        assert self.item_exists(symbol_to_be_deleted), 'Item should exist before the deletion'

        # ACT
        self.dynamo_store.clean_table(symbols_to_remove=[symbol_to_be_deleted])

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count + 1, initial_count,
                         'Items count should be decremented by 1 after the clean up')
        self.assertFalse(self.item_exists(symbol_to_be_deleted), 'Item should be deleted')

    def test_clean_table_PassListWithNumberOfExistingSymbols_ExpectSymbolsDeletedFromDB(self):
        # ARRANGE:
        self.load_companies('companies_dump.json')
        initial_count = self.get_number_of_items_in_table()
        symbols_to_be_deleted = ["AA", "AACG", "AAMC"]
        for symbol in symbols_to_be_deleted:
            assert self.item_exists(symbol), f'Item {symbol} should exist before the deletion'
        # ACT
        self.dynamo_store.clean_table(symbols_to_remove=symbols_to_be_deleted)

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count + len(symbols_to_be_deleted), initial_count,
                         f'Items count should be decremented by {len(symbols_to_be_deleted)} after the clean up')
        for symbol_to_be_deleted in symbols_to_be_deleted:
            self.assertFalse(self.item_exists(symbol_to_be_deleted), f'Item {symbol_to_be_deleted} should be deleted')

    def test_clean_table_PassEmptyListOfSymbols_ExpectAllSymbolsFromDB(self):
        # ARRANGE:
        self.load_companies('companies_dump.json')

        # ACT
        self.dynamo_store.clean_table(symbols_to_remove=[])

        # ASSERT
        result_count = self.get_number_of_items_in_table()
        self.assertEqual(result_count, 0, f'Add items should be deleted')

    def load_companies(self, file: str):
        with open(file, mode='r') as companies_file:
            companies = json.load(companies_file, parse_float=decimal.Decimal)
        for company in companies.values():
            dynamo_db_table.put_item(Item=company)

    def get_number_of_items_in_table(self):
        return len(dynamo_db_table.scan()['Items'])

    def item_exists(self, symbol: str):
        return 'Item' in dynamo_db_table.get_item(Key={"symbol": symbol})

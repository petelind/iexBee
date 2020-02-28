import decimal
import json
from unittest import TestCase
from unittest.mock import patch
from persistence.dynamostore import DynamoStore
from boto3.dynamodb.conditions import Key
from collections.abc import MutableMapping
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
        serialized_doc = app.remove_empty_strings(
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

    def test_clean_table_PassEmptyListOfSymbols_ExpectTableDeleteMethodCalled(self):
        # ARRANGE:
        self.load_companies('tests/fixtures/companies_dump.json')

        # ACT
        with patch.object(dynamo_store.table, 'delete') as mock:
            dynamo_store.clean_table(symbols_to_remove=[])

        # ASSERT
        mock.assert_called()

    def test_remove_empty_strings_PassReferenceDictWithEmptyValue_ExpectReferenceDictWithoutEmptyValues(self):
        # ARRANGE
        src_dict = self.read_fixture('tests/fixtures/ref_dict_toclean.json')
        ref_dict = self.read_fixture('tests/fixtures/ref_dict.json')

        # ACT
        res_dict = app.remove_empty_strings(dict_to_clean=src_dict)

        # ASSERT
        self.assertDictEqual(res_dict, ref_dict, f'Result dict and reference dict are different.')

    def test_remove_empty_strings_PassCompaniesDump_ExpectNoEmptyValuesInResultDict(self):
        # ARRANGE
        src_dict = self.read_fixture('tests/fixtures/companies_dump.json')

        # ACT
        res_dict = app.remove_empty_strings(dict_to_clean=src_dict)

        # ASSERT
        dict_has_empty_values = self.has_empty_value_in_dict(res_dict)
        self.assertFalse(dict_has_empty_values, f"Result dict shouldn't have empty values")

    def test_remove_empty_strings_PassEmptyDict_ExpectWarningAndReturnNothing(self):
        # ARRANGE
        src_dict = {}

        # ACT
        res_dict = app.remove_empty_strings(dict_to_clean=src_dict)

        # ASSERT
        self.assertEqual(res_dict, None, f"Function should return empty value.")

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

    def has_empty_value_in_dict(self, src_dict: dict):
        for key, value in src_dict.items():
            if value or value is False or value == 0:
                if isinstance(value, MutableMapping):
                    return self.has_empty_value_in_dict(value)
                elif isinstance(value, list):
                    return self.has_empty_value_in_list(value)
            else:
                return True

    def has_empty_value_in_list(self, src_list: list):
        for value in src_list:
            if value or value is False or value == 0:
                if isinstance(value, MutableMapping):
                    return self.has_empty_value_in_dict(value)
                elif isinstance(value, list):
                    return self.has_empty_value_in_list(value)
            else:
                return True

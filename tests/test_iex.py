from unittest import TestCase
from datawell.iex import Iex
import app
import json


class MockIEX:
    def __init__(self):
        self.dict_symbols = {}
        self.Logger = app.get_logger(__name__)
        self.Symbols = self.get_stocks()

    def get_stocks(self):
        with open('tests/fixtures/companies_dump.json', mode='r') as companies_file:
            return json.load(companies_file)


Iex = MockIEX
IexTest = Iex()


class TestIEX(TestCase):
    def test_iex_get_stocks_patched(self):
        # ARRANGE
        with open('tests/fixtures/companies_dump.json', mode='r') as companies_file:
            companies = json.load(companies_file)

        # ASSERT
        self.assertDictEqual(companies, IexTest.Symbols)

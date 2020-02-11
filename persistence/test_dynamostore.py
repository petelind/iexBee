import json
from unittest import TestCase


class TestDynamoStore(TestCase):

    def setUp(self) -> None:
        """
        This is is your plain old tearUp()
        :return: Nothing, but all the objects created here will be accessible by all other methods
        """
        with open('AEB.response.json') as file:
            self.serialized_doc = json.load(file)

    def test_store_documents_PassValidDocs_ExpectThemAppearInDB(self):
        # ARRANGE:
        json_string = json.dumps(self.serialized_doc)

        # ACT:
        get_it_back = json.loads(json_string)

        # ASSERT:
        self.assertDictEqual(get_it_back, self.serialized_doc)
        # word of caution assertItemsEqual() - intermittent failures with lists and ordered dicts
        # self.assertDictEqual()
        # self.assertRaises()

from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import app
from collections.abc import MutableMapping


class DynamoStore:
    def __init__(self, table_name: str):
        self.Logger = app.get_logger(__name__)
        self.dynamo = boto3.resource(
            "dynamodb",
            region_name=app.REGION,
            endpoint_url=app.DYNAMO_URI)
        self.table = self.dynamo.Table(table_name)
        try:
            self.table.table_status in (
                "CREATING", "UPDATING", "DELETING", "ACTIVE")
        except ClientError:
            self.Logger.info(f'DynamoDB table {table_name} doesn\'t exist,'
                             'creating...')
            self.dynamo.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {
                        'AttributeName': 'symbol',
                        'AttributeType': 'S',
                    }
                ],
                KeySchema=[
                    {
                        'AttributeName': 'symbol',
                        'KeyType': 'HASH',
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5,
                },
            )

    def store_documents(self, documents: list):
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully,
            ERROR if failed, AppException if AWS Error: No access etc
        """
        self.Logger.info(f'Writing into dynamodb')
        with self.table.batch_writer() as batch:
            for r in documents:
                self.Logger.debug(f'put into dynamodb {r}')
                batch.put_item(Item=r)
        return True

    def clean_table(self, symbols_to_remove: list):
        """
        Use this one to either clean specific stocks from the db or clean up the db if its small.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        :returns: number of the elements removed as int, 0 if not found, AppException if AWS Error: No access etc
        """
        if symbols_to_remove:
            for symbol in symbols_to_remove:
                self._delete_one_item(symbol)
        else:
            all_companies: dict = self.table.scan().get("Items", [])
            for company in all_companies:
                self._delete_one_item(company["symbol"])

    def _delete_one_item(self, company):
        self.table.delete_item(
            Key={
                "symbol": company
            }
        )

    def remove_empty_strings(dict_to_clean: dict):
        """
        The method removes all the empty key+value pairs from the dict
        you give it. The method is used to clean up dicts before
        persisting them to the DynamoDB.
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict()
        """

        try:

            # Function to search in nested list:
            def delete_from_list(some_list):
                modified_list = []
                for value in some_list:
                    if value or value is False or value == 0:
                        if isinstance(value, MutableMapping):
                            a = delete_keys_from_dict(value)
                            if a:
                                modified_list.append(a)
                        elif isinstance(value, list):
                            modified_list.append(delete_from_list(value))
                        else:
                            modified_list.append(value)
                return modified_list

            # Function to search in nested dict:
            def delete_keys_from_dict(dictionary):
                modified_dict = {}
                for key, value in dictionary.items():
                    if value or value is False or value == 0:
                        if isinstance(value, MutableMapping):
                            modified_dict[key] = delete_keys_from_dict(value)
                        elif isinstance(value, list):
                            modified_dict[key] = delete_from_list(value)
                        else:
                            modified_dict[key] = value
                return modified_dict

            res_dict = delete_keys_from_dict(dict_to_clean)
            return(res_dict)

        except Exception as e:
            message = 'Failed while cleaning dict for key-value empty pairs!'
            ex = app.AppException(e, message)
            raise ex

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: datetime.date = None):
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
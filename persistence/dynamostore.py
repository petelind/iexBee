from datetime import datetime
import boto3
import app
from collections.abc import MutableMapping
from boto3.dynamodb.conditions import Key


class DynamoStore:
    def __init__(self, table_name: str, part_key: str = "symbol"):
        self.Logger = app.get_logger(__name__)
        # Initialize both client and resource along with the class for usage in methods
        self.dynamo_client = boto3.client(
            'dynamodb',
            region_name=app.REGION,
            endpoint_url=app.DYNAMO_URI)
        self.dynamo_resource = boto3.resource(
            "dynamodb",
            region_name=app.REGION,
            endpoint_url=app.DYNAMO_URI)
        self.table = self.dynamo_resource.Table(table_name)
        try:
            self.table.table_status in (
                "CREATING", "UPDATING", "DELETING", "ACTIVE")
        except self.dynamo_resource.meta.client.exceptions.ResourceNotFoundException:
            self.Logger.info(f'Table {table_name} doesn\'t exist.')
            self.create_table(table_name, part_key)

    def create_table(self, table_name, part_key: str):
        """
        Creates DynamoDB table with given keys
        :param table_name: DynamoDB table name
        :param part_key: DynamoDB partition key
        :return: Nothing
        """

        waiter = self.dynamo_client.get_waiter('table_exists')
        self.Logger.info(f'Creating DynamoDB table {table_name}...')
        self.dynamo_resource.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {
                    'AttributeName': part_key,
                    'AttributeType': 'S',
                }
            ],
            KeySchema=[
                {
                    'AttributeName': part_key,
                    'KeyType': 'HASH',
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5,
            },
        )

        waiter.wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 10
            }
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
                batch.put_item(Item=self.remove_empty_strings(r))
        return True

    def clean_table(self, symbols_to_remove: list):
        """
        Use this one to either clean specific stocks from the db or delete the table if symbols_to_remove is empty.
        :param symbols_to_remove: list of dicts each containing 'symbol' string
        """
        try:
            with self.table.batch_writer() as batch:
                if symbols_to_remove:
                    for symbol in symbols_to_remove:
                        batch.delete_item(Key={"symbol": symbol})
                else:
                    self.table.delete()
        except Exception as e:
            message = 'Failed to clean table'
            ex = app.AppException(e, message)
            raise ex

    def remove_empty_strings(self, dict_to_clean: dict):
        """
        The method removes all the empty key+value pairs from the dict
        you give it. The method is used to clean up dicts before
        persisting them to the DynamoDB.
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict()
        """

        try:

            if not dict_to_clean:
                self.Logger.warning(f'Empty dict has been provided. Function returns nothing.')
                return None

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
                            a = delete_from_list(value)
                            if a:
                                modified_list.append(a)
                        else:
                            modified_list.append(value)
                return modified_list

            # Function to search in nested dict:
            def delete_keys_from_dict(dictionary):
                modified_dict = {}
                for key, value in dictionary.items():
                    if value or value is False or value == 0:
                        if isinstance(value, MutableMapping):
                            a = delete_keys_from_dict(value)
                            if a:
                                modified_dict[key] = a
                        elif isinstance(value, list):
                            a = delete_from_list(value)
                            if a:
                                modified_dict[key] = a
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
        :param target_date: desired date as a datetime.date, leave empty to
            get for all available dates
        :return: a list of dicts() each containing data available for a stock
            for a given period of time
        """
        getInfo = {'Items': []}
        self.Logger.info(f'''Looking for the info with provided peremeters:
            symbol = {symbol_to_find} and date = {target_date}''')
        try:
            if symbol_to_find is not None:
                if target_date is not None:
                    string_date = target_date.strftime("%Y-%m-%d")
                    symbol_expression = Key('symbol').eq(symbol_to_find)
                    date_expression = Key('date').eq(string_date)
                    getInfo = self.table.query(
                        KeyConditionExpression=symbol_expression,
                        FilterExpression=date_expression)
                else:
                    symbol_expression = Key('symbol').eq(symbol_to_find)
                    getInfo = self.table.query(
                        KeyConditionExpression=symbol_expression)
            else:
                if target_date is not None:
                    string_date = target_date.strftime("%Y-%m-%d")
                    date_expression = Key('date').eq(string_date)
                    getInfo = self.table.scan(FilterExpression=date_expression)
            return getInfo['Items']
        except Exception as e:
            raise app.AppException(e, message="""Unexpected behaviour during
                the request to the DynamoDB. {e}""")

from datetime import datetime
import boto3
import app


class DynamoStore:
    def __init__(self, table_name: str):
        self.Logger = app.get_logger(__name__)
        self.dynamodb = boto3.resource("dynamodb",
                                       region_name=app.REGION,
                                       endpoint_url=app.DYNAMO_URI)
        try:
            self.dynamo_table = self.dynamodb.Table(table_name)

        except self.dynamodb.exceptions.ResourceNotFoundException:
            self.Logger.info(f'DynamoDB table {table_name} doesn\'t exist, creating...')
            self.dynamodb.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {
                        'AttributeName': 'Symbol',
                        'AttributeType': 'S',
                    }
                ],
                KeySchema=[
                    {
                        'AttributeName': 'Symbol',
                        'KeyType': 'HASH',
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5,
                },
            )
            self.dynamo_table = self.dynamodb.Table(table_name)

    def store_documents(self, documents: list):
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        :return: ActionStatus with SUCCESS when stored successfully,
            ERROR if failed, AppException if AWS Error: No access etc
        """
        self.Logger.info(f'Writing into dynamodb')
        with self.dynamo_table.batch_writer() as batch:
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

    def remove_empty_strings(dict_to_clean: dict):
        """
        Removes all the empty key+value pairs from the dict you give it; use to clean up dicts before persisting them to the DynamoDB
        :param dict_to_clean: as dict()
        :return: only non-empty key+value pairs from the source dict as dict()
        """

    def get_filtered_documents(self, symbol_to_find: str = None, target_date: datetime.date = None):
        """
        Returns a list of documents matching given ticker and/or date
        :param symbol_to_find: ticker as a string
        :param target_date: desired date as a datetime.date, leave empty to get for all available dates
        :return: a list of dicts() each containing data available for a stock for a given period of time
        """
import boto3
import logging
import app
import botocore
from sys import getsizeof
from pickle import dumps, loads
from persistence.basestore import BaseStore

class S3Store(BaseStore):
    def __init__(self, bucket_name: str, log_level=logging.INFO):
        self.log_level = log_level
        self.bucket_name = bucket_name
        self.Logger = app.get_logger(__name__, level=self.log_level)

        self.s3_client = boto3.client(
            's3',
            region_name=app.REGION,
            endpoint_url=app.S3_URI
        )
        self.s3_res = boto3.resource(
            's3',
            region_name=app.REGION,
            endpoint_url=app.S3_URI
        )
    
    @app.batchify(param_to_slice='documents', size=25,
        multiprocess=True)
    @app.func_time(logger=app.get_logger(__name__))
    def store_documents(self, documents: list):
        """
        Persists list of dict() provided into the Dynamo table of the repo
        :param documents:
        """

        ticks = [d['symbol'] for d in documents]
        size = getsizeof(documents)

        self.Logger.info(
            f'Writing {ticks} into s3 '
            f'with size {size} bytes',
            extra={"message_info": {"Type": "S3 write", "Tickers": ticks, "Size": size}}
        )
        
        try:
            for Item in documents:
                object = self.s3_res.Object(
                    self.bucket_name, f'{Item["date"]}/{Item["symbol"]}'
                )
                object.put(Body=dumps(Item))
            
        except Exception as ex:
            raise app.AppException(ex, f'Failed to write data to s3!')

        return True

    @app.func_time(logger=app.get_logger(__name__))
    def get_filtered_documents(self, 
            symbol_to_find: str = None, 
            target_date: str = ''
        ):
        appResults = app.Results()

        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=target_date
        )
        content = []
        reqitem = f'{target_date}/{symbol_to_find}'
        try:
            # if we have more than 1000 keys need to paginate
            [content.extend(page['Contents']) for page in pages]
            for element in content:
                # skip elements that not expected
                if reqitem not in element['Key']:
                    continue
                if (symbol_to_find and
                    not element['Key'].endswith(symbol_to_find)):
                    continue
                object = self.s3_res.Object(
                    self.bucket_name, element['Key']
                )
                self.Logger.info(f'retrieve {element["Key"]}')
                appResults.Results.append(
                    loads(object.get()['Body'].read())
                )

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                self.Logger.warn(
                    f"Requested Item {reqitem} does not exists"
                )
            else:
                # Something else has gone wrong.
                raise
        return appResults
            

    def clean_table():
        return False

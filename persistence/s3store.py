import boto3
import logging
import app
from sys import getsizeof
from pickle import dumps


class S3Store:
    def __init__(self, bucket_name: str, log_level=logging.INFO):
        self.log_level = log_level
        self.bucket_name = bucket_name
        self.Logger = app.get_logger(__name__, level=self.log_level)

        self.s3_res = boto3.resource(
            's3',
            region_name=app.REGION,
            endpoint_url=app.S3_URI
        )
    
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
                object = self.s3_res.Object(self.bucket_name, f'{Item["date"]}/{Item["symbol"]}')
                object.put(Body=dumps(Item))
            
        except Exception as ex:
            raise app.AppException(ex, f'Failed to write data to s3!')

        return True

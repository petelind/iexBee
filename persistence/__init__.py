import boto3
import app

table_name = 'CompaniesIntegrationTesting'

dynamo_db_client = boto3.client('dynamodb', endpoint_url=app.DYNAMO_URI)
dynamo_db_resource = boto3.resource('dynamodb', endpoint_url=app.DYNAMO_URI)
dynamo_db_table = dynamo_db_resource.Table(table_name)

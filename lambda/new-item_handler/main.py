import json
import boto3
import os
import csv
from io import StringIO
from datetime import datetime

BUCKET_NAME = os.environ['BUCKET_TARGET']
ROLE_TO_ASSUME = os.environ['ROLE_TO_ASSUME']

folder = 'replication/prestamype/tc_contable'

sts = boto3.client('sts')
assumed_role = sts.assume_role(
    RoleArn=ROLE_TO_ASSUME,
    RoleSessionName="AssumeRoleForS3Write"
)

credentials = assumed_role['Credentials']
s3 = boto3.client(
    's3',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)
# s3 = boto3.client('s3')

def generate_object_key(record_date):

    date_obj = datetime.strptime(record_date, "%Y-%m-%d %H:%M:%S")
    formatted_date = date_obj.strftime("%Y%m%d")
    return f'{folder}/data_tc_{formatted_date}.csv'

def handler(event, context):
    
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['pk', 'tc_date', 'tc_contable'])
    csv_writer.writeheader()

    for record in event['Records']:
        tc_date = record['dynamodb']['NewImage']['tc_date']['S']
        tc_contable = record['dynamodb']['NewImage']['tc_contable']['N']
        pk = record['dynamodb']['NewImage']['pk']['S']

        object_key = generate_object_key(tc_date)
        
        csv_writer.writerow({'pk': pk, 'tc_date': tc_date, 'tc_contable': str(tc_contable)})
        s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps(f'File {object_key} added')
    }

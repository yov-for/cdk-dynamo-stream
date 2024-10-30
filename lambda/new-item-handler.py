import json
import boto3
import os
import csv
from io import StringIO

BUCKET_NAME = os.environ['BUCKET_TARGET']

s3 = boto3.client('s3')

def handler(event, context):
    object_key = 'test.csv'
    
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['tc_date', 'tc_contable'])
    csv_writer.writeheader()

    for record in event['Records']:
        print(record)
        tc_date = record['dynamodb']['NewImage']['tc_date']['S']
        print(tc_date)
        tc_contable = record['dynamodb']['NewImage']['tc_contable']['N']
        print(tc_contable)

        
        csv_writer.writerow({'tc_date': tc_date, 'tc_contable': str(tc_contable)})
        s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=csv_buffer.getvalue())


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

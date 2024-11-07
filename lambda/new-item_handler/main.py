import json
import boto3
import os
import csv
from io import StringIO
from datetime import datetime

BUCKET_NAME = os.environ['BUCKET_TARGET']

s3 = boto3.client('s3')
folder = 'BUCKET_FOLDER'

def handler(event, context):
    date_obj = datetime.strptime(tc_date, "%Y-%m-%d %H:%M:%S.%f")
    formatted_date = date_obj.strftime("%Y%m%d")
    object_key = f'{folder}/data_tc_{formatted_date}.csv'
    
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['tc_date', 'tc_contable'])
    csv_writer.writeheader()

    for record in event['Records']:
        tc_date = record['dynamodb']['NewImage']['tc_date']['S']
        tc_contable = record['dynamodb']['NewImage']['tc_contable']['N']
        print(tc_date, tc_contable)
        
        csv_writer.writerow({'tc_date': tc_date, 'tc_contable': str(tc_contable)})
        s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps(f'File {object_key} added')
    }

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from tempfile import mkdtemp
from datetime import datetime, timedelta, timezone
import time
import boto3
from botocore.errorfactory import ClientError
import os
import json

TABLE_NAME = os.environ['TABLE_NAME']
PATH_SCRAP_WEB = os.environ['PATH_SCRAP_WEB']
AWS_REGION = os.environ['AWS_REGION']
TAKEN_BACK_DAYS = int(os.environ['TAKEN_BACK_DAYS'])
OBJECT_PREFIX = 'replication/prestamype/tc_contable'

ddb_client = boto3.client('dynamodb', region_name=AWS_REGION)

def write2ddb(date_searched, tc):
    table_name = TABLE_NAME
    formatted_date = date_searched.strftime("%Y-%m-%d 00:00:00.000")
    item = {
        'pk': { 'S': formatted_date },
        'tc_date': { 'S': formatted_date },
        'tc_contable': { 'N': str(tc) }
    }   
    ddb_client.put_item(TableName=table_name, Item=item)

def get_message(status_code, date_searched, tc=None):
    if status_code == 404:
        message = f'Error at date {date_searched.strftime("%d/%m/%Y")}'
    elif status_code == 400:
        message = f'Date {date_searched.strftime("%d/%m/%Y")} was already requested'
    elif status_code == 200:
        message = f'Data ok at date {date_searched.strftime("%d/%m/%Y")}, tc is "{tc}"'
    return {
        'statusCode': status_code,
        'body': json.dumps({'date_searched': date_searched.strftime('%d/%m/%Y'), 'tc_contable': tc, 'message': message})
    }

def lambda_handler(event, context):

    # Chrome Settings
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log"
    )

    # Date to be searched
    date2search = (datetime.now(timezone.utc) - timedelta(hours=5) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    date2search_iteration = date2search

    # Validate if date was already requested
    tc_contable = None
    
    # In case it is sunday, set null, no need to scrap the web
    if date2search_iteration.weekday() == 6:
        write2ddb(date2search, 0)
        return get_message(200, date2search, tc_contable)
    else:
        # In case it is saturday, search the previus day, friday
        if date2search_iteration.weekday() == 5: date2search_iteration = date2search_iteration - timedelta(days=1)
        # Driver set up
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(PATH_SCRAP_WEB)
        try_check = 0
        while (tc_contable == None) and ((date2search - date2search_iteration).days <= TAKEN_BACK_DAYS):
            print('Date: ', date2search_iteration)
            try:
                input_field = driver.find_element(By.NAME, 'ctl00$cphContent$rdpDate$dateInput')
                input_field.clear()
                input_field.send_keys(date2search_iteration.strftime('%d/%m/%Y'))
                input_field.send_keys(Keys.RETURN)

                time.sleep(1.5) # Wait for data to be fetched

                datos = driver.find_elements(By.CSS_SELECTOR, '.rgMultiHeaderRow > .rgHeader.APLI_fila2:last-child')[0]
                tc_contable = float(datos.text) if datos.text != '' else None
                try_check = 0
                date2search_iteration = date2search_iteration - timedelta(days=1)
            except Exception as e:
                print('Error: ', e)
                driver.get(PATH_SCRAP_WEB)
                if try_check < 1: # 2 chances
                    try_check += 1
                else:
                    try_check = 0
                    date2search_iteration = date2search_iteration - timedelta(days=1)

        driver.close()
        if (tc_contable == None):
            return get_message(404, date2search)
        else:
            write2ddb(date2search, tc_contable)
            return get_message(200, date2search, tc_contable)

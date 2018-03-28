import os
import requests
import time
import datetime
import boto3



def request_data_API(header,page_number):
    print('requesting page {}'.format(page_number))
    url = 'https://est.tsheets.com/api/v1/timesheets?start_date=2000-01-01&page={}'.format(page_number)

    attempts = 0
    while attempts < 5:
        data = requests.get(url, headers=header)
        attempts += 1
        if data.status_code == 200:
            return True, data
        print('\t bad status code: {}. attempt {} of 5'.format(data.status_code, attempts))
        time.sleep(5)

    tprint('\t skipping page {}: failed 5 times'.format(page_number))
    return False, data

def upload_to_s3(response, bucket_name, s3_client, page_number, today):

    path = 'data/timesheets/{}_page_{}.json'.format(today, page_number)
    s3_client.put_object(Bucket=bucket_name, Key=path, Body=response.content)

if __name__=="__main__":
    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    auth_token = os.environ['CAPSTONE_API_TOKEN']
    header = {'Authorization': auth_token}
    bucket_name = os.environ['CAPSTONE_BUCKET']

    s3_client = boto3.client('s3')
    today = str(datetime.date.today())

    page_number = 1
    while True:
        status, response = request_data_API(header,page_number)

        print('\t has more: {}'.format(response.json()['more']))
        if status: # good status, continue with data uploading
            upload_to_s3(response, bucket_name, s3_client, page_number, today)

        if not response.json()['more']:
            break

        page_number += 1

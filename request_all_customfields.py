import os
import requests
import time
import datetime
import boto3


def request_page(page_number, header):
    url = 'https://rest.tsheets.com/api/v1/customfields?active=both&value_type=both&applies_to=all&page={}'.format(page_number)
    print('requesting customfields page {}'.format(page_number))

    attempts = 0
    while attempts < 5:
        response = requests.get(url, headers=header)
        attempts += 1
        if response.status_code == 200:
            return True, response
        print('\t bad status code: {}. attempt {} of 5'.format(response.status_code, attempts))

        time.sleep(5)
    print('\t skipping page {}: failed 5 times'.format(page_number))
    return False, response

def upload_to_s3(response, bucket_name, s3_client, page_number, today):

    path = 'data/customfields/{}_page_{}.json'.format(today, page_number)
    s3_client.put_object(Bucket=bucket_name, Key=path, Body=response.content)


def main():

    auth_token = os.environ['CAPSTONE_API_TOKEN']
    bucket_name = os.environ['CAPSTONE_BUCKET']
    header = {'Authorization': auth_token}

    s3_client = boto3.client('s3')
    today = str(datetime.date.today())

    page_number = 1
    while True:
        status, response = request_page(page_number, header)

        print('\t has more: {}'.format(response.json()['more']))
        if status: # good status, continue with data uploading
            upload_to_s3(response, bucket_name, s3_client, page_number, today)

        if not response.json()['more']:
            break

        page_number += 1


if __name__ == '__main__':
    main()

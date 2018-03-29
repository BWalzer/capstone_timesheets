import os
import requests
import time
import datetime
import boto3
import psycopg2


def request_page(page_number, header, customfield_id, modified_since):
    url = 'https://rest.tsheets.com/api/v1/customfielditems'
    params = {'active': 'both',
              'page': page_number,
              'customfield_id': customfield_id,
              'modified_since': modified_since + 'T00:00:00-00:00'}
    print('\t requesting customfield items page {}'.format(page_number))

    attempts = 0
    while attempts < 5:
        response = requests.get(url, headers=header, params=params)
        attempts += 1
        if response.status_code == 200:
            return True, response
        print('\t\t bad status code: {}. attempt {} of 5'.format(response.status_code, attempts))

        time.sleep(5)
    print('\t\t skipping page {}: failed 5 times'.format(page_number))
    return False, response

def upload_to_s3(response, bucket_name, s3_client, page_number, today, customfield_id):

    path = 'data/customfield_items/{}_page={}_customfield_id={}.json'.format(today, page_number, customfield_id)
    s3_client.put_object(Bucket=bucket_name, Key=path, Body=response.content)

def get_customfield_ids(conn):
    cursor = conn.cursor()

    query = 'SELECT customfield_id FROM customfields'
    cursor.execute(query)

    customfield_ids = [_[0] for _ in cursor.fetchall()]
    cursor.close()

    return customfield_ids

def main():
    auth_token = os.environ['CAPSTONE_API_TOKEN']
    bucket_name = os.environ['CAPSTONE_BUCKET']
    header = {'Authorization': auth_token}

    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
    cursor = conn.cursor()

    s3_client = boto3.client('s3')
    today = str(datetime.date.today())

    cursor.execute('SELECT MAX(last_updated) FROM customfields')
    last_updated = str(cursor.fetchall()[0][0].date())

    customfield_ids = get_customfield_ids(conn)
    for customfield_id in customfield_ids:
        print('customfield id {}'.format(customfield_id))

        page_number = 1
        while True:
            status, response = request_page(page_number, header, customfield_id, last_updated)

            if status: # good status, continue with data uploading
                upload_to_s3(response, bucket_name, s3_client, page_number, today, customfield_id)

            if not response.json()['more']:
                break

            page_number += 1

    conn.close()

if __name__ == '__main__':
    main()

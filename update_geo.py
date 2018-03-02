import pandas as pd
import pandas as pd
import psycopg2
import os
import io
import multiprocessing
import os
import json
import requests
import time
import datetime
import boto3

#This is a script for periodically uploading to all geolocation data

auth_token = os.environ['CAPSTONE_API_TOKEN']
headers = {'Authorization': auth_token}

db_name = os.environ['CAPSTONE_DB_NAME']
host = os.environ['CAPSTONE_DB_HOST']
username = os.environ['CAPSTONE_DB_USERNAME']
password = os.environ['CAPSTONE_DB_PASSWORD']

conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
today = str(datetime.date.today())


def get_files_from_s3(path):
    s3 = boto3.resource('s3')
    bucket_name = os.environ['CAPSTONE_BUCKET']
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read()
        geo=json.loads(geo_response.text)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T

    return df

#for periodic uploading
def periodic_uploaddf_tosql(df):
    cursor = conn.cursor()

    template = ', '.join(['%s'] * len(df.columns))

    #table already created with constraints
    #geo_id is primary key
    query = '''INSERT INTO geo
       (accuracy, altitude, created, device_identifier, heading,
       geo_id, latitude, longitude, source, speed, employee_id,
       last_updated)
           VALUES ({}) ON CONFLICT (geo_id)
           DO UPDATE SET last_updated={}'''.format(template, today)

    for index, row in df.iterrows():
        cursor.execute(query=query, vars=row)
    conn.commit()

def uploadlog_tosql(logentry):
    cursor = conn.cursor()
    template = ', '.join(['%s'] * len(logentry))
    querylog = '''INSERT INTO geolog
               (page, response, uploaded)
               VALUES ({})'''.format(template)

    cursor.execute(query=querylog, vars=logentry)
    conn.commit()


def main():
    page=1
    try:
        geo, georesponse=get_files_from_s3(path)
        logentry=[page, int(str(georesponse)[11:14]), datetime.datetime.now()]
        print(logentry)
        uploaddf_tosql(df)
        uploadlog_tosql(logentry)

    conn.close()



if __name__ == '__main__':
    main()

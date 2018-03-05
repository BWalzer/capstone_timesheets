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

#This is a script for mass uploading to all geolocation data

auth_token = os.environ['CAPSTONE_API_TOKEN']
headers = {'Authorization': auth_token}

db_name = os.environ['CAPSTONE_DB_NAME']
host = os.environ['CAPSTONE_DB_HOST']
username = os.environ['CAPSTONE_DB_USERNAME']
password = os.environ['CAPSTONE_DB_PASSWORD']
conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)

today = str(datetime.date.today())

def upload_to_s3(response, bucket_name, s3_client, page_number, today):

    path = 'data/jobcodes/{}_page_{}.json'.format(today, page_number)
    s3_client.put_object(Bucket=bucket_name, Key=path, Body=response.content)


def get_response(page):
    date='2000-08-01T12:00:00-06:00'
    geo_url='https://rest.tsheets.com/api/v1/geolocations'
    params = {'modified_since': date, 'page': page}
    geo_response=requests.get(geo_url, headers=headers, params=params)
    geo=json.loads(geo_response.text)
    return geo, geo_response

#for mass uploading
def mass_uploaddf_tosql(df):
    cursor = conn.cursor()

    template = ', '.join(['%s'] * len(df.columns))

    #table already created with constraints
    query = '''INSERT INTO geo
       (accuracy, altitude, created, device_identifier, heading,
       geo_id, latitude, longitude, source, speed, employee_id, )
           VALUES ({})'''.format(template)

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
        geo, georesponse=get_response(page)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T
    except:
        geo, georesponse=get_response(page)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T

    while geo['more']==True:
        page+=1
        try:
            geo, georesponse=get_response(page)
        except:
            geo, georesponse=get_response(page)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T
        logentry=[page, int(str(georesponse)[11:14]), datetime.datetime.now()]
        print(logentry)
        uploaddf_tosql(df)
        uploadlog_tosql(logentry)

    conn.close()



if __name__ == '__main__':
    main()

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

auth_token = os.environ['CAPSTONE_TOKEN']
headers = {'Authorization': auth_token}

def get_response(page):
    date='2000-08-01T12:00:00-06:00'
    geo_url='https://rest.tsheets.com/api/v1/geolocations'
    params = {'modified_since': date, 'page': page}
    geo_response=requests.get(geo_url, headers=headers, params=params)
    geo=json.loads(geo_response.text)
    return geo, geo_response

def uploaddf_tosql(df, logentry):
    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
    cursor = conn.cursor()

    template = ', '.join(['%s'] * len(df.columns))

    #table already created with constraints
    query = '''INSERT INTO geo
       (accuracy, altitude, created, device_identifier, heading,
       geo_id, latitude, longitude, source, speed, employee_id)
           VALUES ({})'''.format(template)

    for index, row in df.iterrows():
        cursor.execute(query=query, vars=row)

    querylog = '''INSERT INTO geolog
               (page, response, uploaded)
               VALUES ({})'''.format(template)


    cursor.execute(query=querylog, vars=logentry)

    conn.commit()

def main():
    page=1
    geo, georesponse=get_response(page)
    df=pd.DataFrame(geo['results']['geolocations'],index=None).T

    while geo['more']==True:
        page+=1
        geo, georesponse=get_response(page)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T
        logentry=[page, int(str(georesponse)[11:14]), datetime.datetime.now()]
        uploaddf_tosql(df, logentry)

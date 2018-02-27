import pandas as pd
import pandas as pd
import psycopg2
import os
import io
import multiprocessing
import os
import json
import requests

def main():
    auth_token = os.environ['CAPSTONE_TOKEN']

    headers = {'Authorization': auth_token}

    date='2000-08-01T12:00:00-06:00'
    page=1
    geo_url='https://rest.tsheets.com/api/v1/geolocations?modified_since={}&page={}'.format(date, page)
    geo_response=requests.get(geo_url, headers=headers)
    geo=json.loads(geo_response.text)
    df=pd.DataFrame(geo['results']['geolocations'],index=None).T

    while geo['more']==True:
        page+=1
        geo_url='https://rest.tsheets.com/api/v1/geolocations?modified_since={}&page={}'.format(date, page)
        geo_response=requests.get(geo_url, headers=headers)
        df=pd.DataFrame(geo['results']['geolocations'],index=None).T
        df=pd.concat([df,df],axis=0)
        print(page)

    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)

    cursor = conn.cursor()


    template = ', '.join(['%s'] * len(df.columns))

    query = '''INSERT INTO geo
       (accuracy, altitude, created, device_identifier, heading,
       geo_id, latitude, longitude, source, speed, employee_id)
           VALUES ({})'''.format(template)

    for index, row in df.iterrows():
        cursor.execute(query=query, vars=row)

    conn.commit()

if __name__ == '__main__':
    main()

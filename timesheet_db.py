import os
import requests
import pandas as pd
import psycopg2

def clean_to_df(data):
    timesheet=pd.DataFrame(data.json()['results']['timesheets']).T
    timesheet['end_time']=timesheet['end']
    timesheet['start_time']=timesheet['start']
    timesheet['sheet_id']=timesheet['id']
    timesheet['timezone']=timesheet['tz'].astype('int')
    timesheet['customfields']=timesheet['customfields'].astype('str')
    timesheet['end_time']=timesheet['end_time'].replace('','1900-01-01T01:01:01-01:01')
    timesheet['start_time']=timesheet['start_time'].replace('','1900-01-01T01:01:01-01:01')
    timesheet_df=timesheet[['date','duration','end_time','start_time',
    'sheet_id','jobcode_id','last_modified','location','locked','notes',
    'on_the_clock','type','timezone','tz_str','user_id','customfields','attached_files']]
    return timesheet_df

def request_data_API(auth_token,headers):
    page=2
    url = 'https://est.tsheets.com/api/v1/timesheets?start_date=2017-01-01&page=1'
    print('requesting page 1')
    data = requests.get(url, headers=headers)
    timesheet=clean_to_df(data)
    while True:
        print('requesting page {}'.format(page))
        url = 'https://est.tsheets.com/api/v1/timesheets?start_date=2017-01-01&page={}'.format(page)
        data = requests.get(url, headers=headers)
        timesheet_df=clean_to_df(data)
        timesheet = pd.concat([timesheet, timesheet_df])
        page += 1
        if data.json()['more']==False:
            break
    return timesheet

def insert_to_DB(db_name, username, host, password,timesheets):
    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
    cursor = conn.cursor()
    template = ', '.join(['%s'] * len(timesheets.columns))

    query = '''INSERT INTO timesheets
           (date,duration,end_time,start_time,
           sheet_id,jobcode_id,last_modified,location,locked,notes,
           on_the_clock,type,timezone,tz_str,user_id,customfields,attached_files)
               VALUES ({})'''.format(template)
    duprow_count=0
    for _, timesheet in timesheets.iterrows():
        try:
            cursor.execute(query=query, vars=timesheet)
        except psycopg2.IntegrityError as error:
            print (error)
            duprow_count+=1
            conn.rollback()
        conn.commit()
    conn.close()
    print (duprow_count)

if __name__=="__main__":
    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']
    auth_token = os.environ['CAPSTONE_API_TOKEN']
    headers = {'Authorization': auth_token}
    timesheet=request_data_API(auth_token,headers)
    insert_to_DB(db_name, username, host, password,timesheet)

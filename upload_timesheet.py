import boto3
import psycopg2
import pandas as pd
import json
import os


def get_file_paths(s3_client, bucket_name, prefix):
    print('\t getting all file_paths')
    bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

    if not 'Contents' in bucket_contents.keys(): # no files in the bucket
        print('\t\t no files found in bucket path')
        return False

    file_paths = [x['Key'] for x in bucket_contents['Contents']]

    return file_paths

def get_json_file(s3_client, bucket_name, file_path):
    print('\t getting the json file')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_path)

    json_file = json.loads(response['Body'].read())

    return json_file

def create_dataframe(json_file):
    print('\t creating dataframe')
    timesheet=pd.DataFrame(json_file['results']['timesheets']).T

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

def upload_to_db(conn, timesheets, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, timesheet in timesheets.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=timesheet)
        except psycopg2.IntegrityError as error:
            print(error)
            conn.rollback()
        conn.commit()
    cursor.close()

if __name__ == '__main__':
    bucket_name = os.environ['CAPSTONE_BUCKET']

    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
    s3_client = boto3.client('s3')

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/timesheets/')

    for file_path in file_paths:
        json_file = get_json_file(s3_client, bucket_name, file_path)
        request_date = file_path[16:26]

        timesheets = create_dataframe(json_file)

        template = ', '.join(['%s'] * len(timesheets.columns))
        # query = '''INSERT INTO employees
        #        (active, approved_to, client_url, company_name, created,
        #               customfields, email, email_verified, employee_number, exempt,
        #               first_name, group_id, hire_date, last_active, last_modified,
        #               last_name, manager_of_group_ids, mobile_number, pay_interval,
        #               pay_rate, payroll_id, permissions, profile_image_url,
        #               pto_balances, require_password_change, salaried, submitted_to,
        #               term_date, username, employee_id, last_updated)

        query = '''INSERT INTO timesheets
                             (date,duration,end_time,start_time,
                             sheet_id,jobcode_id,last_modified,location,locked,notes,
                             on_the_clock,type,timezone,tz_str,user_id,customfields,attached_files,last_updated)
                   VALUES ({template}, '{last_updated}')
                   ON CONFLICT(sheet_id) DO
                   UPDATE SET
                       date = excluded.date, end_time = excluded.end_time,
                       start_time = excluded.start_time, sheet_id = excluded.sheet_id,
                       jobcode_id = excluded.jobcode_id, last_modified = excluded.last_modified,
                       location=excluded.location,locked=excluded.locked,notes=excluded.notes,
                       on_the_clock=excluded.on_the_clock,type=excluded.type,timezone=excluded.timezone, tz_str=excluded.tz_str,
                       user_id=excluded.user_id,customfields=excluded.customfields,attached_files=excluded.attached_files,last_updated=excluded.
                       last_updated'''.format(template=template, last_updated=request_date)



        upload_to_db(conn, timesheets, query)

        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()

import os
from os import listdir
from os.path import isfile, join
import psycopg2
import csv
import boto3
from io import StringIO
import pandas as pd

def get_file_paths(s3_client, bucket_name, prefix):
    print('\t getting all file_paths')
    bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

    if not 'Contents' in bucket_contents.keys(): # no files in the bucket
        print('\t\t no files found in bucket path')
        return False

    file_paths = [x['Key'] for x in bucket_contents['Contents']]

    return file_paths[1:]


def get_timesheet_csv(s3_client, bucket_name, file_path):
    #pulls from s3 bucket and reads the file to pandas
    csvobj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    body=csvobj['Body']
    csvstr=body.read().decode('utf-8')
    df=pd.read_csv(StringIO(csvstr))
    return df

def upload_file_sql(df, conn):

    #for uploading csvs saved on ec2
    #with open (path, 'r', newline='') as f:
    #     reader = csv.reader(f)
    #     columns = next(reader)
    #     data=next(reader)
    query=('''INSERT INTO timesheet_logs
            (id, gmt_created, local_created, user_id, username, ts_user_id,
            ts_username, ts_id, edit_type, ip_address, message) VALUES ({})
            ON CONFLICT (id) DO
            UPDATE SET
                id=excluded.id, gmt_created=excluded.gmt_created,
                local_created=excluded.local_created, user_id=excluded.user_id,
                ts_user_id=excluded.ts_user_id, ts_username=excluded.ts_username,
                ts_id=excluded.ts_id, edit_type=excluded.edit_type,
                ip_address=excluded.ip_address, message=excluded.message'''
                .format(','.join(['%s'] * len(df.columns))))

    cursor = conn.cursor()
    for _,row in df.iterrows():
        cursor.execute(query=query, vars=row)
    conn.commit()
    cursor.close()

def main():

    bucket_name = os.environ['CAPSTONE_BUCKET']

    db_name = os.environ['CAPSTONE_DB_NAME']
    host = os.environ['CAPSTONE_DB_HOST']
    username = os.environ['CAPSTONE_DB_USERNAME']
    password = os.environ['CAPSTONE_DB_PASSWORD']

    conn = psycopg2.connect(database=db_name, user=username, host=host, password=password)
    s3_client = boto3.client('s3')

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/timesheet_logs/')

    if file_paths: # file paths found, continue
        for file_path in file_paths:
            df=get_timesheet_csv(s3_client, bucket_name, file_path)
            upload_file_sql(df, conn)
            print('{} has been uploaded'.format(file_path))

    #s3_client.delete_object(Bucket=bucket_name, Key=file_path)

if __name__ == '__main__':
    main()

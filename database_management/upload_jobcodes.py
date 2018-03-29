import boto3
import psycopg2
import pandas as pd
import json
import os


def get_file_paths(s3_client, bucket_name, prefix):
    print('\t getting all file_paths')
    bucket_contents = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)

    file_paths = [x['Key'] for x in bucket_contents['Contents']]

    return file_paths

def get_json_file(s3_client, bucket_name, file_path):
    print('\t getting the json file')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_path)

    json_file = json.loads(response['Body'].read())

    return json_file

def create_dataframe(json_file):
    print('\t creating dataframe')
    jobcodes = pd.DataFrame(json_file['results']['jobcodes']).T

    print(jobcodes.columns)
    jobcodes['jobcode_id'] = jobcodes['id']
    jobcodes.drop(columns=['id'], inplace=True)


    return jobcodes

def upload_to_db(conn, jobcodes, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, jobcode in jobcodes.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=jobcode)
        except psycopg2.IntegrityError as error:
            print(error)
            conn.rollback()
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

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/jobcodes/')

    for file_path in file_paths:
        json_file = get_json_file(s3_client, bucket_name, file_path)

        last_updated = file_path[14:24]
        if json_file['results']['jobcodes'] != []: # check for empty response
            jobcodes = create_dataframe(json_file)

            template = ', '.join(['%s'] * len(jobcodes.columns))
            query = ('''INSERT INTO jobcodes
                           (active, assigned_to_all, billable, billable_rate, created,
                           filtered_customfielditems, has_children, last_modified,
                           locations, name, parent_id, required_customfields, short_code,
                           type, jobcode_id, last_updated)
                       VALUES ({template}, '{last_updated}')
                       ON CONFLICT(jobcode_id) DO
                       UPDATE SET
                           active = excluded.active, assigned_to_all = excluded.assigned_to_all,
                           billable = excluded.billable, billable_rate = excluded.billable_rate,
                           created = excluded.created, filtered_customfielditems = excluded.filtered_customfielditems,
                           has_children = excluded.has_children, last_modified = excluded.last_modified,
                           locations = excluded.locations, name = excluded.name,
                           parent_id = excluded.parent_id, required_customfields = excluded.required_customfields,
                           short_code = excluded.short_code, type = excluded.type,
                           jobcode_id = excluded.jobcode_id, last_updated = excluded.last_updated'''
                           .format(template=template, last_updated=last_updated))

            upload_to_db(conn, jobcodes, query)

        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()



if __name__ == '__main__':
    main()

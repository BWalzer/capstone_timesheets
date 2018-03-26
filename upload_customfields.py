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
    customfields = pd.DataFrame(json_file['results']['customfields']).T

    customfields['customfield_id'] = customfields['id']
    customfields.drop(columns=['id'], inplace=True)


    return customfields

def upload_to_db(conn, customfields, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, customfield in customfields.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=customfield)
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

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/customfields/')

    if file_paths: # files found, no problem
        for file_path in file_paths:
            json_file = get_json_file(s3_client, bucket_name, file_path)
            request_date = file_path[18:28]

            if json_file['results']['customfields'] != []:
                customfields = create_dataframe(json_file)

                template = ', '.join(['%s'] * len(customfields.columns))
                query = '''INSERT INTO customfields
                              (active, applies_to, created, last_modified, name,
                              regex_filter, required, required_customfields, short_code,
                              type, ui_preference, customfield_id, last_updated)
                           VALUES ({template}, '{last_updated}')
                           ON CONFLICT(customfield_id) DO
                           UPDATE SET
                              active = excluded.active, applies_to = excluded.applies_to,
                              created = excluded.created, last_modified = excluded.last_modified,
                              name = excluded.name, regex_filter = excluded.regex_filter,
                              required = excluded.required, required_customfields = excluded.required_customfields,
                              short_code = excluded.short_code, type = excluded.type,
                              ui_preference = excluded.ui_preference, customfield_id = excluded.customfield_id,
                              last_updated = excluded.last_updated'''.format(template=template, last_updated=request_date)

                upload_to_db(conn, customfields, query)

            s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()



if __name__ == '__main__':
    main()

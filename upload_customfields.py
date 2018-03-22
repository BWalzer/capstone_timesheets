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
    print('\t creating customfield items dataframe')
    customfield_items = pd.DataFrame(json_file['results']['customfielditems']).T

    customfield_items['customfield_item_id'] = customfield_items['id']
    customfield_items.drop(columns=['id'], inplace=True)


    return customfield_items

def upload_to_db(conn, customfield_items, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, customfield_item in customfield_items.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=customfield_item)
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

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/customfield_items/')

    for file_path in file_paths:
        json_file = get_json_file(s3_client, bucket_name, file_path)

        customfield_items = create_dataframe(json_file)

        template = ', '.join(['%s'] * len(customfield_items.columns))
        query = '''INSERT INTO customfield_items
               (active, customfield_id, last_modified, name,
                      required_customfields, short_code, customfield_item_id)
                   VALUES ({})'''.format(template)

        upload_to_db(conn, customfield_items, query)

        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()



if __name__ == '__main__':
    main()

import boto3
import psycopg2
import pandas as pd
import json
import os
import datetime
#This is a script for periodically uploading to all geolocation data


#need to edit to get one object at a time and the correct file name
#reference date in the filename

def get_file_paths(s3_client, bucket_name, prefix):
    #structured to take 1000 at a time (s3 API limit)
    #gets the next one and appends it
    print('\t getting all file_paths')
    file_paths=[]

    kwargs={'Bucket': bucket_name, 'Prefix': prefix}
    while True:
        bucket_contents = s3_client.list_objects_v2(**kwargs)
        file_paths.append([x['Key'] for x in bucket_contents['Contents']])

        try:
            kwargs['ContinuationToken']=bucket_contents['NextContinuationToken']
        except KeyError:
            break
    #flattens the file_paths
    file_path_list=[file for pages in file_paths for file in pages]
    return file_path_list

def get_json_file(s3_client, bucket_name, file_path):
    print('\t getting the json file')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    json_file = json.loads(response['Body'].read())
    return json_file

def create_dataframe(json_file):
    today = str(datetime.date.today())
    df=pd.DataFrame(json_file['results']['geolocations'],index=None).T
    return df

#for periodic uploading

def upload_to_db(conn, df, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, geo_item in df.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=geo_item)
        except psycopg2.IntegrityError as error:
            print(error)
            conn.rollback()
        conn.commit()
    cursor.close()

def uploadlog_tosql(logentry):
    cursor = conn.cursor()
    template = ', '.join(['%s'] * len(logentry))
    querylog = '''INSERT INTO geolog
               (uploaded, filename)
               VALUES ({})'''.format(template)

    cursor.execute(query=querylog, vars=logentry)
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

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/geolocations/')

    #table already created with constraints
    #geo_id is primary key

    for file_path in file_paths:
        json_file = get_json_file(s3_client, bucket_name, file_path)

        request_date=file_path[18:28]

        geo_items = create_dataframe(json_file)

        template = ', '.join(['%s'] * len(geo_items.columns))

        query = '''INSERT INTO geo
            (accuracy, altitude, created, device_identifier, heading,
            geo_id, latitude, longitude, source, speed, employee_id, last_updated)
           VALUES ({template}, '{last_updated}') ON CONFLICT (geo_id)
           DO UPDATE SET
           accuracy=excluded.accuracy, altitude=excluded.altitude,
           created=excluded.created, device_identifier=excluded.device_identifier,
           heading=excluded.heading, geo_id=excluded.geo_id, latitude=excluded.latitude,
           longitude=excluded.longitude, source=excluded.source, speed=excluded.speed,
           employee_id=excluded.employee_id,last_updated=excluded.last_updated
           '''.format(template=template, last_updated=request_date)

        upload_to_db(conn, geo_items, query)

        logentry=[request_date, file_path]
        uploadlog_tosql(logentry)
        
        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()


if __name__ == '__main__':
    main()

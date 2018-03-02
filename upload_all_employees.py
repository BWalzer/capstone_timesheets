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
    employees = pd.DataFrame(json_file['results']['users']).T

    employees['employee_id'] = employees['id']
    employees.drop(columns=['id'], inplace=True)

    employees['permissions'] = employees['permissions'].astype(str)
    employees['pto_balances'] = employees['pto_balances'].astype(str)

    employees['created'] = employees['created'].apply(lambda x: None if (x == '0000-00-00' or not x) else x)
    employees['last_active'] = employees['last_active'].apply(lambda x: None if (x == '0000-00-00' or not x) else x)
    employees['last_modified'] = employees['last_modified'].apply(lambda x: None if (x == '0000-00-00' or not x) else x)
    employees['term_date'] = employees['term_date'].apply(lambda x: None if (x == '0000-00-00' or not x) else x)
    employees['hire_date'] = employees['hire_date'].apply(lambda x: None if (x == '0000-00-00' or not x) else x)

    return employees

def upload_to_db(conn, employees, query):
    print('\t uploading to dataframe')
    cursor = conn.cursor()

    for _, jobcode in employees.iterrows():
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

    file_paths = get_file_paths(s3_client, bucket_name, prefix='data/employees/')

    for file_path in file_paths:
        json_file = get_json_file(s3_client, bucket_name, file_path)

        employees = create_dataframe(json_file)

        template = ', '.join(['%s'] * len(employees.columns))
        query = '''INSERT INTO employees
               (active, approved_to, client_url, company_name, created,
                      customfields, email, email_verified, employee_number, exempt,
                      first_name, group_id, hire_date, last_active, last_modified,
                      last_name, manager_of_group_ids, mobile_number, pay_interval,
                      pay_rate, payroll_id, permissions, profile_image_url,
                      pto_balances, require_password_change, salaried, submitted_to,
                      term_date, username, employee_id)
                   VALUES ({})'''.format(template)

        upload_to_db(conn, employees, query)

        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()



if __name__ == '__main__':
    main()

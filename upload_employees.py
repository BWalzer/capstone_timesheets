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

    for _, employee in employees.iterrows():
        print('\t\t uploading row')
        try:
            cursor.execute(query=query, vars=employee)
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
        request_date = file_path[15:25]
        employees = create_dataframe(json_file)

        template = ', '.join(['%s'] * len(employees.columns))
        query = '''INSERT INTO employees
                      (active, approved_to, client_url, company_name, created,
                      customfields, email, email_verified, employee_number, exempt,
                      first_name, group_id, hire_date, last_active, last_modified,
                      last_name, manager_of_group_ids, mobile_number, pay_interval,
                      pay_rate, payroll_id, permissions, profile_image_url,
                      pto_balances, require_password_change, salaried, submitted_to,
                      term_date, username, employee_id, last_updated)
                   VALUES ({template}, '{last_updated}')
                   ON CONFLICT(employee_id) DO
                   UPDATE SET
                       active = excluded.active, approved_to = excluded.approved_to,
                       client_url = excluded.client_url, company_name = excluded.company_name,
                       created = excluded.created, customfields = excluded.customfields,
                       email = excluded.email, email_verified = excluded.email_verified,
                       employee_number = excluded.employee_number, exempt = excluded.exempt,
                       first_name = excluded.first_name, group_id = excluded.group_id,
                       hire_date = excluded.hire_date, last_active = excluded.last_active,
                       last_modified = excluded.last_modified, last_name = excluded.last_name,
                       manager_of_group_ids = excluded.manager_of_group_ids,
                       mobile_number = excluded.mobile_number, pay_interval = excluded.pay_interval,
                       pay_rate = excluded.pay_rate, payroll_id = excluded.payroll_id,
                       permissions = excluded.permissions, profile_image_url = excluded.profile_image_url,
                       pto_balances = excluded.pto_balances, require_password_change = excluded.require_password_change,
                       salaried = excluded.salaried, submitted_to = excluded.submitted_to,
                       term_date = excluded.term_date, username = excluded.username,
                       employee_id = excluded.employee_id, last_updated = excluded.last_updated'''.format(template=template, last_updated=str(request_date))

        upload_to_db(conn, employees, query)

        s3_client.delete_object(Bucket=bucket_name, Key=file_path)

    conn.close()



if __name__ == '__main__':
    main()

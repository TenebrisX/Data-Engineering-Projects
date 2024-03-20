import time
import requests
import json
import os
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

# Retrieve HTTP connection details from Airflow connection 'http_conn_id'
http_conn_id = HttpHook.get_connection('http_conn_id')  # Get connection details
api_key = http_conn_id.extra_dejson.get('api_key')  # Extract API key
base_url = http_conn_id.host  # Extract base URL

# Define PostgreSQL connection ID
postgres_conn_id = 'postgresql_de'  # PostgreSQL connection ID

# Constants for nickname and cohort
nickname = 'kotlyarov-bar'  # User nickname
cohort = '21'  # Cohort number

# Define headers for API requests
headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

# Function to make HTTP requests
def make_request(ti, endpoint, method='GET', params=None):
    """
    Make HTTP requests to the specified endpoint.

    Parameters:
        - ti (TaskInstance): The task instance.
        - endpoint (str): The URL endpoint.
        - method (str): The HTTP method (GET or POST).
        - params (dict): Optional parameters for the request.

    Returns:
        - response (requests.Response): The HTTP response object.
    """
    print(f'Making {method} request to {endpoint}')  # Log request initiation

    if method == 'GET':
        response = requests.get(endpoint, headers=headers, params=params)
    elif method == 'POST':
        response = requests.post(endpoint, headers=headers)

    response.raise_for_status()  # Raise an exception for HTTP errors

    print(f'Response is {response.content}')  # Log response content
    return response

# Function to generate a report and push task_id to XCom
def generate_report(ti):
    """
    Function to generate a report by making a POST request and push task_id to XCom.

    Parameters:
        - ti (TaskInstance): The task instance.

    Returns:
        None
    """
    response = make_request(ti, f'{base_url}/generate_report', method='POST')
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)

# Function to get a report and push report_id to XCom
def get_report(ti):
    """
    Function to get a report by making GET requests and push report_id to XCom.

    Parameters:
        - ti (TaskInstance): The task instance.

    Returns:
        None
    """
    task_id = ti.xcom_pull(key='task_id')
    report_id = None

    for i in range(20):
        response = make_request(ti, f'{base_url}/get_report', method='GET', params={'task_id': task_id})
        status = json.loads(response.content)['status']

        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')

# Function to upload files from S3
def upload_from_s3(ti, file_names):
    """
    Function to upload files from S3.

    Parameters:
        - ti (TaskInstance): The task instance.
        - file_names (list): List of file names to be uploaded.

    Returns:
        None
    """
    response = make_request(ti, f'{base_url}upload_from_s3/?report_id={report_id}&date={str(date)}T00:00:00',
                            headers=headers)
    response.raise_for_status()

    source_path = 'https://storage.yandexcloud.net/s3-sprint3/cohort_21/kotlyarov-bar/project/TWpBeU15MHhNaTB5T0ZRd056b3lOem96TkFscmIzUnNlV0Z5YjNZdFltRnk=/'
    dest_path = '/lessons/original_csvs'

    for s in file_names:
        dest_file_path = os.path.join(dest_path, s)

        if os.path.exists(dest_file_path):
            print(f"File '{s}' already exists. Skipping import.")
        else:
            df = pd.read_csv(os.path.join(source_path, s), sep=',')
            df.to_csv(dest_file_path, index=False)
            print(f"File '{s}' imported successfully.")

# Function to upload data to staging in PostgreSQL
def upload_data_to_staging(ti, filename, pg_table, pg_schema):
    """
    Function to upload data to staging in PostgreSQL.

    Parameters:
        - ti (TaskInstance): The task instance.
        - filename (str): Name of the file to be uploaded.
        - pg_table (str): PostgreSQL table name.
        - pg_schema (str): PostgreSQL schema name.

    Returns:
        None
    """
    path = '/lessons/original_csvs/'
    df = pd.read_csv(path + filename)

    if 'id' in df.columns:
        df = df.drop('id', axis=1)
    if 'uniq_id' in df.columns:
        df = df.drop_duplicates(subset=['uniq_id'])

    if filename == 'user_order_log.csv':
        if 'status' not in df.columns:
            df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    print(f'{row_count} rows were inserted')

# Define the DAG
dag = DAG(
    dag_id='s3_load',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['original tables load'],
)

# Define the tasks
generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,  # Indicates that the function should receive the 'ti' parameter
    dag=dag)

get_report = PythonOperator(
    task_id='get_report',
    python_callable=get_report,
    provide_context=True,  # Indicates that the function should receive the 'ti' parameter
    dag=dag)

t_upload_from_s3 = PythonOperator(
    task_id='upload_from_s3',
    python_callable=upload_from_s3,
    provide_context=True,  # Indicates that the function should receive the 'ti' parameter
    op_kwargs={'file_names': ['customer_research.csv', 'user_activity_log.csv',
             'user_order_log.csv', 'price_log.csv']},
    dag=dag)

# Define tasks for uploading data to staging
tables_to_staging = ['customer_research', 'user_activity_log', 'user_order_log', 'price_log']
upload_to_staging_tasks = []

for table in tables_to_staging:
    upload_task = PythonOperator(
        task_id=f'upload_{table}_to_staging',
        python_callable=upload_data_to_staging,
        provide_context=True,  # Indicates that the function should receive the 'ti' parameter
        op_kwargs={'filename': f'{table}.csv',
                   'pg_table': table,
                   'pg_schema': 'staging'},
        dag=dag
    )
    upload_to_staging_tasks.append(upload_task)

# Set up task dependencies
generate_report >> get_report >> t_upload_from_s3

for upload_task in upload_to_staging_tasks:
    t_upload_from_s3 >> upload_task

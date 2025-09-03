"""Airflow DAG for source tables processing with data quality checks.

This module contains an Airflow DAG that handles source table processing, including
data loading from S3, staging operations, and comprehensive data quality checks
with result tracking.
"""

import requests
import json
import os

import pandas as pd

import psycopg2
import psycopg2.extras

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

http_conn_id = HttpHook.get_connection('http_conn_id')  
api_key = http_conn_id.extra_dejson.get('api_key')  
base_url = http_conn_id.host  

postgres_conn_id = 'postgresql_de'  

nickname = 'kotlyarov-bar'  
cohort = '21'  

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def make_request(ti, endpoint, method='GET', params=None):
    """Make HTTP requests to the specified endpoint.

    Args:
        ti (TaskInstance): The task instance.
        endpoint (str): The URL endpoint.
        method (str): The HTTP method (GET or POST). Defaults to 'GET'.
        params (dict, optional): Parameters for the request. Defaults to None.

    Returns:
        requests.Response: The HTTP response object.

    Raises:
        requests.exceptions.HTTPError: If HTTP request fails.
    """
    print(f'Making {method} request to {endpoint}')  
    if method == 'GET':
        response = requests.get(endpoint, headers=headers, params=params)
    elif method == 'POST':
        response = requests.post(endpoint, headers=headers)
    response.raise_for_status()  
    print(f'Response is {response.content}')  
    return response


def generate_report(ti):
    """Generate a report by making a POST request and push task_id to XCom.

    Args:
        ti (TaskInstance): The task instance.
    """
    response = make_request(ti, f'{base_url}/generate_report', method='POST')
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)


def get_report(ti):
    """Get a report by making GET requests and push report_id to XCom.

    Polls the API endpoint until report generation is complete or timeout occurs.

    Args:
        ti (TaskInstance): The task instance.

    Raises:
        TimeoutError: If report is not ready after maximum attempts.
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


def upload_from_s3(ti, file_names):
    """Upload files from S3 to local storage.

    Args:
        ti (TaskInstance): The task instance.
        file_names (list): List of file names to be uploaded.
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


def upload_data_to_staging(ti, filename, pg_table, pg_schema):
    """Upload data to staging in PostgreSQL.

    Args:
        ti (TaskInstance): The task instance.
        filename (str): Name of the file to be uploaded.
        pg_table (str): PostgreSQL table name.
        pg_schema (str): PostgreSQL schema name.
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


def check_failure_file_customer_research(context):
    """Insert DQ check result for failed customer_research file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_customer_research",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('customer_research', 'file_sensor', CURRENT_DATE, 1)
          """
    )


def check_success_file_customer_research(context):
    """Insert DQ check result for successful customer_research file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_customer_research",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('customer_research', 'file_sensor', CURRENT_DATE, 0)
          """
    )


def check_success_file_user_order_log(context):
    """Insert DQ check result for successful user_order_log file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'file_sensor', CURRENT_DATE, 0)
          """
    )


def check_failure_file_user_order_log(context):
    """Insert DQ check result for failed user_order_log file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'file_sensor', CURRENT_DATE, 1)
          """
    )


def check_success_file_user_activity_log(context):
    """Insert DQ check result for successful user_activity_log file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_file_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'file_sensor', CURRENT_DATE, 0)
          """
    )


def check_failure_file_user_activity_log(context):
    """Insert DQ check result for failed user_activity_log file sensor.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_file_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'file_sensor', CURRENT_DATE, 1)
          """
    )


def check_success_insert_user_order_log(context):
    """Insert DQ check result for successful user_order_log null value check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'user_order_log_isNull', CURRENT_DATE, 0)
          """
    )


def check_failure_insert_user_order_log(context):
    """Insert DQ check result for failed user_order_log null value check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'user_order_log_isNull', CURRENT_DATE, 1)
          """
    )


def check_success_insert_user_activity_log(context):
    """Insert DQ check result for successful user_activity_log null value check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'user_activity_log_isNull', CURRENT_DATE, 0)
          """
    )


def check_failure_insert_user_activity_log(context):
    """Insert DQ check result for failed user_activity_log null value check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'user_activity_log_isNull', CURRENT_DATE, 1)
          """
    )


def check_success_insert_user_order_log2(context):
    """Insert DQ check result for successful user_order_log row count check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_order_log2",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'check_row_count_user_order_log', CURRENT_DATE, 0)
          """
    )


def check_failure_insert_user_order_log2(context):
    """Insert DQ check result for failed user_order_log row count check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_order_log2",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_order_log', 'check_row_count_user_order_log', CURRENT_DATE, 1)
          """
    )


def check_success_insert_user_activity_log2(context):
    """Insert DQ check result for successful user_activity_log row count check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="success_insert_user_activity_log2",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'check_row_count_user_activity_log', CURRENT_DATE, 0)
          """
    )


def check_failure_insert_user_activity_log2(context):
    """Insert DQ check result for failed user_activity_log row count check.

    Args:
        context (dict): The context dictionary containing task execution context.
    """
    insert_dq_checks_results = PostgresOperator(
        task_id="failure_insert_user_activity_log2",
        sql="""
            INSERT INTO dq_checks_results
            VALUES ('user_activity_log', 'check_row_count_user_activity_log', CURRENT_DATE, 1)
          """
    )


dag = DAG(
    dag_id='source_tables_dag',
    schedule_interval="@daily",
    start_date=datetime(2020, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['source_tables'],
)

begin = DummyOperator(task_id="begin", dag=dag)

with TaskGroup(group_id='group1', dag=dag) as fg1:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
        dag=dag
    )

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report,
        provide_context=True,
        dag=dag
    )

    upload_from_s3 = PythonOperator(
        task_id='upload_from_s3',
        python_callable=upload_from_s3,
        provide_context=True,
        op_kwargs={'file_names': ['customer_research.csv', 'user_activity_log.csv', 'user_order_log.csv', 'price_log.csv']},
        dag=dag
    )

with TaskGroup(group_id='group2', dag=dag) as fg2:
    load_customer_research = PythonOperator(
        task_id='load_customer_research',
        python_callable=upload_data_to_staging,
        op_kwargs={'filename': str(datetime.now().date()) + "customer_research.csv", 'pg_table': 'stage.customer_research'},
        dag=dag
    )

    load_user_order_log = PythonOperator(
        task_id='load_user_order_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'filename': str(datetime.now().date()) + "user_order_log.csv", 'pg_table': 'stage.user_order_log'},
        dag=dag
    )

    load_user_activity_log = PythonOperator(
        task_id='load_user_activity_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'filename': str(datetime.now().date()) + "user_activity_log.csv", 'pg_table': 'stage.user_activity_log'},
        dag=dag
    )

    load_price_log = PythonOperator(
        task_id='load_price_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'filename': str(datetime.now().date()) + "price_log.csv", 'pg_table': 'stage.price_log'},
        dag=dag
    )

    sql_check = SQLCheckOperator(
        task_id="user_order_log_isNull",
        sql="sql/data_quality_check/user_order_log_isNull_check.sql",
        on_success_callback=check_success_insert_user_order_log,
        on_failure_callback=check_failure_insert_user_order_log,
        dag=dag
    )

    sql_check2 = SQLCheckOperator(
        task_id="user_activity_log_isNull",
        sql="sql/data_quality_check/datauser_activity_log_isNull_check.sql",
        on_success_callback=check_success_insert_user_activity_log,
        on_failure_callback=check_failure_insert_user_activity_log,
        dag=dag
    )

    sql_check3 = SQLValueCheckOperator(
        task_id='check_row_count_user_order_log',
        sql="Select count(distinct(customer_id)) from user_order_log",
        pass_value=3,
        tolerance=0.1,
        on_success_callback=check_success_insert_user_order_log2,
        on_failure_callback=check_failure_insert_user_order_log2,
        dag=dag
    )

    sql_check4 = SQLValueCheckOperator(
        task_id='check_row_count_user_activity_log',
        sql="Select count(distinct(customer_id)) from user_activity_log",
        pass_value=3,
        tolerance=0.1,
        on_success_callback=check_success_insert_user_activity_log2,
        on_failure_callback=check_failure_insert_user_activity_log2,
        dag=dag
    )

end = DummyOperator(task_id="end", dag=dag)

begin >> [fg1, fg2] >> end
fg1 >> generate_report >> get_report >> upload_from_s3
fg2 >> [load_customer_research, load_user_order_log, load_user_activity_log, load_price_log]
load_user_order_log >> [sql_check, sql_check3]
load_user_activity_log >> [sql_check2, sql_check4]
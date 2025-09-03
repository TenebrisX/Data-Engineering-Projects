"""Airflow DAG for incremental sales mart and customer retention data processing.

This module contains an Airflow DAG that handles incremental data loading from external APIs,
processes staging data, and updates mart tables for sales and customer retention analytics.
"""

import time
import requests
import json
import pandas as pd
from psycopg2.errors import UniqueViolation

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

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


def generate_report(ti):
    """Generate a report via API and push task_id to XCom.

    Makes a POST request to the generate_report API endpoint, extracts the task_id
    from the response, and pushes it to XCom for use in subsequent tasks.

    Args:
        ti (TaskInstance): The task instance providing access to XCom and context.
    """
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()

    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)

    print(f'Response is {response.content}')


def get_report(ti):
    """Retrieve a report_id from API endpoint and push to XCom.

    Makes repeated GET requests to the get_report API endpoint until
    a 'SUCCESS' status is received, or until maximum attempts reached.
    Extracts the report_id and pushes it to XCom.

    Args:
        ti (TaskInstance): The task instance providing access to XCom and context.

    Raises:
        TimeoutError: If report_id is not obtained after maximum attempts.
    """
    print('Making request get_report')

    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()

        print(f'Response is {response.content}')

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


def get_increment(date, ti):
    """Retrieve an increment_id from API endpoint and push to XCom.

    Makes a GET request to the get_increment API endpoint, extracts the
    increment_id from the response, and pushes it to XCom.

    Args:
        date (str): The date for which the increment_id is retrieved.
        ti (TaskInstance): The task instance providing access to XCom and context.

    Raises:
        ValueError: If increment_id is empty, indicating API call error.
    """
    print('Making request get_increment')

    report_id = ti.xcom_pull(key='report_id')

    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()

    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')

    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    """Upload data from S3 file to PostgreSQL staging table.

    Retrieves increment_id from XCom, downloads file from S3, cleans data by removing
    duplicates and unwanted columns, and inserts DataFrame into PostgreSQL table.

    Args:
        filename (str): The name of the file to download from S3 and upload.
        date (str): The date associated with the data, used in local filename.
        pg_table (str): The name of the PostgreSQL table to upload data.
        pg_schema (str): The PostgreSQL schema where the table is located.
        ti (TaskInstance): The task instance providing access to XCom and context.
    """
    if pg_table in ('user_order_log', 'user_activity_log'):
        clean_or_replace_data(pg_table, date, date)

    try:
        increment_id = ti.xcom_pull(key='increment_id')

        s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
        print(s3_filename)

        local_filename = date.replace('-', '') + '_' + filename
        print(local_filename)

        response = requests.get(s3_filename)
        response.raise_for_status()

        open(f"{local_filename}", "wb").write(response.content)
        print(response.content)

        df = pd.read_csv(local_filename)

        if 'id' in df.columns:
            df = df.drop('id', axis=1)
        if 'uniq_id' in df.columns:
            df = df.drop_duplicates(subset=['uniq_id'])

        if filename == 'user_order_log_inc.csv':
            if 'status' not in df.columns:
                df['status'] = 'shipped'

        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
        print(f'{row_count} rows was inserted')

    except UniqueViolation:
        print('Record with uniq_id already exists. Skipping the task.')


def clean_or_replace_data(pg_table, start_date, end_date):
    """Clean or replace data in PostgreSQL table within specified date range.

    Constructs and executes SQL script to delete data from the specified PostgreSQL table
    within the given date range.

    Args:
        pg_table (str): The name of the PostgreSQL table to clean or replace data.
        start_date (str): The start date of the date range for data removal.
        end_date (str): The end date of the date range for data removal.
    """
    sql_script = f"delete from staging.{pg_table} where date_time between '{start_date}' and '{end_date}';"

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    with engine.connect() as connection:
        connection.execute(sql_script)


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}' 

with DAG(
        'sales_mart_and_customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
        tags=['increment tables load', 'f_sales','f_customer_retention'],
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_log_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})

    upload_customer_research_inc = PythonOperator(
        task_id='upload_customer_research_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research_inc.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})

    upload_user_activity_log_inc = PythonOperator(
        task_id='upload_user_activity_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_activity_log_inc.csv',
                   'pg_table': 'user_activity_log',
                   'pg_schema': 'staging'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}})

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}})

    generate_report >> get_report >> get_increment
    get_increment >> [upload_user_order_log_inc, upload_customer_research_inc, upload_user_activity_log_inc]
    upload_user_activity_log_inc >> update_d_city_table >> [update_d_item_table, update_d_customer_table]
    update_d_customer_table >> update_f_sales >> update_f_customer_retention
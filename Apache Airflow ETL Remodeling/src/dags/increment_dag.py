# Import necessary libraries
import time  # Time-related functions
import requests  # HTTP requests
import json  # JSON parsing
import pandas as pd  # Data manipulation with pandas
from psycopg2.errors import UniqueViolation # UniqueViolation error from psycopg2

# Import specific components from the datetime module
from datetime import datetime, timedelta  # Date and time handling

# Importing required modules and operators from Airflow
from airflow import DAG  # Directed Acyclic Graph in Airflow
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator  # Python operators
from airflow.providers.postgres.operators.postgres import PostgresOperator  # PostgreSQL operator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # PostgreSQL hook
from airflow.hooks.http_hook import HttpHook  # HTTP hook for Airflow

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

# Function to generate a report and push task_id to XCom
def generate_report(ti):
    """
    Initiates the generation of a report via a POST request and pushes the resulting task_id to XCom.

    Parameters:
    - ti (TaskInstance): The task instance, providing access to XCom and other context.

    Returns:
    None

    This function makes a POST request to the generate_report API endpoint, extracts the task_id
    from the response, and pushes it to XCom for use in subsequent tasks.

    Example usage:
    generate_report(task_instance)
    """

    print('Making request generate_report')  # Log request initiation

    # Make a POST request to generate_report endpoint
    response = requests.post(f'{base_url}/generate_report', headers=headers)  # Send POST request
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Extract task_id from the response and push it to XCom
    task_id = json.loads(response.content)['task_id']  # Extract task_id from JSON response
    ti.xcom_push(key='task_id', value=task_id)  # Push task_id to XCom

    # Print the response content
    print(f'Response is {response.content}')  # Log response content


# curl --location --request POST 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/generate_report' \
# --header 'X-Nickname: kotlyarov-bar' \
# --header 'X-Cohort: 21' \
# --header 'X-Project: True' \
# --header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460' \
# --data-raw ''


# Function to get a report and push report_id to XCom
def get_report(ti):
    """
    Retrieves a report_id from an API endpoint and pushes it to XCom.

    Parameters:
    - ti (TaskInstance): The task instance, providing access to XCom and other context.

    Returns:
    None

    This function makes repeated GET requests to the get_report API endpoint until
    a 'SUCCESS' status is received, or until a maximum of 20 attempts. It extracts
    the report_id from the response and pushes it to XCom for use in subsequent tasks.

    Example usage:
    get_report(task_instance)
    """

    print('Making request get_report')  # Log request initiation

    # Retrieve task_id from XCom
    task_id = ti.xcom_pull(key='task_id')  # Pull task_id from XCom

    report_id = None  # Initialize report_id

    # Try to get the report for up to 20 attempts
    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)  # Send GET request
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Print the response content
        print(f'Response is {response.content}')  # Log response content

        # Check if the status is 'SUCCESS'
        status = json.loads(response.content)['status']  # Extract status from JSON response
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']  # Extract report_id from JSON response
            break  # Break the loop if status is 'SUCCESS'
        else:
            time.sleep(10)  # Wait for 10 seconds before the next attempt

    # Raise TimeoutError if report_id is not obtained
    if not report_id:
        raise TimeoutError()  # Raise TimeoutError exception

    # Push report_id to XCom
    ti.xcom_push(key='report_id', value=report_id)  # Push report_id to XCom
    print(f'Report_id={report_id}')  # Log report_id


# curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_report?task_id=MjAyMy0xMi0yOFQwNzoyNzozNAlrb3RseWFyb3YtYmFy' \
# --header 'X-Nickname: kotlyarov-bar' \
# --header 'X-Cohort: 21' \
# --header 'X-Project: True' \
# --header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460'


# Function to get an increment and push increment_id to XCom
def get_increment(date, ti):
    """
    Retrieves an increment_id from an API endpoint and pushes it to XCom.

    Parameters:
    - date (str): The date for which the increment_id is retrieved.
    - ti (TaskInstance): The task instance, providing access to XCom and other context.

    Returns:
    None

    This function makes a GET request to the get_increment API endpoint, extracts the
    increment_id from the response, and pushes it to XCom for use in subsequent tasks.

    Example usage:
    get_increment('2023-01-01', task_instance)
    """

    print('Making request get_increment')  # Log request initiation

    # Retrieve report_id from XCom
    report_id = ti.xcom_pull(key='report_id')  # Pull report_id from XCom

    # Make a GET request to get_increment endpoint
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)  # Send GET request
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Print the response content
    print(f'Response is {response.content}')  # Log response content

    # Extract increment_id from the response and push it to XCom
    increment_id = json.loads(response.content)['data']['increment_id']  # Extract increment_id from JSON response
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')  # Raise ValueError if increment_id is empty

    # Push increment_id to XCom
    ti.xcom_push(key='increment_id', value=increment_id)  # Push increment_id to XCom
    print(f'increment_id={increment_id}')  # Log increment_id


# curl --location --request GET 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net/get_increment?report_id=TWpBeU15MHhNaTB5T0ZRd056b3lOem96TkFscmIzUnNlV0Z5YjNZdFltRnk=&date=2023-12-20 00:00:00' \
# --header 'X-Nickname: kotlyarov-bar' \
# --header 'X-Cohort: 21' \
# --header 'X-Project: True' \
# --header 'X-API-KEY: 5f55e6c0-e9e5-4a9c-b313-63c01fc31460'


# Function to upload data to staging
def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    """
    Uploads data from an S3 file to a PostgreSQL table in a staging environment.

    Parameters:
    - filename (str): The name of the file to download from S3 and upload to PostgreSQL.
    - date (str): The date associated with the data, used in the local filename.
    - pg_table (str): The name of the PostgreSQL table to upload data.
    - pg_schema (str): The PostgreSQL schema where the table is located.
    - ti (TaskInstance): The task instance, providing access to XCom and other context.

    Returns:
    None

    This function retrieves an increment_id from XCom, constructs S3 and local filenames,
    downloads the file from S3, cleans data by removing duplicates and unwanted columns,
    creates a PostgresHook, gets the SQLAlchemy engine, and inserts the DataFrame into
    the specified PostgreSQL table.

    Example usage:
    upload_data_to_staging('my_file.csv', '2023-01-01', 'my_table', 'my_schema', task_instance)
    """

    # Clean or replace data in PostgreSQL table if it's in a predefined list
    if pg_table in ('user_order_log', 'user_activity_log'):
        clean_or_replace_data(pg_table, date, date)

    try:
        # Retrieve increment_id from XCom
        increment_id = ti.xcom_pull(key='increment_id')  # Pull increment_id from XCom

        # Construct S3 filename
        s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
        print(s3_filename)  # Log S3 filename

        # Construct local filename
        local_filename = date.replace('-', '') + '_' + filename
        print(local_filename)  # Log local filename

        # Make a GET request to the S3 filename
        response = requests.get(s3_filename)  # Send GET request to S3
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Write the content to a local file
        open(f"{local_filename}", "wb").write(response.content)  # Write content to a local file
        print(response.content)  # Log response content

        # Read the local CSV file into a pandas DataFrame
        df = pd.read_csv(local_filename)  # Read CSV file into DataFrame

        # Drop 'id' column and duplicates based on 'uniq_id'
        if 'id' in df.columns:
            df = df.drop('id', axis=1)  # Drop 'id' column
        if 'uniq_id' in df.columns:
            df = df.drop_duplicates(subset=['uniq_id'])  # Drop duplicate rows based on 'uniq_id'

        # Add 'status' column if it does not exist
        if filename == 'user_order_log_inc.csv':
            if 'status' not in df.columns:
                df['status'] = 'shipped'  # Add 'status' column with default value 'shipped'

        # Create a PostgresHook, get SQLAlchemy engine, and insert DataFrame into PostgreSQL table
        postgres_hook = PostgresHook(postgres_conn_id)  # Create a PostgresHook
        engine = postgres_hook.get_sqlalchemy_engine()  # Get SQLAlchemy engine
        row_count = df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)  # Insert DataFrame into PostgreSQL table
        print(f'{row_count} rows was inserted')  # Log the number of rows inserted

    except UniqueViolation:
        print('Record with uniq_id already exists. Skipping the task.')


def clean_or_replace_data(pg_table, start_date, end_date):
    """
    Cleans or replaces data in a PostgreSQL table within a specified date range.

    Parameters:
    - pg_table (str): The name of the PostgreSQL table to clean or replace data.
    - start_date (str): The start date of the date range for data removal.
    - end_date (str): The end date of the date range for data removal.

    Returns:
    None

    This function constructs a SQL script to delete data from the specified PostgreSQL table
    within the given date range. It uses the PostgresHook to connect to the database, gets the
    SQLAlchemy engine for executing SQL queries, establishes a connection to the database, and
    executes the SQL script to perform the data removal.

    Example usage:
    clean_or_replace_data('my_table', '2023-01-01', '2023-01-31')
    """
    sql_script = f"delete from staging.{pg_table} where date_time between '{start_date}' and '{end_date}';"

     # Create a PostgresHook to connect to the PostgreSQL database
    postgres_hook = PostgresHook(postgres_conn_id) # Create a PostgresHook
    engine = postgres_hook.get_sqlalchemy_engine() # Get SQLAlchemy engine
    with engine.connect() as connection:
        connection.execute(sql_script) # Execute sql_script


# Default arguments for the DAG
args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Airflow template variable {{ ds }} for the business date
business_dt = '{{ ds }}' 

# DAG named 'sales_mart'
with DAG(
        'sales_mart_and_customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=7),
        end_date=datetime.today() - timedelta(days=1),
        tags=['increment tables load', 'f_sales','f_customer_retention'],
) as dag:
    # Tasks using PythonOperator and PostgresOperator
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)  # PythonOperator to execute generate_report function

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)  # PythonOperator to execute get_report function

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})  # PythonOperator to execute get_increment function with additional keyword arguments

    upload_user_order_log_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_order_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'})  # PythonOperator to execute upload_data_to_staging function with additional keyword arguments

    upload_customer_research_inc = PythonOperator(
        task_id='upload_customer_research_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'customer_research_inc.csv',
                   'pg_table': 'customer_research',
                   'pg_schema': 'staging'})  # PythonOperator to execute upload_data_to_staging function with additional keyword arguments

    upload_user_activity_log_inc = PythonOperator(
        task_id='upload_user_activity_log',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_activity_log_inc.csv',
                   'pg_table': 'user_activity_log',
                   'pg_schema': 'staging'})  # PythonOperator to execute upload_data_to_staging function with additional keyword arguments

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")  # PostgreSQLOperator to execute SQL script for updating d_item table

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")  # PostgreSQLOperator to execute SQL script for updating d_customer table

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")  # PostgreSQLOperator to execute SQL script for updating d_city table

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}})  # PostgreSQLOperator to execute SQL script for updating f_sales table with parameters

    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql",
        parameters={"date": {business_dt}})  # PostgreSQLOperator to execute SQL script for updating f_customer_retention table with parameters

    # Task dependencies using bitshift
    generate_report >> get_report >> get_increment
    get_increment >> [upload_user_order_log_inc, upload_customer_research_inc, upload_user_activity_log_inc]
    upload_user_activity_log_inc >> update_d_city_table >> [update_d_item_table, update_d_customer_table]
    update_d_customer_table >> update_f_sales >> update_f_customer_retention

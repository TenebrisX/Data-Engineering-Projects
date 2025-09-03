from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import dag
import pendulum
import os
import boto3

def remove_files(directory):
    """Removes files within a specified directory.

    Args:
        directory: The path to the directory.
    """

    files = os.listdir(directory)
    for file in files:
        file_path = os.path.join(directory, file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Removed: {file_path}")
        except Exception as e:
            print(f"Error removing {file_path}: {e}")

def fetch_s3_file(bucket: str, key: str):
    """Fetches a file from an S3 bucket using Airflow Connections."""

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=None,
        aws_secret_access_key=None 
    )

    try: 
        conn = BaseHook.get_connection('aws_s3') 
        aws_access_key_id = conn.login
        aws_secret_access_key = conn.password
    except Exception as e:
        print(f"Error retrieving S3 credentials: {e}")

    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key, 
    )

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )

bash_command_tmpl = """
head /data/*.csv 
""" 

@dag(schedule_interval=None, 
     start_date=pendulum.parse('2022-07-13'),
     tags=['s3', 'csv', 'download'])
def dag_get_data():
    """Defines an Airflow DAG to download CSV files from S3."""  

    bucket_files = ['groups.csv', 'users.csv', 'dialogs.csv', 'group_log.csv']

    remove_existing_files = PythonOperator(
        task_id='remove_existing_files',
        python_callable=remove_files,
        op_kwargs={'directory': '/data/'}
    )

    fetch_csv_tasks = []
    for f in bucket_files:
        fetch_csv = PythonOperator(
            task_id=f'fetch_{f}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': f}
        )
        fetch_csv_tasks.append(fetch_csv)

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,  
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )

    remove_existing_files >> fetch_csv_tasks >> print_10_lines_of_each

download_dag = dag_get_data()

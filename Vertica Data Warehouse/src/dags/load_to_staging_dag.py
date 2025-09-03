"""Airflow DAG for CSV to Vertica ETL pipeline.

This module contains an Airflow DAG that loads CSV data files into Vertica database tables
using the COPY command for efficient bulk data loading. The DAG handles multiple tables
including users, groups, dialogs, and group_log.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import vertica_python

vertica_conn_id = "vertica_con" 
vertica_conn = BaseHook.get_connection(vertica_conn_id) 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 3)
}

dag = DAG(
    'csv_to_vertica_etl',
    default_args=default_args,
    description='ETL pipeline to load CSV data into Vertica',
    schedule_interval=None,
    tags=['csv', 'vertica', 'staging', 'load']
)


def load_csv_to_vertica(table_name, csv_file):
    """Load CSV data into a specified Vertica table using bulk COPY operation.

    This function connects to Vertica, truncates the target table, and loads CSV data
    using the COPY command for efficient bulk loading. Rejected records are stored
    in a separate rejection table for error analysis.

    Args:
        table_name (str): The name of the target Vertica table.
        csv_file (str): The path to the CSV file to be loaded.

    Raises:
        vertica_python.Error: If database connection or operation fails.
        FileNotFoundError: If the specified CSV file doesn't exist.
    """
    conn_info = {
        'host': vertica_conn.host,
        'port': vertica_conn.port,
        'user': vertica_conn.login,
        'password': vertica_conn.password,
        'database': vertica_conn.schema
    }

    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()

    copy_command = f"""
    TRUNCATE TABLE STV2024021962__STAGING.{table_name}; 

    COPY STV2024021962__STAGING.{table_name}
    FROM LOCAL '{csv_file}'
    DELIMITER ',' 
    ENCLOSED BY '\"' 
    NULL AS ''
    TRAILING NULLCOLS
    REJECTED DATA AS TABLE STV2024021962__STAGING.{table_name}_rejected;
    """

    cur.execute(copy_command)

    connection.commit()
    connection.close()


tables = ['users', 'groups', 'dialogs', 'group_log']

for table in tables:
    task = PythonOperator(
        task_id=f'load_{table}_to_vertica',
        python_callable=load_csv_to_vertica,
        op_args=[table, f'/data/{table}.csv'],
        dag=dag,
    )

if __name__ == "__main__":
    dag.cli()
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import vertica_python

# Connection Configuration
vertica_conn_id = "vertica_con"  # Airflow Connection ID for Vertica
vertica_conn = BaseHook.get_connection(vertica_conn_id)  # Retrieve connection info

# Default DAG Arguments
default_args = {
    'owner': 'airflow',  # DAG ownership
    'depends_on_past': False,  # Task runs independently of past runs
    'start_date': datetime(2020, 9, 3)  # Initial start date
}

# DAG Initialization
dag = DAG(
    'csv_to_vertica_etl',
    default_args=default_args,
    description='ETL pipeline to load CSV data into Vertica',
    schedule_interval=None,  # DAG will not run on a schedule
    tags=['csv', 'vertica', 'staging', 'load']  # Descriptive tags for the DAG
)

# Core ETL function
def load_csv_to_vertica(table_name, csv_file):
    """Loads CSV data into a specified Vertica table.

    Args:
        table_name: The name of the target Vertica table.
        csv_file: The path to the CSV file to be loaded.
    """

    # Retrieve Vertica connection details from Airflow
    conn_info = {
        'host': vertica_conn.host,
        'port': vertica_conn.port,
        'user': vertica_conn.login,
        'password': vertica_conn.password,
        'database': vertica_conn.schema
    }

    # Connect to Vertica 
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()

    # Construct Vertica COPY command for efficient loading
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

    # Execute the load process
    cur.execute(copy_command)

    # Finalize changes and cleanup
    connection.commit()
    connection.close()

# Task creation loop:
tables = ['users', 'groups', 'dialogs', 'group_log']

for table in tables:
    task = PythonOperator(
        task_id=f'load_{table}_to_vertica',
        python_callable=load_csv_to_vertica,
        op_args=[table, f'/data/{table}.csv'],  # Pass table name and CSV path
        dag=dag,
    )

# Entry point (for direct execution)
if __name__ == "__main__":
    dag.cli()

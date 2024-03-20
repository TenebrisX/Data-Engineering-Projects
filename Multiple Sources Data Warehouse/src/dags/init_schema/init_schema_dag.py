import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from init_schema.schema_init import SchemaDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Setting the DAG execution schedule - every 15 minutes.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # DAG start date. Can be set to today.
    catchup=False,  # Should the DAG run for previous periods (from start_date to today) - False (no).
    tags=['cdm', 'dds', 'stg', 'schema', 'ddl'],  # Tags used for filtering in the Airflow UI.
    is_paused_upon_creation=True  # Paused/running upon creation. Immediately running.
)
def init_schema_dag():
    # Establish connection to the DWH database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Retrieve the path to the directory with SQL files from Airflow variables.
    ddl_path = Variable.get("SQL_DDL_FILES_PATH")

    # Declare a task that creates the table structure.
    @task(task_id="stg_schema_init")
    def stg_schema_init():
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/stg")

    @task(task_id="dds_schema_init")
    def dds_schema_init():
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/dds")

    @task(task_id="cdm_schema_init")
    def cdm_schema_init():
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/cdm")

    # Initialize the declared tasks.
    stg_init_schema = stg_schema_init()
    dds_init_schema = dds_schema_init()
    cdm_init_schema = cdm_schema_init()

    # Set the execution sequence of tasks. We only have schema initialization.
    [stg_init_schema, dds_init_schema, cdm_init_schema] # type: ignore

# Call the function describing the DAG.
init_schema = init_schema_dag()  # noqa

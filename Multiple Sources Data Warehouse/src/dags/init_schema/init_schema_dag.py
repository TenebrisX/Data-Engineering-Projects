"""Airflow DAG for database schema initialization.

This module contains an Airflow DAG that initializes the schema of the Data Warehouse (DWH)
database by creating necessary tables and structures for STG, DDS, and CDM schemas.
"""

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from init_schema.schema_init import SchemaDdl
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['cdm', 'dds', 'stg', 'schema', 'ddl'],
    is_paused_upon_creation=True
)
def init_schema_dag():
    """Initialize database schema for STG, DDS, and CDM layers."""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    ddl_path = Variable.get("SQL_DDL_FILES_PATH")

    @task(task_id="stg_schema_init")
    def stg_schema_init():
        """Initialize STG (staging) schema tables."""
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/stg")

    @task(task_id="dds_schema_init")
    def dds_schema_init():
        """Initialize DDS (Data Distribution Service) schema tables."""
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/dds")

    @task(task_id="cdm_schema_init")
    def cdm_schema_init():
        """Initialize CDM (Common Data Model) schema tables."""
        schema_loader = SchemaDdl(dwh_pg_connect, log)
        schema_loader.init_schema(ddl_path + "/cdm")

    stg_init_schema = stg_schema_init()
    dds_init_schema = dds_schema_init()
    cdm_init_schema = cdm_schema_init()

    [stg_init_schema, dds_init_schema, cdm_init_schema]

init_schema = init_schema_dag()
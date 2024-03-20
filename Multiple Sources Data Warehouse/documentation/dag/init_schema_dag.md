# Initialization Schema DAG

The `init_schema_dag` is an Airflow Directed Acyclic Graph (DAG) designed to initialize the schema of the Data Warehouse (DWH) database. It consists of tasks that create the necessary tables and structures for the Staging (STG), Data Distribution Service (DDS), and Common Data Model (CDM) schemas.

## DAG Configuration

- **Schedule Interval:** Every 15 minutes
- **Start Date:** May 5, 2022, UTC
- **Catchup:** False (Do not run for previous periods)
- **Tags:** cdm, dds, stg, schema, ddl
- **Paused Upon Creation:** True (Initially paused)

## Tasks

### stg_schema_init

This task initializes the Staging (STG) schema by executing SQL DDL scripts located in the specified path.

### dds_schema_init

This task initializes the Data Distribution Service (DDS) schema by executing SQL DDL scripts located in the specified path.

### cdm_schema_init

This task initializes the Common Data Model (CDM) schema by executing SQL DDL scripts located in the specified path.

## Execution

The tasks are executed sequentially, initializing the STG, DDS, and CDM schemas in the specified order.

## Dependencies

None


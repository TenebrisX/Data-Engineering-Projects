# Airflow DAG Documentation: Bonus System DAG

## Overview

This document provides detailed information about the Bonus System DAG, which is responsible for loading data related to ranks, users, and events from the bonus system's origin database to the data warehouse (DWH) database.

## DAG Structure

The DAG consists of tasks for loading ranks, users, and events. These tasks are executed at regular intervals to keep the data in the DWH up-to-date.

### DAG Schedule

The DAG is scheduled to run every 15 minutes (`schedule_interval='0/15 * * * *'`).

### Start Date

The DAG starts its execution on May 5, 2022, in Coordinated Universal Time (UTC) (`start_date=pendulum.datetime(2022, 5, 5, tz="UTC")`).

### Catchup

The DAG does not catch up for previous periods (`catchup=False`).

### Tags

The DAG is tagged with 'stg', 'origin', and 'bonus_system' for easy filtering in the Airflow UI (`tags=['stg', 'origin', 'bonus_system']`).

### Paused Upon Creation

The DAG is initially paused upon creation (`is_paused_upon_creation=True`).

## Tasks

### Task: Load Ranks (`ranks_load`)

This task loads ranks data from the origin bonus system database to the DWH.

### Task: Load Users (`users_load`)

This task loads users data from the origin bonus system database to the DWH.

### Task: Load Events (`events_load`)

This task loads events data from the origin bonus system database to the DWH.

## Dependencies

- The "Load Ranks" and "Load Users" tasks must be completed before the "Load Events" task can run (`[ranks_dict, users_dict] >> events_dict`).

## Connections

### Data Warehouse (DWH) Connection

The DAG uses a PostgreSQL connection named "PG_WAREHOUSE_CONNECTION" to connect to the data warehouse.

### Origin Bonus System Connection

The DAG uses a PostgreSQL connection named "PG_ORIGIN_BONUS_SYSTEM_CONNECTION" to connect to the bonus system's origin database.

## Logging

Logging is configured using the Python `logging` module.

## Execution

To execute the DAG, ensure that the necessary PostgreSQL connections are configured in Airflow and the DAG is unpaused.

---

**Note:** Customize this documentation further based on specific details, dependencies, or additional configurations related to your environment and use case.

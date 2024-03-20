# Airflow DAG for Data Loading

## Overview

This Airflow DAG (`cdm_dag`) is designed for loading data from the Data Warehouse (DWH) into the CDM (Central Data Mart) using a series of tasks. The DAG is scheduled to run every 15 minutes and performs two main tasks: loading settlement report data and loading courier ledger data.

## DAG Configuration

- **Schedule Interval:** Every 15 minutes
- **Start Date:** May 5, 2022, in UTC
- **Catchup:** Disabled
- **Tags:** ['dds', 'cdm']
- **Paused Upon Creation:** True

## Tasks

### 1. Load Settlement Report Data

- **Task ID:** `dm_settlement_report_load`
- **Task Function:** `load_dm_settlement_report`
- **Description:** Loads settlement report data from the DWH to the CDM using the `SettlementReportLoader` class.
- **Dependencies:** None

### 2. Load Courier Ledger Data

- **Task ID:** `dm_courier_ledger_load`
- **Task Function:** `load_dm_courier_ledger`
- **Description:** Loads courier ledger data from the DWH to the CDM using the `CourierLedgerLoader` class.
- **Dependencies:** None

## Workflow Logic

1. Create a connection to the DWH database (`PG_WAREHOUSE_CONNECTION`).
2. Define tasks for loading settlement report and courier ledger data.
3. Initialize the defined tasks and specify the execution sequence.
4. Instantiate the DAG (`cdm`).

## Workflow Execution

1. The DAG runs every 15 minutes.
2. Tasks are executed sequentially: first, the settlement report data is loaded, followed by the courier ledger data.
3. Each task uses its respective repository and loader classes for data extraction, transformation, and loading.
4. Workflow settings are updated after each execution, tracking the last loaded date or ID.

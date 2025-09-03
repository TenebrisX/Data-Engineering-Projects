"""Airflow DAG for CDM layer data loading.

This module contains an Airflow DAG responsible for loading data from the Data Warehouse (DWH)
into the CDM (Common Data Model) layer, including settlement reports and courier ledger data.
"""

import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from cdm.dm_settlement_report_loader import SettlementReportLoader
from cdm.dm_courier_ledger_loader import CourierLedgerLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds', 'cdm'],
    is_paused_upon_creation=True
)
def cdm_dag():
    """CDM DAG for loading settlement reports and courier ledger data."""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_settlement_report_load")
    def load_dm_settlement_report():
        """Load settlement report data from DDS to CDM layer."""
        settlement_loader = SettlementReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        settlement_loader.load_reports()

    @task(task_id="dm_courier_ledger_load")
    def load_dm_courier_ledger():
        """Load courier ledger data from DDS to CDM layer."""
        courier_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        courier_loader.load_couriers()

    dm_settlement_report_task = load_dm_settlement_report()
    dm_courier_ledger_task = load_dm_courier_ledger()

    [dm_settlement_report_task, dm_courier_ledger_task]

cdm = cdm_dag()
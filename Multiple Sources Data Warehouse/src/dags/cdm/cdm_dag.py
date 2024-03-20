import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from cdm.dm_settlement_report_loader import SettlementReportLoader
from cdm.dm_courier_ledger_loader import CourierLedgerLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Schedule the DAG to run every 15 minutes.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Set the start date for DAG execution (can be set to today).
    catchup=False,  # Do not run the DAG for previous periods (from start_date to today).
    tags=['dds', 'cdm'],  # Tags used for filtering in the Airflow interface.
    is_paused_upon_creation=True  # The DAG is initially paused upon creation.
)
def cdm_dag():
    # Create a connection to the DWH database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Define a task to load settlement report data.
    @task(task_id="dm_settlement_report_load")
    def load_dm_settlement_report():
        # Create an instance of the SettlementReportLoader class to encapsulate the logic.
        settlement_loader = SettlementReportLoader(dwh_pg_connect, dwh_pg_connect, log)
        settlement_loader.load_reports()  # Invoke the function to transfer data.

    # Define a task to load courier ledger data.
    @task(task_id="dm_courier_ledger_load")
    def load_dm_courier_ledger():
        # Create an instance of the CourierLedgerLoader class to encapsulate the logic.
        courier_loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        courier_loader.load_couriers()  # Invoke the function to transfer data.

    # Initialize the defined tasks.
    dm_settlement_report_task = load_dm_settlement_report()
    dm_courier_ledger_task = load_dm_courier_ledger()

    # Specify the execution sequence of tasks.
    [dm_settlement_report_task, dm_courier_ledger_task]

# Instantiate the DAG
cdm = cdm_dag()

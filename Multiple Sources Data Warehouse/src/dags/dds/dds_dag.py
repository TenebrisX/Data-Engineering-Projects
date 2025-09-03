"""Airflow DAG for DDS layer data loading.

This module contains an Airflow DAG responsible for loading data from staging (STG) layer
to the Data Distribution Service (DDS) layer, including dimension and fact tables.
"""

import logging
import pendulum
from airflow.decorators import dag, task
from dds.dm_users_loader import DMUserLoader
from dds.dm_restaurants_loader import DMRestaurantLoader
from dds.dm_timestamps_loader import DMTimestampLoader
from dds.dm_products_loader import DMProductsLoader
from dds.dm_orders_loader import DMOrdersLoader
from dds.c_couriers_loader import CCurierLoader
from dds.c_deliveries_loader import CDeliveriesLoader
from dds.fct_product_sales_loader import FCTLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['stg', 'dds'],
    is_paused_upon_creation=True
)
def dds_dag():
    """DDS DAG for loading dimension and fact tables from staging layer."""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_dm_users():
        """Load users dimension table from staging to DDS."""
        rest_loader = DMUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        """Load restaurants dimension table from staging to DDS."""
        rest_loader = DMRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="c_couriers_load")
    def load_c_couriers():
        """Load couriers dimension table from staging to DDS."""
        rest_loader = CCurierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        """Load timestamps dimension table from staging to DDS."""
        rest_loader = DMTimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id="c_deliveries_load")
    def load_c_deliveries():
        """Load deliveries table from staging to DDS."""
        rest_loader = CDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()

    @task(task_id="dm_products_load")
    def load_dm_products():
        """Load products dimension table from staging to DDS."""
        rest_loader = DMProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        """Load orders dimension table from staging to DDS."""
        rest_loader = DMOrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="fct_product_sales_load")
    def load_fct():
        """Load product sales fact table from staging to DDS."""
        rest_loader = FCTLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_facts()

    users_dict = load_dm_users()
    restaurants_dict = load_dm_restaurants()
    couriers_dict = load_c_couriers()
    timestamps_dict = load_dm_timestamps()
    deliveries_dict = load_c_deliveries()
    products_dict = load_dm_products()
    orders_dict = load_dm_orders()
    fct_dict = load_fct()

    [users_dict, restaurants_dict, couriers_dict, timestamps_dict] >> deliveries_dict >> products_dict >> orders_dict >> fct_dict

dds = dds_dag()
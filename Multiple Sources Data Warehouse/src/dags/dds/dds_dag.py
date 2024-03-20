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
    schedule_interval='0/15 * * * *',  # Schedule the DAG to run every 15 minutes.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Specify the start date of the DAG.
    catchup=False,  # Do not run DAG for previous periods on startup.
    tags=['stg', 'dds'],  # Tags used for filtering in the Airflow interface.
    is_paused_upon_creation=True  # Pause the DAG upon creation.
)
def dds_dag():
    # Create a connection to the data warehouse.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Define tasks for loading different data entities.

    @task(task_id="dm_users_load")
    def load_dm_users():
        """Task to load DM Users data."""
        rest_loader = DMUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        """Task to load DM Restaurants data."""
        rest_loader = DMRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="c_couriers_load")
    def load_c_couriers():
        """Task to load C Couriers data."""
        rest_loader = CCurierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_couriers()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        """Task to load DM Timestamps data."""
        rest_loader = DMTimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_timestamps()

    @task(task_id="c_deliveries_load")
    def load_c_deliveries():
        """Task to load C Deliveries data."""
        rest_loader = CDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()

    @task(task_id="dm_products_load")
    def load_dm_products():
        """Task to load DM Products data."""
        rest_loader = DMProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        """Task to load DM Orders data."""
        rest_loader = DMOrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="fct_product_sales_load")
    def load_fct():
        """Task to load FCT Product Sales data."""
        rest_loader = FCTLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_facts()

    # Initialize the declared tasks.
    users_dict = load_dm_users()
    restaurants_dict = load_dm_restaurants()
    couriers_dict = load_c_couriers()
    timestamps_dict = load_dm_timestamps()
    deliveries_dict = load_c_deliveries()
    products_dict = load_dm_products()
    orders_dict = load_dm_orders()
    fct_dict = load_fct()

    # Define the execution sequence of tasks.
    [users_dict, restaurants_dict, couriers_dict, timestamps_dict] >> deliveries_dict >> products_dict >> orders_dict >> fct_dict  # type: ignore

# Create an instance of the DAG.
dds = dds_dag()

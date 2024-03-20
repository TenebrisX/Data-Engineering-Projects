from airflow.decorators import dag, task
import logging
import pendulum

from lib import ConnectionBuilder

from stg.delivery_system_dag.loaders.couriers_loader import CourierLoader
from stg.delivery_system_dag.loaders.restaurants_loader import RestaurantLoader
from stg.delivery_system_dag.loaders.delivery_loader import DeliveryLoader
from stg.delivery_system_dag.readers.couriers_reader import CourierApiReader
from stg.delivery_system_dag.readers.restaurant_reader import RestaurantApiReader
from stg.delivery_system_dag.readers.deliveries_reader import DeliveryApiReader
from stg.delivery_system_dag.loaders.dest.couriesrs_dest import CourierDest
from stg.delivery_system_dag.loaders.dest.restaurants_dest import RestaurantDest
from stg.delivery_system_dag.loaders.dest.deliveries_dest import DeliveryDest

logger = logging.getLogger(__name__)

# Configuration (replace with your actual values)
dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

@dag(
    schedule_interval='0/15 * * * *',  # Set the schedule for every 15 minutes
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Set the start date
    catchup=False,  # Do not catch up for previous periods
    tags=['api', 'stg', 'origin'],  # Tags for filtering in Airflow UI
    is_paused_upon_creation=True  # Pause upon creation
)
def stg_delivery_system():

    # Connect to database and API
    couriers_api_client = CourierApiReader(logger)
    restaurants_api_client = RestaurantApiReader(logger)
    deliveries_api_client = DeliveryApiReader(logger)
    
    
    courier_dest = CourierDest(dwh_pg_connect)
    restaurant_dest = RestaurantDest(dwh_pg_connect)
    delivery_dest = DeliveryDest(dwh_pg_connect)
    

    @task()
    def load_couriers():
        """
        Loads courier data from the API to the staging database.
        """
        loader = CourierLoader(
            dwh_pg_connect, couriers_api_client, courier_dest, logger
        )
        loader.load_couriers()

    @task()
    def load_restaurants():
        """
        Loads restaurant data from the API to the staging database.
        """
        loader = RestaurantLoader(
            dwh_pg_connect, restaurants_api_client, restaurant_dest, logger
        )
        loader.load_restaurants()

    @task()
    def load_deliveries():
        """
        Loads delivery data from the API to the staging database.
        """
        loader = DeliveryLoader(
            dwh_pg_connect, deliveries_api_client, delivery_dest, logger
        )
        loader.load_deliveries()

    couriers_loader = load_couriers()
    restaurants_loader = load_restaurants()
    deliveries_loader = load_deliveries()
    
    [couriers_loader, restaurants_loader, deliveries_loader]

delivery_system_dag = stg_delivery_system()

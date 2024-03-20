import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from lib.pg_saver import PgSaver
from stg.order_system_dag.loaders.restaurant_loader import RestaurantLoader
from stg.order_system_dag.readers.restaurant_reader import RestaurantReader
from stg.order_system_dag.readers.users_reader import UsersReader
from stg.order_system_dag.loaders.users_loader import UsersLoader
from stg.order_system_dag.readers.orders_reader import OrdersReader
from stg.order_system_dag.loaders.orders_loader import OrdersLoader
from lib import ConnectionBuilder, MongoConnect

# Configure logger
log = logging.getLogger(__name__)

# Define Airflow DAG
@dag(
    schedule_interval='0/15 * * * *',  # Set the schedule for every 15 minutes
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Set the start date
    catchup=False,  # Do not catch up for previous periods
    tags=['mongo', 'stg', 'origin'],  # Tags for filtering in Airflow UI
    is_paused_upon_creation=True  # Pause upon creation
)
def stg_order_system():
    # Create a connection to the data warehouse (dwh)
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Get MongoDB connection details from Airflow variables
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    # Initialize PgSaver for saving data to PostgreSQL
    pg_saver = PgSaver()
    
    # Initialize MongoDB connection
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

    # Define tasks for loading restaurants, users, and orders
    @task(task_id="load_restaurants")
    def load_restaurants():
        # Initialize RestaurantReader for reading data from MongoDB
        collection_reader = RestaurantReader(mongo_connect)
        # Initialize RestaurantLoader for loading data to PostgreSQL
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        # Run the data copy process
        loader.run_copy()

    @task(task_id="load_users")
    def load_users():
        # Initialize UsersReader for reading data from MongoDB
        collection_reader = UsersReader(mongo_connect)
        # Initialize UsersLoader for loading data to PostgreSQL
        loader = UsersLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        # Run the data copy process
        loader.run_copy()

    @task(task_id="load_orders")
    def load_orders():
        # Initialize OrdersReader for reading data from MongoDB
        collection_reader = OrdersReader(mongo_connect)
        # Initialize OrdersLoader for loading data to PostgreSQL
        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        # Run the data copy process
        loader.run_copy()

    # Define dependencies between tasks
    restaurant_loader = load_restaurants()
    users_loader = load_users()
    orders_loader = load_orders()

    [restaurant_loader, users_loader] >> orders_loader  # type: ignore

# Instantiate the DAG
order_stg_dag = stg_order_system()  # noqa

"""Airflow DAG for order system data extraction and loading.

This module contains an Airflow DAG that extracts data from MongoDB collections
related to orders, restaurants, and users, and loads it into PostgreSQL staging tables.
"""

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

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['mongo', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def stg_order_system():
    """STG DAG for extracting order system data from MongoDB to PostgreSQL staging."""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    pg_saver = PgSaver()
    
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

    @task(task_id="load_restaurants")
    def load_restaurants():
        """Load restaurant data from MongoDB to PostgreSQL staging."""
        collection_reader = RestaurantReader(mongo_connect)
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task(task_id="load_users")
    def load_users():
        """Load user data from MongoDB to PostgreSQL staging."""
        collection_reader = UsersReader(mongo_connect)
        loader = UsersLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    @task(task_id="load_orders")
    def load_orders():
        """Load order data from MongoDB to PostgreSQL staging."""
        collection_reader = OrdersReader(mongo_connect)
        loader = OrdersLoader(collection_reader, dwh_pg_connect, pg_saver, log)
        loader.run_copy()

    restaurant_loader = load_restaurants()
    users_loader = load_users()
    orders_loader = load_orders()

    [restaurant_loader, users_loader] >> orders_loader

order_stg_dag = stg_order_system()
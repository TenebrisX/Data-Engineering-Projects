"""Airflow DAG for bonus system data loading.

This module contains an Airflow DAG responsible for loading data from the bonus system
origin database to the data warehouse staging layer, including ranks, users, and events.
"""

import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_dag.loaders.ranks_loader import RankLoader
from stg.bonus_system_dag.loaders.users_loader import UserLoader
from stg.bonus_system_dag.loaders.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False, 
    tags=['stg', 'origin', 'bonus_system'],  
    is_paused_upon_creation=True 
)
def stg_bonus_system_dag():
    """STG DAG for loading bonus system data from origin to staging."""
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        """Load ranks data from bonus system origin to staging."""
        ranks_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        ranks_loader.load() 

    @task(task_id="users_load")
    def load_users():
        """Load users data from bonus system origin to staging."""
        users_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        users_loader.load() 

    @task(task_id="events_load")
    def load_events():
        """Load events data from bonus system origin to staging."""
        events_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        events_loader.load() 

    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    [ranks_dict, users_dict] >> events_dict

stg_bonus_system_dag = stg_bonus_system_dag()
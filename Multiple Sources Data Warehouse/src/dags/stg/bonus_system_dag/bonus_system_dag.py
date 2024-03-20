import logging

import pendulum
from airflow.decorators import dag, task
from stg.bonus_system_dag.loaders.ranks_loader import RankLoader
from stg.bonus_system_dag.loaders.users_loader import UserLoader
from stg.bonus_system_dag.loaders.events_loader import EventLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

# Define the Airflow DAG
@dag(
    schedule_interval='0/15 * * * *',  # Set the DAG schedule - every 15 minutes.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Start date of DAG execution.
    catchup=False,  # Do not run the DAG for previous periods (from start_date to today).
    tags=['stg', 'origin', 'bonus_system'],  # Tags used for filtering in the Airflow interface.
    is_paused_upon_creation=True  # The DAG is initially paused.
)
def stg_bonus_system_dag():
    # Create a connection to the DWH database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Create a connection to the bonus system subsystem database.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Declare a task to load ranks data.
    @task(task_id="ranks_load")
    def load_ranks():
        # Create an instance of the RankLoader class that implements the logic.
        ranks_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        ranks_loader.load()  # Invoke the function that transfers data.

    # Declare a task to load users data.
    @task(task_id="users_load")
    def load_users():
        # Create an instance of the UserLoader class that implements the logic.
        users_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        users_loader.load()  # Invoke the function that transfers data.

    # Declare a task to load events data.
    @task(task_id="events_load")
    def load_events():
        # Create an instance of the EventLoader class that implements the logic.
        events_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        events_loader.load()  # Invoke the function that transfers data.

    # Initialize the declared tasks.
    ranks_dict = load_ranks()
    users_dict = load_users()
    events_dict = load_events()

    # Set the execution sequence of tasks.
    [ranks_dict, users_dict] >> events_dict

# Instantiate the Airflow DAG
stg_bonus_system_dag = stg_bonus_system_dag()

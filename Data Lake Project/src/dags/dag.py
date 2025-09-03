import os
from datetime import datetime

import airflow
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3'
os.environ['PYTHONPATH'] = os.pathsep.join([os.environ['PYTHONPATH'], '/lessons/']) 

imput_events_path = "/user/kotlyarovb/sm_data_lake_project/staging/events"
input_geo_path = "/user/kotlyarovb/sm_data_lake_project/staging/geo"
output_path = "/user/kotlyarovb/sm_data_lake_project/analytics"

@dag(
    dag_id="spark",
    default_args={
        'owner': 'airflow',
        'start_date': datetime.today(),
    },
    schedule_interval='@daily',
    tags=['spark', 'hdfs', 'dm', 'sm_data_lake_project'],
)

def create_datamarts_dag():

    @task(task_id='dm_users')
    def create_user_datamart():
        spark_submit_dm_users = SparkSubmitOperator(
            task_id='dm_users',
            application='/lessons/scripts/main.py',
            conn_id='yarn_spark',
            application_args=[imput_events_path, input_geo_path, f"{output_path}/dm_users", 'user'],
            conf={"spark.driver.maxResultSize": "20g"},
            executor_cores=2,
            executor_memory='2g'
        )
        spark_submit_dm_users.execute(context={}) 
        return 'spark_submit_dm_users_complete'

    @task(task_id='dm_zones')
    def create_zones_datamart():
        spark_submit_dm_zones = SparkSubmitOperator(
            task_id='dm_zones',
            application='/lessons/scripts/main.py',
            conn_id='yarn_spark',
            application_args=[imput_events_path, input_geo_path, f"{output_path}/dm_zones", 'zones'],
            conf={"spark.driver.maxResultSize": "20g"},
            executor_cores=2,
            executor_memory='2g'
        )
        spark_submit_dm_zones.execute(context={}) 
        return 'spark_submit_dm_zones_complete'

    @task(task_id='dm_friends_recomendation')
    def create_friend_recommendations():
        spark_submit_dm_firends = SparkSubmitOperator(
            task_id='dm_friends_recomendation',
            application='/lessons/scripts/main.py',
            conn_id='yarn_spark',
            application_args=[imput_events_path, input_geo_path, f"{output_path}/dm_friends_recomendation", 'friends'],
            conf={"spark.driver.maxResultSize": "20g"},
            executor_cores=2,
            executor_memory='2g'
        )
        spark_submit_dm_firends.execute(context={}) 
        return 'spark_submit_dm_firends_complete'

    create_user_datamart() >> create_zones_datamart() >> create_friend_recommendations()

dag = create_datamarts_dag()
import sys
import os
import logging

from pyspark.sql import SparkSession

from scripts.utils.data_processor import DataLoader, DataWriter
from scripts.utils.geo_processor import GeoProcessor
from scripts.analytics_engine import AnalyticsEngine
from scripts.user_geo_analyzer import find_nearest_city, determine_actual_city, determine_home_city, calculate_travel, calculate_local_time
from scripts.firend_recommender import FriendRecommender
# Environment Setup (if needed)
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3'

# Setup Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

def create_user_datamart(events_path, cities_path, output_path):
    """
    Creates a user datamart from events and geo data.

    Args:
        events_path (str): Path to the events data.
        cities_path (str): Path to the geo data.
        output_path (str): Path to store the resulting datamart.
    """
    logger.info("Starting user datamart creation")

    try: 
        spark = SparkSession.builder.appName('datamart_users').getOrCreate()
        loader = DataLoader(spark)
    
        # Load datasets
        events_df = loader.load_events_users(events_path)
        cities_df = loader.load_cities(cities_path)
        
        # Broadcast the cities_df for improved join performance
        cities_df = cities_df.hint("broadcast")

        events_geo_df = find_nearest_city(events_df, cities_df)
        # Cache events_geo_df for reuse
        events_geo_df.persist()
        
        actual_city_df = determine_actual_city(events_geo_df)
        home_city_df = determine_home_city(events_geo_df)
        travel_city_df = calculate_travel(events_geo_df)
        time_info_df = calculate_local_time(events_geo_df)

        mart_users_df = events_geo_df \
            .select('user_id') \
            .distinct() \
            .join(actual_city_df, 'user_id', 'left') \
            .join(home_city_df, 'user_id', 'left') \
            .join(travel_city_df, 'user_id', 'left') \
            .join(time_info_df, 'user_id', 'left') \
            .persist()
        
        writer = DataWriter()
        writer.write_data_parquet(mart_users_df, output_path)
        
        logger.info("User datamart creation completed successfully")
    except Exception as e:
        logger.error(f"User datamart creation failed: {e}")


def create_zones_datamart(events_path, cities_path, output_path):
    """
    Creates a zone-level datamart from events and geo data.

    Args:
        events_path (str): Path to the events data.
        cities_path (str): Path to the geo data.
        output_path (str): Path to store the resulting datamart.
    """
    logger.info("Starting zones datamart creation")

    try:
        spark = SparkSession.builder.appName('datamart_zones').getOrCreate()
        loader = DataLoader(spark)
        cities_df = loader.load_cities(cities_path)
        events_df = loader.load_events_zones(events_path)

        geo_processor = GeoProcessor(events_df, cities_df)
        events_geo_df = geo_processor.find_nearest_zone()

        analytics = AnalyticsEngine(events_geo_df)
        mart_zones_df = analytics.calculate_mart_zones()

        writer = DataWriter()
        writer.write_data_parquet(mart_zones_df, output_path)

        logger.info("Zones datamart creation completed successfully")

    except Exception as e:
        logger.error(f"Zones datamart creation failed: {e}")
        

def create_friend_recommendations(events_path, cities_path, output_path):
    """Creates friend recommendations based on shared subscriptions and proximity."""
    logger.info("Starting friend recommendations generation")

    try:
        spark = SparkSession.builder.appName('datamart_friends').getOrCreate()
        loader = DataLoader(spark)
        cities_df = loader.load_cities(cities_path)
        events_df = loader.load_events_firends_suggestions(events_path)

        geo = GeoProcessor(events_df, cities_df)
        user_zone_df = geo.determine_user_zone(events_df.where("event_type = 'message'"))

        recommender = FriendRecommender(events_df, user_zone_df, cities_df)
        recommendations_df = recommender.calculate_recommendations()

        writer = DataWriter()
        writer.write_data_parquet(recommendations_df, output_path)

        logger.info("Friend recommendations generation completed successfully")

    except Exception as e:
        logger.error(f"Friend recommendations generation failed: {e}")


if __name__ == "__main__":
    # Command-Line Argument Handling
    if len(sys.argv) != 5:
        print("Usage: main.py <events_data_path> <cities_data_path> <output_path> <datamart_type>", file=sys.stderr)
        sys.exit(-1)

    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    output_path = sys.argv[3]
    datamart_type = sys.argv[4]

    if datamart_type == 'user':
        create_user_datamart(events_path, cities_path, output_path)
    elif datamart_type == 'zones':
        create_zones_datamart(events_path, cities_path, output_path)
    elif datamart_type == 'friends':
        create_friend_recommendations(events_path, cities_path, output_path)
    else:
        logger.error("Invalid datamart type. Choose from 'user', 'zones', or 'friends'")
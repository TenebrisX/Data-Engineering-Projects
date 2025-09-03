import logging

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from scripts.utils.distance_calculator import calculate_distance

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)


class GeoProcessor:
    """ Processes event data with geographic information."""

    def __init__(self, events_df, cities_df):
        self.events_df = events_df
        self.cities_df = cities_df

    def find_nearest_zone(self):
        """Determines the nearest zone (city) for each event.

        Returns:
            pyspark.sql.DataFrame: DataFrame with geographic event data.
        """
        
        logger.info("Starting nearest zone calculation")

        events_geo_df = self.events_df.crossJoin(self.cities_df) \
            .withColumn('distance', calculate_distance(
                F.col('msg_lat_rad'), F.col('msg_lng_rad'),
                F.col('lat_rad'), F.col('lng_rad'))) \
            .selectExpr('date', 'user', 'event_type', 'message_from as user_id', 'id as zone_id', 'distance', 'message_id') \
            .withColumn('row_number', F.row_number().over(Window.partitionBy('message_id').orderBy('distance'))) \
            .filter(F.col('row_number') == 1) \
            .persist()

        logger.info("Nearest zones calculated successfully")
        return events_geo_df
    
    def determine_user_zone(self, event_df):
        """Determines the nearest zone (city) for each user's last message.

        Args:
            event_df (pyspark.sql.DataFrame): DataFrame with event data.

        Returns:
            pyspark.sql.DataFrame: DataFrame with user and their last known zone.
        """

        window = Window.partitionBy('user_msg_from').orderBy(F.col('date').desc(), F.col('message_id').desc())

        user_zone_df = event_df \
            .withColumn('lat_rad', F.radians(F.col('lat'))) \
            .withColumn('lng_rad', F.radians(F.col('lon'))) \
            .withColumn('rn', F.row_number().over(window)) \
            .where('rn = 1') \
            .join(self.cities_df, ['lat_rad', 'lng_rad'], how='left') \
            .drop('lat', 'lon', 'rn') \
            .withColumnRenamed('id', 'zone_id') \
            .persist()

        logger.info("User zones determined")
        return user_zone_df
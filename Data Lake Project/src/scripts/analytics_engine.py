import pyspark.sql.functions as F
from pyspark.sql.window import Window
import logging

# Setup Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

class AnalyticsEngine:
    """ Calculates user and zone-level analytics. """

    def __init__(self, events_geo_df):
        self.events_geo_df = events_geo_df

    def calculate_mart_zones(self):
        """Calculates aggregated metrics for user activity within zones.

        Returns:
            pyspark.sql.DataFrame: The final datamart DataFrame.
        """

        logger.info("Starting Zone-level analytics")
        # Registrations (assuming 'message_from' indicates new user)
        window_rn = Window.partitionBy('user_id').orderBy(F.col('date').desc())
        registrations_df = self.events_geo_df \
            .withColumn('month', F.trunc(F.col('date'), 'month')) \
            .withColumn('week', F.trunc(F.col('date'), 'week')) \
            .where('user_id is not null') \
            .withColumn('rn', F.row_number().over(window_rn)) \
            .where('rn = 1') \
            .selectExpr('month', 'week', 'zone_id') \
            .distinct()

        # Zone analytics
        window_week = Window.partitionBy('week', 'zone_id')
        window_month = Window.partitionBy('month', 'zone_id')

        mart_zones_df = self.events_geo_df \
            .withColumn('week_message', F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).over(window_week)) \
            .withColumn('week_reaction', F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).over(window_week)) \
            .withColumn('week_subscription', F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).over(window_week)) \
            .withColumn('month_message', F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).over(window_month)) \
            .withColumn('month_reaction', F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).over(window_month)) \
            .withColumn('month_subscription', F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).over(window_month)) \
            .join(registrations_df, ['month', 'week', 'zone_id'], 'left') \
                .fillna({'week_message': 0, 'week_reaction': 0, 'week_subscription': 0,
                     'month_message': 0, 'month_reaction': 0, 'month_subscription': 0}) \
            .orderBy('month', 'week', 'zone_id') 

        logger.info("Zone-level analytics calculated successfully")
        return mart_zones_df
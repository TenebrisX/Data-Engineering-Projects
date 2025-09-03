import pyspark.sql.functions as F
import logging
from scripts.utils.distance_calculator import calculate_distance

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

class FriendRecommender:
    def __init__(self, events_df, user_zone_df, cities_df):
        self.events_df = events_df
        self.user_zone_df = user_zone_df
        self.cities_df = cities_df

    def extract_and_deduplicate_message_pairs(self):
        """Extracts sender-receiver pairs and removes duplicate directions.

        Ensures that if user A sends a message to user B, we don't also include
        the redundant pair of user B sending a message to user A.
        """ 
        logger.debug("Extracting and deduplicating message pairs")

        return self.events_df \
            .where("event_type = 'message'") \
            .selectExpr('user_msg_from as user_left', 'user_msg_to as user_right') \
            .unionAll(self.events_df 
                .where("event_type = 'message'")
                .selectExpr('user_msg_to as user_left', 'user_msg_from as user_right')) \
            .distinct()

    def get_subscription_pairs(self):
        """Finds users subscribed to the same channels."""
        logger.debug("Finding subscription pairs")

        return self.events_df \
            .where('user_id is not null and subscription_channel is not null') \
            .selectExpr('user_id as user_left', 'subscription_channel') \
            .join(self.events_df.selectExpr('user_id as user_right', 'subscription_channel'),
                  'subscription_channel', 'inner') \
            .where('user_left <> user_right')

    def calculate_recommendations(self):
        """Calculates friend recommendations based on shared subscriptions and proximity.
        """ 

        logger.info("Starting friend recommendation calculation")

        # Stage 1: Extract message sender-receiver pairs, removing duplicates
        message_pairs = self.extract_and_deduplicate_message_pairs()  

        # Stage 2: Identify users subscribing to the same channels
        subscription_pairs = self.get_subscription_pairs()          

        # Stage 3: Start building recommendations based on shared subscriptions
        recommendations_df = self._create_initial_recommendations(subscription_pairs)

        # Stage 4: Filter recommendations, ensuring users have messaged each other
        recommendations_df = self._filter_by_messages(recommendations_df, message_pairs)

        # Stage 5: Incorporate geographical data and filter based on proximity 
        recommendations_df = self._join_and_filter_by_zone(recommendations_df)

        logger.info("Friend recommendations calculated")
        return recommendations_df


    def _create_initial_recommendations(self, subscription_pairs):
        """Starts building recommendations based on shared subscriptions."""
        logger.info("started _create_initial_recommendations")
        df = subscription_pairs.alias('subs') \
            .withColumn('users_all', F.trim(F.concat(F.col('subs.user_left'), F.lit('-'), F.col('subs.user_right'))))
        logger.info("finished _create_initial_recommendations")
        return df

    def _filter_by_messages(self, df, message_pairs):
        """Removes pairs where users haven't directly sent messages to each other."""
        logger.info("started _filter_by_messages")
        df = df.join(message_pairs.alias('msgs'),
                     F.col('users_all') == F.trim(F.concat(F.col('msgs.user_left'), F.lit('-'), F.col('msgs.user_right'))),
                     'leftanti') \
            .drop('users_all', 'subs.subscription_channel')
        logger.info("finished _filter_by_messages")
        return df

    def _join_and_filter_by_zone(self, df):  
        """Joins geographic data and filters users based on proximity. """
        logger.info("started _join_and_filter_by_zone")
        df = df \
            .join(self.user_zone_df.alias('zones'), F.col('subs.user_left') == F.col('zones.user_id'), 'left') \
            .join(self.user_zone_df.withColumnRenamed('zone_id', 'zone_id_right').alias('zones_right'),
                  F.col('subs.user_right') == F.col('zones_right.user_id'), 'left') \
            .withColumn('distance_between_users', 
                        calculate_distance(F.col('zones.lat_rad'), F.col('zones.lng_rad'), 
                                           F.col('zones_right.lat_rad'), F.col('zones_right.lng_rad'))) \
            .where(F.col('distance_between_users') <= 1) \
            .selectExpr('subs.user_left', 'subs.user_right', 'zones.date as processed_dttm', 'zones.zone_id as zone_id_left')
        logger.info("finished _join_and_filter_by_zone")
        return df 

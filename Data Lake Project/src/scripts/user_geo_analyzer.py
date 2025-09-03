import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql.types import TimestampType
from scripts.utils.distance_calculator import calculate_distance
import logging
import pytz

logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 

console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

def find_nearest_city(events_df, cities_df):
    """Finds the nearest city for each message event.

    Args:
        events_df (pyspark.sql.DataFrame): DataFrame containing events data.
        cities_df (pyspark.sql.DataFrame): DataFrame containing cities data.

    Returns:
        pyspark.sql.DataFrame: DataFrame with message IDs, nearest city IDs, and distance.
    """

    events_df = events_df.crossJoin(cities_df)
    events_df = events_df.withColumn(
        'distance',
        calculate_distance(
            F.col('msg_lat_rad'), F.col('msg_lng_rad'), F.col('lat_rad'), F.col('lng_rad')
        )
    )
    

    window = Window.partitionBy('message_id').orderBy('distance')
    return events_df.withColumn('row_number', F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number', 'msg_lat_rad', 'msg_lng_rad', 'lat_rad', 'lng_rad') \
        .withColumnRenamed('city_id', 'city')
        

def determine_actual_city(events_geo_df):
    """Determines the most recent city a user sent a message from.

    Args:
        events_geo_df (pyspark.sql.DataFrame): DataFrame with geographic event data.

    Returns:
        pyspark.sql.DataFrame: DataFrame with user IDs and their most recent city.
    """

    window = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    return events_geo_df.selectExpr('user_id', 'message_id', 'date', 'city') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .selectExpr('user_id', 'city as act_city')
        

def determine_home_city(events_geo_df, min_duration_days=27):
    """Determines a user's home city based on consecutive days spent there.

    Args:
        events_geo_df (pyspark.sql.DataFrame): DataFrame with geographic event data.
        min_duration_days (int, optional): Minimum consecutive days in a city to consider it 'home'. Defaults to 27.

    Returns:
        pyspark.sql.DataFrame: DataFrame with user IDs and their determined home city.
    """

    logger.debug("Calculating cities where user stayed at least the minimum duration")

    window = Window().partitionBy('user_id', 'date', 'city').orderBy('message_id')
    window_2 = Window().partitionBy('user_id').orderBy('date', 'message_id')

    subquery = events_geo_df \
        .selectExpr('user_id', 'date', 'message_id', 'city') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .withColumn('prev_city', F.lag('city', offset=1).over(window_2)) \
        .where('prev_city is null or city != prev_city') \
        .withColumn('dt_of_city_change', F.lead('date', offset=1).over(window_2)) \
        .withColumn('days_in_city',
                    F.when(F.col('dt_of_city_change').isNotNull(),
                           F.datediff(F.col('dt_of_city_change'), F.col('date')))) \
        .groupBy('user_id', 'city') \
        .agg(F.sum(F.col('days_in_city')).alias('total_days')) \
        .filter('total_days >= {}'.format(min_duration_days))

    result = events_geo_df \
        .join(subquery, ['user_id', 'city'], 'inner') \
        .selectExpr('user_id', 'city as home_city') \
        .filter('total_days >= {}'.format(min_duration_days)) \
        .filter('days_in_city >= {}'.format(min_duration_days))

    return result
        

def calculate_travel(events_geo_df):
    """Calculates travel information for each user based on city changes.

    Args:
        events_geo_df (pyspark.sql.DataFrame): DataFrame with geographic event data.

    Returns:
        pyspark.sql.DataFrame: DataFrame containing user IDs, travel count, and an array of visited cities.
    """

    window = Window.partitionBy('user_id').orderBy('date', 'message_id')

    travel_city_df = events_geo_df \
        .selectExpr('user_id', 'date', 'message_id', 'city') \
        .withColumn('prev_city_message', F.lag('city', offset=1).over(window)) \
        .where('city != prev_city_message') \
        .groupBy('user_id') \
        .agg(
            F.count('city').alias('travel_count'),
            F.collect_list('city').alias('travel_array')
        )

    return travel_city_df


def calculate_local_time(events_geo_df):
    """Calculates the local time for a user's most recent event in each city.

    Args:
        events_geo_df (pyspark.sql.DataFrame): DataFrame with geographic event data and datetime information.

    Returns:
        pyspark.sql.DataFrame: DataFrame with user IDs, local time, and timezone.
    """

    window = Window.partitionBy('user_id').orderBy(F.col('datetime').desc())

    def local_time_from_city(timestamp, city):
        city_timezone = pytz.timezone('Australia/' + city.replace(' ', '_'))
        return timestamp.astimezone(city_timezone)

    udf_local_time = F.udf(local_time_from_city, TimestampType())

    time_info_df = events_geo_df \
        .select('user_id', 'date', 'datetime', 'message_id', 'city') \
        .where('datetime is not null') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .withColumn('time', F.col('datetime').cast('Timestamp')) \
        .withColumn('local_time', udf_local_time(F.col('time'), F.col('city'))) \
        .selectExpr('user_id', 'local_time', 'city as timezone')

    return time_info_df
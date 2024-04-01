import pyspark.sql.functions as F
import logging

# Setup Logger
logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 
# Create a console handler to see log messages 
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

class DataLoadError(Exception):
    """Represents an error that occurs during data loading."""
    pass

class DataLoader:
    """ Provides functionality for loading events and cities data into Spark DataFrames. """

    def __init__(self, spark):
        self.spark = spark

    def load_events_users(self, path, event_type='message'):
        """Loads events data from the specified path.

        Args:
            path (str): The path to the events data.
            event_type (str, optional): The type of event to filter for. Defaults to 'message'.

        Returns:
            pyspark.sql.DataFrame: The loaded events DataFrame.

        Raises:
            DataLoadError: If there's a failure during data loading.
        """

        try:
            events_df = self.spark.read.parquet(f'{path}/event_type={event_type}') \
                .selectExpr("event.message_id",
                            "event.message_from as user_id",
                            "date",
                            "event.datetime as datetime",
                            "lat",
                            "lon") \
                .where("user_id is not null") \
                .withColumn('msg_lat_rad', F.radians(F.col('lat'))) \
                .withColumn('msg_lng_rad', F.radians(F.col('lon'))) \
                .drop('lat', 'lon')
            logger.info("load_events_users data loaded successfully")
            return events_df 

        except Exception as e:
            logger.error(f"Failed to load events_users data: {e}")
            raise DataLoadError("Events data loading failed") from e


    def load_cities(self, path):
        """Loads cities data from the specified path.

        Args:
            path (str): The path to the cities data.

        Returns:
            pyspark.sql.DataFrame: The loaded cities DataFrame.

        Raises:
            DataLoadError: If there's a failure during data loading.
        """

        try:
            cities_df = self.spark.read \
                .option('header', True) \
                .option('delimiter', ';') \
                .csv(path) \
                .withColumn('lat_rad', F.radians(F.regexp_replace('lat', ',', '.'))) \
                .withColumn('lng_rad', F.radians(F.regexp_replace('lng', ',', '.'))) \
                .na.replace(' ', '') \
                .drop('lat', 'lng')
            logger.info("Geo data loaded successfully")
            return cities_df

        except Exception as e:
            logger.error(f"Failed to load geo data: {e}")
            raise DataLoadError("Geo data loading failed") from e
        
    def load_events_zones(self, path):
        """Loads and preprocesses events data.

        Args:
            path (str): The path to the events Parquet files.

        Returns:
            pyspark.sql.DataFrame: The processed events DataFrame.
        """
        try:
            events_df = self.spark.read.parquet(path) \
                .selectExpr("date",
                            "event.user as user",
                            "event_type",
                            "event.message_id",
                            "event.message_from",
                            "lat",
                            "lon") \
                .where('lat is not null and lon is not null') \
                .withColumn('msg_lat_rad', F.radians(F.col('lat'))) \
                .withColumn('msg_lng_rad', F.radians(F.col('lon'))) \
                .drop('lat', 'lon') \
                .persist()

            logger.info("load_events_zones data loaded successfully")
            return events_df
        
        except Exception as e:
            logger.error(f"Failed to load events_zones data: {e}")
            raise DataLoadError("Events data loading failed") from e
        
    def load_events_firends_suggestions(self, path):
        """Loads and preprocesses events data.

        Args:
            path (str): The path to the events Parquet files.

        Returns:
            pyspark.sql.DataFrame: The processed events DataFrame.
        """
        try:
            events_df = self.spark.read.parquet(f'{path}') \
                .selectExpr('date',
                            'event_type',
                            'event.user as user_id',
                            'event.subscription_channel',
                            'event.message_id',
                            'event.message_from as user_msg_from',
                            'event.message_to as user_msg_to',
                            'lat',
                            'lon') \
                .persist()

            logger.info("load_events_firends_suggestions data loaded successfully")
            return events_df
        except Exception as e:
            logger.error(f"Failed to load events_firends_suggestions data: {e}")
            raise DataLoadError("Events data loading failed") from e
    
    

class DataWriter:
    """ Provides functionality to write PySpark DataFrames to disk. """ 

    def write_data_parquet(self, df, output_path):
        """ Writes a PySpark DataFrame to the specified output path in Parquet format.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to be written.
            output_path (str): The destination path for the output data.
        """

        df.coalesce(4) \
          .write \
          .mode('overwrite') \
          .parquet(output_path)
        logger.info(f"Datamart written to {output_path}")
          


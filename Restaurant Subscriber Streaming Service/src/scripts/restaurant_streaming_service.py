# Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Kafka and PostgreSQL configuration parameters
KAFKA_BOOTSTRAP_SERVERS = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'
KAFKA_SECURITY_PROTOCOL = 'SASL_SSL'
KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512'
KAFKA_SASL_CONFIG = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password=\"ltcneltyn\";'
POSTGRES_URL = "jdbc:postgresql://localhost:5432/de"
POSTGRES_DRIVER = 'org.postgresql.Driver'
POSTGRES_USER_CLOUD = "student"
POSTGRES_PASSWORD_CLOUD = "de-student"
POSTGRES_USER_LOCAL = "jovyan"
POSTGRES_PASSWORD_LOCAL = "jovyan"


# Input and output Kafka topics
TOPIC_NAME_IN = 'kafka.kotlyarovb_in'
TOPIC_NAME_OUT = 'kafka.kotlyarovb_out'

class RestaurantStreamingService:
    """
    logic of the restaurant advertising subscription service.
    """

    def __init__(self, spark_session, input_topic, output_topic):
        self.spark = spark_session
        self.input_topic = input_topic
        self.output_topic = output_topic

        # Define the input JSON schema
        self.schema = StructType([
            StructField("restaurant_id", StringType()),
            StructField("adv_campaign_id", StringType()),
            StructField("adv_campaign_content", StringType()),
            StructField("adv_campaign_owner", StringType()),
            StructField("adv_campaign_owner_contact", StringType()),
            StructField("adv_campaign_datetime_start", LongType()),
            StructField("adv_campaign_datetime_end", LongType()),
            StructField("datetime_created", LongType()),
        ])

    def read_restaurant_data(self) -> DataFrame:
        """
        Reads restaurant advertising data from the input Kafka topic.

        Returns:
            DataFrame: A DataFrame containing the parsed restaurant data.
        """

        df = self.spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('subscribe', self.input_topic) \
            .option('kafka.security.protocol', KAFKA_SECURITY_PROTOCOL) \
            .option('kafka.sasl.mechanism', KAFKA_SASL_MECHANISM) \
            .option('kafka.sasl.jaas.config', KAFKA_SASL_CONFIG) \
            .load() \
            .select(from_json(col('value').cast(StringType()), self.schema).alias('event'))

        return df

    def read_subscriber_data(self) -> DataFrame:
        """
        Reads the subscriber data from the PostgreSQL database.

       Returns:
           DataFrame: A DataFrame containing subscriber information.
       """

        df = self.spark.read \
            .format('jdbc') \
            .option('url', POSTGRES_URL) \
            .option('driver', POSTGRES_DRIVER) \
            .option('dbtable', 'subscribers_restaurants') \
            .option('user', POSTGRES_USER_CLOUD) \
            .option('password', POSTGRES_PASSWORD_CLOUD) \
            .load() \
            .dropDuplicates(["client_id", "restaurant_id"])

        return df

    def join_and_filter_data(self, restaurant_df: DataFrame, subscriber_df: DataFrame) -> DataFrame:
        """
        Joins the restaurant and subscriber data, filters for active campaigns, and adds timestamps.

        Args:
            restaurant_df (DataFrame): The DataFrame containing restaurant advertising data.
            subscriber_df (DataFrame): The DataFrame containing subscriber information.

        Returns:
            DataFrame: The joined and filtered DataFrame.
        """

        result = restaurant_df \
            .withColumn('trigger_datetime_created', current_timestamp()) \
            .where(
                (col("adv_campaign_datetime_start") < col("trigger_datetime_created")) &
                (col("adv_campaign_datetime_end") > col("trigger_datetime_created"))
            ) \
            .withColumn('timestamp', from_unixtime(col('datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())) \
            .withWatermark('timestamp', '10 minutes') \
            .dropDuplicates(['restaurant_id', 'adv_campaign_id']) \
            .drop("timestamp") \
            .join(subscriber_df, "restaurant_id", how="inner") \
            .select(
                "restaurant_id",
                "adv_campaign_id",
                "adv_campaign_content",
                "adv_campaign_owner",
                "adv_campaign_owner_contact",
                "adv_campaign_datetime_start",
                "adv_campaign_datetime_end",
                "client_id", 
                "datetime_created", 
                "trigger_datetime_created"
            )
        return result

    def send_to_kafka(self, df: DataFrame):
        """
        Sends the prepared data to the output Kafka topic.

        Args:
            df (DataFrame): The DataFrame to send to Kafka.
        """

        df.select(to_json(struct("*")).alias("value")) \
            .writeStream \
            .format("kafka") \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('kafka.security.protocol', KAFKA_SECURITY_PROTOCOL) \
            .option('kafka.sasl.mechanism', KAFKA_SASL_MECHANISM) \
            .option('kafka.sasl.jaas.config', KAFKA_SASL_CONFIG) \
            .option("topic", self.output_topic) \
            .trigger(processingTime="1 minutes") \
            .start()

    def write_to_postgres(self, df: DataFrame):
        """
        Writes data to the PostgreSQL database for feedback analytics.

        Args:
            df (DataFrame): The DataFrame to write to PostgreSQL.
        """

        df.writeStream \
            .format("jdbc") \
            .outputMode('append') \
            .option("url", POSTGRES_URL) \
            .option('driver', POSTGRES_DRIVER) \
            .option("dbtable", "subscribers_feedback") \
            .option("user", POSTGRES_USER_LOCAL) \
            .option("password", POSTGRES_PASSWORD_LOCAL) \
            .trigger(processingTime="1 minutes") \
            .start()

    def run(self):
        """
        Starts the streaming service.
        """

        restaurant_df = self.read_restaurant_data()
        subscriber_df = self.read_subscriber_data()
        joined_df = self.join_and_filter_data(restaurant_df, subscriber_df)

        # Persist the joined DataFrame 
        joined_df.persist()

        # Send to Kafka
        self.send_to_kafka(joined_df.select(
            "restaurant_id", 
            "adv_campaign_id",
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "client_id", 
            "datetime_created", 
            "trigger_datetime_created"
        ))

        # Write to PostgreSQL
        self.write_to_postgres(joined_df.withColumn("feedback", lit(None).cast(StringType())))

        # Clear persisted DataFrame from memory
        joined_df.unpersist()

        # Wait for termination 
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName('Restaurant_Subscribe_Streaming_Service')\
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    service = RestaurantStreamingService(spark, TOPIC_NAME_IN, TOPIC_NAME_OUT)
    service.run()
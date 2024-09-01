import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Initialize SparkSession
    logger.info("Initializing SparkSession")
    spark = SparkSession.builder \
        .appName("KafkaToPostgres") \
        .getOrCreate()
    logger.info("SparkSession initialized successfully")

    # Read data from Kafka
    try:
        logger.info("Reading data from Kafka")
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092") \
            .option("subscribe", "sensor-data") \
            .load()
        logger.info("Successfully connected to Kafka and loaded data")
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        raise

    # Define schema for the incoming data
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Convert Kafka message value to a DataFrame
    try:
        logger.info("Parsing and transforming Kafka data")
        json_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        logger.info("Data parsed and transformed successfully")
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

    # Define PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/sensordb"
    jdbc_properties = {
        "user": "myuser",
        "password": "mypassword",
        "driver": "org.postgresql.Driver"
    }

    # Function to write each micro-batch to PostgreSQL
    def write_to_postgres(batch_df, batch_id):
        try:
            logger.info(f"Writing batch {batch_id} to PostgreSQL")
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "sensor_data") \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .mode("append") \
                .save()
            logger.info(f"Batch {batch_id} written successfully")
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")
            raise

    # Apply the foreachBatch function to process each batch and write it to PostgreSQL
    try:
        logger.info("Starting stream query")
        query = json_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_postgres) \
            .start()
        logger.info("Stream query started successfully")
    except Exception as e:
        logger.error(f"Error starting stream query: {e}")
        raise

    # Await termination
    try:
        logger.info("Awaiting termination")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error during stream processing: {e}")
        raise

except Exception as e:
    logger.error(f"Critical error in Spark job: {e}")
    spark.stop()

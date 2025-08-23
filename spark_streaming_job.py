# Import required libraries for logging, Spark operations, and data type handling
import logging
from pyspark.sql import SparkSession  # Main entry point for Spark functionality
from pyspark.sql.functions import from_json, col, to_timestamp  # Functions for data transformation
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType  # Data type definitions

# Configure logging to show INFO level messages and create a logger instance for this module
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # Create logger to track application flow and errors

try:
    # Log the start of SparkSession initialization process
    logger.info("Initializing SparkSession")
    
    # Create or get existing SparkSession with application name "KafkaToPostgres"
    # This is the entry point for all Spark operations and DataFrame manipulations
    spark = SparkSession.builder \
        .appName("KafkaToPostgres") \
        .getOrCreate()  # Either create new session or get existing one
    # Set application name for identification in Spark UI(kafkaToPostgres)
    
    # Log successful SparkSession creation
    logger.info("SparkSession initialized successfully")

    try:
        # Log the start of Kafka connection process
        logger.info("Reading data from Kafka")
        
        # Read streaming data from Kafka using structured streaming
        kafka_stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "sensor-data") \
            .load()
        
        # Log successful Kafka connection and data loading
        logger.info("Successfully connected to Kafka and loaded data")
    
    # Catch any exceptions during Kafka reading and log them
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")  # Log the specific error
        raise  # Re-raise the exception to be caught by outer try-except

    # Define the schema structure for incoming JSON data from Kafka
    # This schema tells Spark how to parse the JSON messages
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),  # sensor_id field as integer, nullable
        StructField("temperature", FloatType(), True),  # temperature field as float, nullable
        StructField("humidity", FloatType(), True),  # humidity field as float, nullable
        StructField("timestamp", StringType(), True)  # timestamp as string initially, nullable
    ])

    # Wrap data parsing and transformation in try-except block
    try:
        # Log the start of data parsing and transformation process
        logger.info("Parsing and transforming Kafka data")
        
        # Parse and transform the raw Kafka data step by step:
        parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")  # Flatten the nested "data" struct to individual columns
        
        # Define the timestamp format expected in the JSON data (ISO 8601 format)
        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX" 
        
        # Convert the string timestamp column to proper Spark TimestampType for better handling
        json_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), timestamp_format)) 

        # Log successful data parsing and transformation
        logger.info("Data parsed and transformed successfully")
    
    # Catch any exceptions during data transformation and log them
    except Exception as e:
        logger.error(f"Error transforming data: {e}")  # Log transformation errors
        raise  # Re-raise the exception

    # Define PostgreSQL JDBC connection parameters for database connectivity
    jdbc_url = "jdbc:postgresql://localhost:5433/sensordb"  # PostgreSQL connection URL
    jdbc_properties = {
        "user": "myuser",  # Database username
        "password": "mypassword",  # Database password
        "driver": "org.postgresql.Driver"  # JDBC driver class for PostgreSQL
    }

    # Define a function to write each micro-batch of data to PostgreSQL
    # This function will be called for every batch of streaming data
    def write_to_postgres(batch_df, batch_id):
        try:
            # Log the start of writing a specific batch to PostgreSQL
            logger.info(f"Writing batch {batch_id} to PostgreSQL")
            
            # Write the current batch DataFrame to PostgreSQL using JDBC
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "sensor_data") \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .mode("append") \
                .save()
            
            # Log successful completion of batch writing
            logger.info(f"Batch {batch_id} written successfully")
        
        # Catch any exceptions during batch writing and log them
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")  # Log write errors
            raise  # Re-raise the exception

    # Wrap the streaming query start process in try-except block
    try:
        # Log the start of streaming query initialization
        logger.info("Starting stream query")
        
        # Start the streaming query that processes data and writes to PostgreSQL
        query = json_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_postgres) \
            .start()  # Start the streaming query asynchronously
        
        # Log successful streaming query start
        logger.info("Stream query started successfully")
    
    # Catch any exceptions during streaming query start and log them
    except Exception as e:
        logger.error(f"Error starting stream query: {e}")  # Log query start errors
        raise  # Re-raise the exception

    # Wrap the stream termination waiting process in try-except block
    try:
        # Log that the application is waiting for stream termination
        logger.info("Awaiting termination")
        
        # Keep the application running until the stream is manually stopped or encounters an error
        query.awaitTermination()  # Blocks until streaming query terminates
    
    # Catch any exceptions during stream processing and log them
    except Exception as e:
        logger.error(f"Error during stream processing: {e}")  # Log stream processing errors
        raise  # Re-raise the exception

# Global exception handler for any critical errors in the entire Spark job
except Exception as e:
    logger.error(f"Critical error in Spark job: {e}")  # Log critical application errors
    spark.stop()  # Stop the SparkSession to clean up resources
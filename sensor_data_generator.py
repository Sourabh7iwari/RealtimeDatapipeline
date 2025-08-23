import random # For generating random sensor data
import time  # For adding delays between data generation
from datetime import datetime, timezone  # For generating timestamp with timezone information
import pytz
from kafka import KafkaProducer  # Kafka client library for producing messages
import json  # For JSON serialization of data

# Define a function to generate realistic sensor data with random values
def generate_sensor_data():

    ist = pytz.timezone("Asia/Kolkata")  # Define Indian Standard Time (IST) timezone
    
    # Create a dictionary containing simulated sensor readings
    data = {
        "sensor_id": random.randint(1, 5),  # Generate random sensor ID between 1 and 5
        "temperature": round(random.uniform(20, 30), 2),  # Generate temperature between 20-30°C, rounded to 2 decimal places
        "humidity": round(random.uniform(30, 70), 2),  # Generate humidity between 30-70%, rounded to 2 decimal places
        "timestamp": datetime.now(ist).isoformat() # Get current time in IST and format it as ISO 8601 string
    }
    return data  # Return the generated sensor data dictionary

# Function to serialize Python dictionary data into JSON format for Kafka transmission
def json_serializer(data):
    # Convert Python dictionary to JSON string, then encode to UTF-8 bytes (required by Kafka)
    return json.dumps(data).encode("utf-8")

# Kafka Producer Configuration - Set up connection to Kafka broker
producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:9092"],  # Kafka broker address (localhost on port 9092)
    value_serializer=json_serializer  # Function to serialize message values before sending
)

topic_name = "sensor-data"  # Must match the topic name used in the Spark streaming application

# Main loop to continuously generate and send sensor data to Kafka
try:
    while True:
        sensor_data = generate_sensor_data()  # Generate a new sensor data record
        print(f"Sending: {sensor_data}") 
        
        # Send the sensor data to the specified Kafka topic
        producer.send(topic_name, sensor_data)  # send message to Kafka
        producer.flush() # Flush the producer to ensure all messages are sent immediately
        time.sleep(2) 

# Handle keyboard interrupt (Ctrl+C) to gracefully stop the data generation
except KeyboardInterrupt:
    print("Stopped sending sensor data.") 
# Ensure proper cleanup of Kafka producer resources regardless of how the program exits
finally:
    producer.close()  # Close the Kafka producer connection to free up resources
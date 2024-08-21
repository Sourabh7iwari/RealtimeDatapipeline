#script for mimicing the sensor output
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
import json

# Define a function to generate random sensor data
def generate_sensor_data():
    data = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return data

# Function to serialize data into JSON
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=json_serializer
)

# Topic name
topic_name = "sensor-data"

# Generate and send sensor data
try:
    while True:
        sensor_data = generate_sensor_data()
        print(f"Sending: {sensor_data}")
        producer.send(topic_name, sensor_data)
        time.sleep(2)  # Wait for 2 seconds before sending the next data
except KeyboardInterrupt:
    print("Stopped sending sensor data.")
finally:
    producer.close()

#!/bin/bash

# Start Docker services
sudo docker compose up -d

# Wait for Kafka & PostgreSQL
sleep 10

# Run Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
  --jars ./jars/postgresql-jdbc.jar \
  spark_streaming_job.py
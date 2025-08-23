#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🚀 Starting Real-Time Data Pipeline...${NC}"

# Step 1: Start Docker services
echo -e "${YELLOW}🐳 Starting Docker containers...${NC}"
sudo docker compose up -d

# Step 2: Wait for Kafka to be fully ready
echo -e "${YELLOW}⏳ Waiting for Kafka to be ready...${NC}"
until docker exec kafka1 kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo "🔴 Kafka not ready yet... retrying in 5s"
  sleep 5
done
echo -e "${GREEN}✅ Kafka is now ready!${NC}"

# Step 3: Create topics
echo -e "${YELLOW}📝 Creating Kafka topics...${NC}"
./create-topic.sh
if [ $? -ne 0 ]; then
  echo -e "${RED}❌ Failed to create topics!${NC}"
  exit 1
fi
echo -e "${GREEN}✅ Topics created or already exist.${NC}"

# Step 4: Start Spark streaming job
echo -e "${YELLOW}🔥 Starting Spark Streaming job...${NC}"
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 \
  --jars ./jars/postgresql-jdbc.jar \
  spark_streaming_job.py

# Optional: Stop on error
if [ $? -ne 0 ]; then
  echo -e "${RED}❌ Spark job failed!${NC}"
  exit 1
fi
docker exec kafka1 kafka-topics \
  --create \
  --if-not-exists \
  --topic sensor-data \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# more topics can be created similarly
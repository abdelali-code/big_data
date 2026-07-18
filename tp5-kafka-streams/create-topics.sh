#!/bin/bash
set -e

BROKER="localhost:9092"
TOPICS=(text-input text-clean text-dead-letter weather-data station-averages clicks click-counts)

for t in "${TOPICS[@]}"; do
  echo "Creating topic: $t"
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic "$t" \
    --bootstrap-server "$BROKER" \
    --partitions 1 \
    --replication-factor 1
done

echo ""
echo "Topics list:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server "$BROKER"

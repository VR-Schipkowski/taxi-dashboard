#!/bin/sh

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic taxi-locations \
  --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic taxi-speeding \
  --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic taxi-area-violations \
  --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1
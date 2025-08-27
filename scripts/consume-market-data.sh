#!/bin/bash

# Market Data Consumer
# Consumes Bitcoin market data from Kafka and displays it

set -e

echo "📊 Bitcoin Market Data Consumer"
echo "==============================="

# Default configuration
KAFKA_BROKER=${KAFKA_BROKER:-"localhost:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"bitcoin-market"}
MAX_MESSAGES=${MAX_MESSAGES:-10}

echo "📡 Configuration:"
echo "  Kafka Broker: $KAFKA_BROKER"
echo "  Kafka Topic: $KAFKA_TOPIC"
echo "  Max Messages: $MAX_MESSAGES"
echo ""

echo "🔍 Consuming market data from Kafka..."
echo "Press Ctrl+C to stop"
echo ""

# Consume messages from Kafka
docker exec rde-kafka-1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$KAFKA_TOPIC" \
    --from-beginning \
    --max-messages "$MAX_MESSAGES" \
    --property print.timestamp=true \
    --property print.key=true \
    --property print.value=true

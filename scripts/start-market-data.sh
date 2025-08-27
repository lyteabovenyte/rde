#!/bin/bash

# Bitcoin Market Data Collector
# Fetches real-time Bitcoin price data and sends to Kafka

set -e

echo "🚀 Starting Bitcoin Market Data Collector"
echo "=========================================="

# Default configuration
KAFKA_BROKER=${KAFKA_BROKER:-"localhost:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"bitcoin-market"}
INTERVAL=${INTERVAL:-30}
CURRENCIES=${CURRENCIES:-"USD,EUR,GBP,JPY,CNY,CAD,AUD,CHF"}

echo "📡 Configuration:"
echo "  Kafka Broker: $KAFKA_BROKER"
echo "  Kafka Topic: $KAFKA_TOPIC"
echo "  Collection Interval: ${INTERVAL}s"
echo "  Target Currencies: $CURRENCIES"
echo ""

# Check if Kafka is running
echo "🔍 Checking Kafka connectivity..."
if ! docker exec rde-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list | grep -q "$KAFKA_TOPIC"; then
    echo "❌ Kafka topic '$KAFKA_TOPIC' not found. Creating it..."
    docker exec rde-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic "$KAFKA_TOPIC" --partitions 1 --replication-factor 1
fi

echo "✅ Kafka is ready"
echo ""

# Build the collector if needed
echo "🔨 Building market data collector..."
cargo build --release --bin market-data

echo ""
echo "📊 Starting data collection..."
echo "Press Ctrl+C to stop"
echo ""

# Run the collector
cargo run --release --bin market-data -- \
    --kafka-broker "$KAFKA_BROKER" \
    --kafka-topic "$KAFKA_TOPIC" \
    --interval "$INTERVAL" \
    --currencies "$CURRENCIES"

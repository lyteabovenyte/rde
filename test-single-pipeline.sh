#!/bin/bash

# Test script for a single dataset end-to-end

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

DATASET=${1:-flights}
TOPIC=$DATASET

echo -e "${GREEN}🧪 Testing $DATASET pipeline end-to-end${NC}"
echo "============================================="

# Clean up any existing processes
echo -e "${YELLOW}🧹 Cleaning up existing processes...${NC}"
pkill -f "rde-cli" || true
pkill -f "kafka-producer" || true
sleep 2

# Delete and recreate topic to start fresh
echo -e "${YELLOW}🔄 Recreating Kafka topic: $TOPIC${NC}"
docker exec rde-kafka-1 kafka-topics --delete --topic $TOPIC --bootstrap-server localhost:9092 2>/dev/null || true
sleep 2
docker exec rde-kafka-1 kafka-topics --create --topic $TOPIC --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Generate sample data for testing
echo -e "${YELLOW}📊 Preparing test data...${NC}"
case $DATASET in
    "flights")
        head -100 data/json-samples/flights.json > /tmp/test-${DATASET}.json
        FORMAT="ndjson"
        CONFIG="examples/flights-pipeline.yml"
        ;;
    "retail") 
        jq '.[0:100]' data/json-samples/retail.json > /tmp/test-${DATASET}.json
        FORMAT="array"
        CONFIG="examples/retail-pipeline.yml"
        ;;
    "spotify")
        jq '.audio_features[0:50][]' data/json-samples/spotify.json > /tmp/test-${DATASET}.ndjson
        mv /tmp/test-${DATASET}.ndjson /tmp/test-${DATASET}.json
        FORMAT="ndjson"
        CONFIG="examples/spotify-pipeline.yml"
        ;;
esac

echo -e "${GREEN}✓ Test data prepared: /tmp/test-${DATASET}.json${NC}"

# Start the pipeline in background
echo -e "${YELLOW}🔄 Starting pipeline...${NC}"
RUST_LOG=info cargo run --bin rde-cli -- -p $CONFIG > logs/test-${DATASET}.log 2>&1 &
PIPELINE_PID=$!
echo -e "${GREEN}✓ Pipeline started (PID: $PIPELINE_PID)${NC}"

# Wait for pipeline to initialize
sleep 3

# Stream test data
echo -e "${YELLOW}📤 Streaming test data to Kafka...${NC}"
cargo run --bin kafka-producer -- \
    --input "/tmp/test-${DATASET}.json" \
    --topic "$TOPIC" \
    --format "$FORMAT" \
    --batch-size 10 \
    --delay-ms 100 \
    --progress-interval 25

echo -e "${GREEN}✓ Data streaming completed${NC}"

# Wait for pipeline to process messages
echo -e "${YELLOW}⏳ Waiting for pipeline to process messages...${NC}"
sleep 10

# Check results
echo -e "${YELLOW}🔍 Checking results...${NC}"

# Check Kafka messages were consumed
echo -e "${BLUE}Kafka topic status:${NC}"
MESSAGE_COUNT=$(docker exec rde-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic $TOPIC --time -1 2>/dev/null | \
    awk -F ':' '{sum += $3} END {print sum}' || echo "0")
echo -e "  📈 Total messages in topic: $MESSAGE_COUNT"

# Check MinIO files
echo -e "${BLUE}MinIO files:${NC}"
TABLE_NAME=""
case $DATASET in
    "flights") TABLE_NAME="flights_data" ;;
    "retail") TABLE_NAME="retail_products" ;;
    "spotify") TABLE_NAME="spotify_audio_features" ;;
esac

FILES=$(docker exec minio-server mc ls myminio/iceberg-data/$TABLE_NAME/ --recursive 2>/dev/null | wc -l || echo "0")
echo -e "  📁 Files in MinIO: $FILES"

if [[ $FILES -gt 0 ]]; then
    echo -e "${GREEN}✅ SUCCESS: Files created in MinIO!${NC}"
    docker exec minio-server mc ls myminio/iceberg-data/$TABLE_NAME/ --recursive
else
    echo -e "${RED}❌ FAILED: No files in MinIO${NC}"
    echo -e "${YELLOW}Pipeline logs:${NC}"
    tail -20 logs/test-${DATASET}.log
fi

# Show recent pipeline logs
echo -e "${BLUE}Recent pipeline logs:${NC}"
tail -10 logs/test-${DATASET}.log

# Cleanup
echo -e "${YELLOW}🧹 Cleaning up...${NC}"
kill $PIPELINE_PID 2>/dev/null || true
rm -f /tmp/test-${DATASET}.json

echo -e "${GREEN}🏁 Test completed for $DATASET${NC}"

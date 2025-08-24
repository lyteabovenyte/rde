#!/bin/bash

# Script to stream test data to Kafka for testing the RDE pipeline

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Default values
BROKERS="localhost:9092"
DELAY_MS=1000
DATA_DIR="data/json-samples"

echo -e "${GREEN}RDE Kafka Test Data Streamer${NC}"
echo "=============================="

# Function to check if Kafka is running
check_kafka() {
    echo -e "${YELLOW}Checking if Kafka is running...${NC}"
    if ! nc -z localhost 9092 2>/dev/null; then
        echo -e "${RED}Error: Kafka doesn't seem to be running on localhost:9092${NC}"
        echo "Please run: docker-compose -f docker/docker-compose.yml up -d"
        exit 1
    fi
    echo -e "${GREEN}✓ Kafka is running${NC}"
}

# Function to stream data to a topic
stream_to_topic() {
    local file=$1
    local topic=$2
    local delay=${3:-$DELAY_MS}
    
    echo -e "${YELLOW}Streaming $file to topic '$topic' with ${delay}ms delay...${NC}"
    
    cargo run --bin kafka-producer -- \
        --input "$file" \
        --topic "$topic" \
        --brokers "$BROKERS" \
        --delay-ms "$delay" \
        --progress-interval 10
}

# Main menu
show_menu() {
    echo ""
    echo "Select data to stream:"
    echo "1) User Events (schema with metadata)"
    echo "2) Product Analytics (NDJSON format)"
    echo "3) Transactions (nested JSON)"
    echo "4) Schema Evolution Demo (gradual field addition)"
    echo "5) All datasets (sequential)"
    echo "6) Custom file/topic"
    echo "0) Exit"
    echo ""
    read -p "Enter your choice: " choice
    
    case $choice in
        1)
            stream_to_topic "$DATA_DIR/user-events.json" "user-events" 500
            ;;
        2)
            stream_to_topic "$DATA_DIR/product-analytics.ndjson" "products" 200
            ;;
        3)
            echo -e "${YELLOW}Note: This file contains nested transactions. Each transaction will be sent as a separate message.${NC}"
            # For nested JSON, we need to extract and send individual transactions
            cargo run --bin kafka-producer -- \
                --input "$DATA_DIR/transactions.json" \
                --topic "transactions" \
                --brokers "$BROKERS" \
                --format object
            ;;
        4)
            echo -e "${YELLOW}This demonstrates schema evolution - watch how fields are added progressively${NC}"
            stream_to_topic "$DATA_DIR/schema-evolution-demo.ndjson" "evolution-test" 2000
            ;;
        5)
            echo -e "${GREEN}Streaming all datasets sequentially...${NC}"
            stream_to_topic "$DATA_DIR/user-events.json" "user-events" 500
            stream_to_topic "$DATA_DIR/product-analytics.ndjson" "products" 200
            stream_to_topic "$DATA_DIR/schema-evolution-demo.ndjson" "evolution-test" 1000
            ;;
        6)
            read -p "Enter file path: " custom_file
            read -p "Enter topic name: " custom_topic
            read -p "Enter delay in ms (default 1000): " custom_delay
            custom_delay=${custom_delay:-1000}
            stream_to_topic "$custom_file" "$custom_topic" "$custom_delay"
            ;;
        0)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice${NC}"
            ;;
    esac
}

# Check if kafka-producer is built
if [ ! -f "target/release/kafka-producer" ] && [ ! -f "target/debug/kafka-producer" ]; then
    echo -e "${YELLOW}Building kafka-producer...${NC}"
    cargo build --bin kafka-producer
fi

# Check Kafka
check_kafka

# Main loop
while true; do
    show_menu
    echo ""
    read -p "Press Enter to continue..."
done

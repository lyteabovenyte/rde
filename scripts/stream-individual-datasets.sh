#!/bin/bash

# Script to stream individual datasets to their respective Kafka topics

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BROKER="localhost:9092"
DATA_DIR="data/json-samples"

echo -e "${GREEN}📡 Individual Dataset Streaming Script${NC}"
echo "======================================"

# Check if Kafka is running
check_kafka() {
    if ! nc -z localhost 9092 2>/dev/null; then
        echo -e "${RED}❌ Kafka is not running on localhost:9092${NC}"
        echo "Please run: docker-compose -f docker/docker-compose.yml up -d"
        exit 1
    fi
    echo -e "${GREEN}✓ Kafka is running${NC}"
}

# Create Kafka topic
create_topic() {
    local topic=$1
    echo -e "${BLUE}Creating topic: $topic${NC}"
    docker exec rde-kafka-1 kafka-topics \
        --create \
        --topic "$topic" \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists 2>/dev/null || true
}

# Stream flights data
stream_flights() {
    echo -e "${YELLOW}✈️  Streaming flights data...${NC}"
    create_topic "flights"
    
    echo -e "${BLUE}Streaming 1M flight records (NDJSON format)${NC}"
    echo -e "${BLUE}This will take a while... Progress will be shown every 10,000 records${NC}"
    
    cargo run --release --bin kafka-producer -- \
        --input "$DATA_DIR/flights.json" \
        --topic "flights" \
        --format ndjson \
        --batch-size 1000 \
        --delay-ms 5 \
        --progress-interval 10000
    
    echo -e "${GREEN}✅ Flights data streaming completed${NC}"
}

# Stream retail data  
stream_retail() {
    echo -e "${YELLOW}🛒 Streaming retail data...${NC}"
    create_topic "retail"
    
    echo -e "${BLUE}Streaming ~2M retail product records (JSON array format)${NC}"
    echo -e "${BLUE}This will take a while... Progress will be shown every 5,000 records${NC}"
    
    cargo run --release --bin kafka-producer -- \
        --input "$DATA_DIR/retail.json" \
        --topic "retail" \
        --format array \
        --batch-size 500 \
        --delay-ms 10 \
        --progress-interval 5000
    
    echo -e "${GREEN}✅ Retail data streaming completed${NC}"
}

# Stream spotify data
stream_spotify() {
    echo -e "${YELLOW}🎵 Streaming Spotify data...${NC}"
    create_topic "spotify"
    
    echo -e "${BLUE}Extracting audio features and streaming...${NC}"
    
    # Extract audio_features array to temporary file
    echo -e "${BLUE}Extracting audio features from nested JSON...${NC}"
    jq '.audio_features[]' "$DATA_DIR/spotify.json" > /tmp/spotify_features.ndjson
    
    echo -e "${BLUE}Streaming audio features (NDJSON format)${NC}"
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/spotify_features.ndjson" \
        --topic "spotify" \
        --format ndjson \
        --batch-size 100 \
        --delay-ms 25 \
        --progress-interval 200
    
    # Cleanup
    rm -f /tmp/spotify_features.ndjson
    
    echo -e "${GREEN}✅ Spotify data streaming completed${NC}"
}

# Show menu
show_menu() {
    echo ""
    echo -e "${YELLOW}Select dataset to stream:${NC}"
    echo "1) ✈️  Flights (1M records, ~5-10 minutes)"
    echo "2) 🛒 Retail (2M records, ~10-15 minutes)"  
    echo "3) 🎵 Spotify (~2K records, ~1 minute)"
    echo "4) 🚀 All datasets (sequential)"
    echo "5) 📊 Quick test (first 1000 records of each)"
    echo "0) 🚪 Exit"
    echo ""
}

# Stream first N records for testing
quick_test() {
    echo -e "${YELLOW}🧪 Quick test mode - streaming first 1000 records of each dataset${NC}"
    
    # Flights (first 1000 lines)
    echo -e "${BLUE}Testing flights data...${NC}"
    create_topic "flights"
    head -1000 "$DATA_DIR/flights.json" > /tmp/flights_test.json
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/flights_test.json" \
        --topic "flights" \
        --format ndjson \
        --batch-size 100 \
        --delay-ms 10 \
        --progress-interval 200
    
    # Retail (extract first 1000 items from array)
    echo -e "${BLUE}Testing retail data...${NC}"
    create_topic "retail"
    jq '.[0:1000]' "$DATA_DIR/retail.json" > /tmp/retail_test.json
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/retail_test.json" \
        --topic "retail" \
        --format array \
        --batch-size 100 \
        --delay-ms 10 \
        --progress-interval 200
    
    # Spotify (first 100 audio features)
    echo -e "${BLUE}Testing spotify data...${NC}"
    create_topic "spotify"
    jq '.audio_features[0:100][]' "$DATA_DIR/spotify.json" > /tmp/spotify_test.ndjson
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/spotify_test.ndjson" \
        --topic "spotify" \
        --format ndjson \
        --batch-size 50 \
        --delay-ms 20 \
        --progress-interval 25
    
    # Cleanup
    rm -f /tmp/flights_test.json /tmp/retail_test.json /tmp/spotify_test.ndjson
    
    echo -e "${GREEN}✅ Quick test completed${NC}"
}

# Stream all datasets
stream_all() {
    echo -e "${YELLOW}🚀 Streaming all datasets sequentially...${NC}"
    echo -e "${RED}⚠️  This will take 20-30 minutes total!${NC}"
    echo ""
    read -p "Continue? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        return
    fi
    
    stream_spotify   # Smallest dataset first
    echo -e "${BLUE}Waiting 10 seconds before next dataset...${NC}"
    sleep 10
    
    stream_flights   # Medium dataset
    echo -e "${BLUE}Waiting 10 seconds before next dataset...${NC}"
    sleep 10
    
    stream_retail    # Largest dataset
    
    echo -e "${GREEN}🎉 All datasets streamed successfully!${NC}"
}

# Verify data files exist
verify_files() {
    local missing_files=0
    
    if [[ ! -f "$DATA_DIR/flights.json" ]]; then
        echo -e "${RED}❌ Missing: $DATA_DIR/flights.json${NC}"
        missing_files=1
    fi
    
    if [[ ! -f "$DATA_DIR/retail.json" ]]; then
        echo -e "${RED}❌ Missing: $DATA_DIR/retail.json${NC}"
        missing_files=1
    fi
    
    if [[ ! -f "$DATA_DIR/spotify.json" ]]; then
        echo -e "${RED}❌ Missing: $DATA_DIR/spotify.json${NC}"
        missing_files=1
    fi
    
    if [[ $missing_files -eq 1 ]]; then
        echo -e "${RED}Please ensure all JSON files are in the $DATA_DIR directory${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ All data files found${NC}"
}

# Check if producer binary exists
check_producer() {
    if [[ ! -f "target/release/kafka-producer" ]] && [[ ! -f "target/debug/kafka-producer" ]]; then
        echo -e "${YELLOW}Building kafka-producer...${NC}"
        cargo build --release --bin kafka-producer
    fi
    echo -e "${GREEN}✓ Kafka producer ready${NC}"
}

# Main execution
main() {
    verify_files
    check_kafka
    check_producer
    
    if [[ $# -gt 0 ]]; then
        case "$1" in
            "flights") stream_flights ;;
            "retail") stream_retail ;;
            "spotify") stream_spotify ;;
            "all") stream_all ;;
            "test") quick_test ;;
            *) 
                echo -e "${RED}Invalid option: $1${NC}"
                echo "Usage: $0 [flights|retail|spotify|all|test]"
                exit 1
                ;;
        esac
        return
    fi
    
    # Interactive mode
    while true; do
        show_menu
        read -p "Enter your choice: " choice
        
        case $choice in
            1) stream_flights ;;
            2) stream_retail ;;
            3) stream_spotify ;;
            4) stream_all ;;
            5) quick_test ;;
            0) 
                echo -e "${GREEN}Goodbye!${NC}"
                exit 0 
                ;;
            *) 
                echo -e "${RED}Invalid choice${NC}"
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"

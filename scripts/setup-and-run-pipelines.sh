#!/bin/bash

# Complete automation script for running the RDE pipeline with sample data

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 RDE Pipeline Automation Script${NC}"
echo "=================================="

# Configuration
KAFKA_BROKER="localhost:9092"
MINIO_ENDPOINT="http://localhost:9000"
DATA_DIR="data/json-samples"

# Check if required files exist
check_files() {
    echo -e "${YELLOW}📂 Checking required files...${NC}"
    
    local files=("$DATA_DIR/flights.json" "$DATA_DIR/retail.json" "$DATA_DIR/spotify.json")
    for file in "${files[@]}"; do
        if [[ ! -f "$file" ]]; then
            echo -e "${RED}❌ Required file not found: $file${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Found: $file${NC}"
    done
}

# Check if services are running
check_services() {
    echo -e "${YELLOW}🔍 Checking required services...${NC}"
    
    # Check Kafka
    if ! nc -z localhost 9092 2>/dev/null; then
        echo -e "${RED}❌ Kafka is not running on localhost:9092${NC}"
        echo "Please run: docker-compose -f docker/docker-compose.yml up -d"
        exit 1
    fi
    echo -e "${GREEN}✓ Kafka is running${NC}"
    
    # Check MinIO
    if ! nc -z localhost 9000 2>/dev/null; then
        echo -e "${RED}❌ MinIO is not running on localhost:9000${NC}"
        echo "Please run: ./scripts/start-minio.sh"
        exit 1
    fi
    echo -e "${GREEN}✓ MinIO is running${NC}"
}

# Build the project
build_project() {
    echo -e "${YELLOW}🔨 Building the project...${NC}"
    cargo build --release --bin rde-cli --bin kafka-producer
    echo -e "${GREEN}✓ Build completed${NC}"
}

# Create Kafka topics
create_topics() {
    echo -e "${YELLOW}📡 Creating Kafka topics...${NC}"
    
    local topics=("flights" "retail" "spotify")
    
    for topic in "${topics[@]}"; do
        echo -e "${BLUE}Creating topic: $topic${NC}"
        docker exec -it rde-kafka-1 kafka-topics \
            --create \
            --topic "$topic" \
            --bootstrap-server localhost:9092 \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists 2>/dev/null || true
    done
    
    echo -e "${GREEN}✓ Topics created${NC}"
}

# Stream data to Kafka topics
stream_data() {
    echo -e "${YELLOW}📊 Streaming data to Kafka topics...${NC}"
    
    # Stream flights data (NDJSON format, 1M records)
    echo -e "${BLUE}Streaming flights data...${NC}"
    cargo run --release --bin kafka-producer -- \
        --input "$DATA_DIR/flights.json" \
        --topic "flights" \
        --format ndjson \
        --batch-size 1000 \
        --delay-ms 10 \
        --progress-interval 10000 &
    FLIGHTS_PID=$!
    
    # Stream retail data (JSON array format, ~2M records)
    echo -e "${BLUE}Streaming retail data...${NC}"
    cargo run --release --bin kafka-producer -- \
        --input "$DATA_DIR/retail.json" \
        --topic "retail" \
        --format array \
        --batch-size 500 \
        --delay-ms 20 \
        --progress-interval 5000 &
    RETAIL_PID=$!
    
    # Stream spotify data (nested JSON, extract audio_features array)
    echo -e "${BLUE}Streaming spotify data...${NC}"
    # First extract audio_features array to a temporary file
    jq '.audio_features[]' "$DATA_DIR/spotify.json" > /tmp/spotify_features.ndjson
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/spotify_features.ndjson" \
        --topic "spotify" \
        --format ndjson \
        --batch-size 100 \
        --delay-ms 50 \
        --progress-interval 500 &
    SPOTIFY_PID=$!
    
    # Store PIDs for later cleanup
    echo $FLIGHTS_PID > /tmp/flights_producer.pid
    echo $RETAIL_PID > /tmp/retail_producer.pid
    echo $SPOTIFY_PID > /tmp/spotify_producer.pid
    
    echo -e "${GREEN}✓ Data streaming started (running in background)${NC}"
    echo -e "${YELLOW}📝 Producer PIDs: Flights=$FLIGHTS_PID, Retail=$RETAIL_PID, Spotify=$SPOTIFY_PID${NC}"
}

# Run RDE pipelines
run_pipelines() {
    echo -e "${YELLOW}🔄 Starting RDE pipelines...${NC}"
    
    # Create log directory
    mkdir -p logs
    
    # Start flights pipeline
    echo -e "${BLUE}Starting flights pipeline...${NC}"
    cargo run --release --bin rde-cli -- \
        --pipeline examples/flights-pipeline.yml > logs/flights-pipeline.log 2>&1 &
    FLIGHTS_PIPELINE_PID=$!
    
    # Start retail pipeline
    echo -e "${BLUE}Starting retail pipeline...${NC}"
    cargo run --release --bin rde-cli -- \
        --pipeline examples/retail-pipeline.yml > logs/retail-pipeline.log 2>&1 &
    RETAIL_PIPELINE_PID=$!
    
    # Start spotify pipeline
    echo -e "${BLUE}Starting spotify pipeline...${NC}"
    cargo run --release --bin rde-cli -- \
        --pipeline examples/spotify-pipeline.yml > logs/spotify-pipeline.log 2>&1 &
    SPOTIFY_PIPELINE_PID=$!
    
    # Store PIDs for later cleanup
    echo $FLIGHTS_PIPELINE_PID > /tmp/flights_pipeline.pid
    echo $RETAIL_PIPELINE_PID > /tmp/retail_pipeline.pid
    echo $SPOTIFY_PIPELINE_PID > /tmp/spotify_pipeline.pid
    
    echo -e "${GREEN}✓ Pipelines started${NC}"
    echo -e "${YELLOW}📝 Pipeline PIDs: Flights=$FLIGHTS_PIPELINE_PID, Retail=$RETAIL_PIPELINE_PID, Spotify=$SPOTIFY_PIPELINE_PID${NC}"
}

# Monitor progress
monitor_progress() {
    echo -e "${YELLOW}📊 Monitoring pipeline progress...${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop monitoring and shut down pipelines${NC}"
    echo ""
    
    while true; do
        echo -e "${GREEN}=== Pipeline Status $(date) ===${NC}"
        
        # Check if producers are still running
        if kill -0 $FLIGHTS_PID 2>/dev/null; then
            echo -e "🟢 Flights producer: RUNNING"
        else
            echo -e "🔴 Flights producer: STOPPED"
        fi
        
        if kill -0 $RETAIL_PID 2>/dev/null; then
            echo -e "🟢 Retail producer: RUNNING"
        else
            echo -e "🔴 Retail producer: STOPPED"
        fi
        
        if kill -0 $SPOTIFY_PID 2>/dev/null; then
            echo -e "🟢 Spotify producer: RUNNING"
        else
            echo -e "🔴 Spotify producer: STOPPED"
        fi
        
        # Check Kafka topic message counts
        echo -e "${BLUE}Kafka topic message counts:${NC}"
        for topic in flights retail spotify; do
            count=$(docker exec rde-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:9092 \
                --topic $topic --time -1 2>/dev/null | \
                awk -F ':' '{sum += $3} END {print sum}' || echo "0")
            echo -e "  📈 $topic: $count messages"
        done
        
        # Show recent log entries
        echo -e "${BLUE}Recent pipeline logs:${NC}"
        for pipeline in flights retail spotify; do
            if [[ -f "logs/${pipeline}-pipeline.log" ]]; then
                echo -e "  📄 $pipeline: $(tail -1 logs/${pipeline}-pipeline.log 2>/dev/null || echo 'No logs yet')"
            fi
        done
        
        echo ""
        sleep 10
    done
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up processes...${NC}"
    
    # Kill producers
    for pid_file in /tmp/flights_producer.pid /tmp/retail_producer.pid /tmp/spotify_producer.pid; do
        if [[ -f "$pid_file" ]]; then
            PID=$(cat "$pid_file")
            if kill -0 "$PID" 2>/dev/null; then
                echo -e "${BLUE}Stopping producer PID: $PID${NC}"
                kill "$PID" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        fi
    done
    
    # Kill pipelines
    for pid_file in /tmp/flights_pipeline.pid /tmp/retail_pipeline.pid /tmp/spotify_pipeline.pid; do
        if [[ -f "$pid_file" ]]; then
            PID=$(cat "$pid_file")
            if kill -0 "$PID" 2>/dev/null; then
                echo -e "${BLUE}Stopping pipeline PID: $PID${NC}"
                kill "$PID" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        fi
    done
    
    # Clean up temporary files
    rm -f /tmp/spotify_features.ndjson
    
    echo -e "${GREEN}✓ Cleanup completed${NC}"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Main execution
main() {
    case "${1:-all}" in
        "setup")
            check_files
            check_services
            build_project
            create_topics
            ;;
        "stream")
            check_services
            stream_data
            echo -e "${GREEN}✓ Data streaming started. Use 'pkill -f kafka-producer' to stop.${NC}"
            ;;
        "pipelines")
            check_services
            run_pipelines
            echo -e "${GREEN}✓ Pipelines started. Check logs/ directory for output.${NC}"
            ;;
        "monitor")
            monitor_progress
            ;;
        "all"|"")
            check_files
            check_services
            build_project
            create_topics
            stream_data
            sleep 5  # Give producers time to start
            run_pipelines
            sleep 5  # Give pipelines time to start
            monitor_progress
            ;;
        "help")
            echo "Usage: $0 [setup|stream|pipelines|monitor|all|help]"
            echo ""
            echo "Commands:"
            echo "  setup     - Check requirements, build project, create topics"
            echo "  stream    - Start data streaming to Kafka"
            echo "  pipelines - Start RDE pipelines"
            echo "  monitor   - Monitor running pipelines"
            echo "  all       - Run complete pipeline (default)"
            echo "  help      - Show this help"
            ;;
        *)
            echo -e "${RED}Unknown command: $1${NC}"
            echo "Use '$0 help' for usage information."
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"

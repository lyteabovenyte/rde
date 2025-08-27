#!/bin/bash

# Crypto Real-time RDE Pipeline - Real-time blockchain and crypto news data processing
# This script sets up the complete pipeline for:
# 1. Bitcoin market data from blockchain.com APIs
# 2. Crypto news data from cryptopanic.com APIs
# 3. Real-time streaming through Kafka to Iceberg tables
# 4. Analytics-ready data for DataFusion processing

set -e

# Change to project root directory
cd "$(dirname "$0")/.."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${PURPLE}🚀 Crypto Real-time RDE Pipeline${NC}"
echo "================================================================="
echo -e "${CYAN}Setting up real-time blockchain and crypto news data pipeline${NC}"
echo ""

# Configuration
KAFKA_BROKER="localhost:9092"
MINIO_ENDPOINT="http://localhost:9000"

# Define our data sources
declare -a DATA_SOURCES=("bitcoin-market" "crypto-news")

# Start infrastructure
start_infrastructure() {
    echo -e "${YELLOW}🏗️ Starting RDE infrastructure...${NC}"
    
    # Stop any existing containers
    docker-compose down 2>/dev/null || true
    
    # Start the stack
    docker-compose up -d minio kafka zookeeper trino prometheus grafana
    
    echo -e "${GREEN}✓ Infrastructure started${NC}"
    
    # Wait for services
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 15
    
    # Verify services
    for service in "MinIO:9000" "Kafka:9092" "Trino:8080" "Prometheus:9090" "Grafana:3000"; do
        name=$(echo $service | cut -d: -f1)
        port=$(echo $service | cut -d: -f2)
        
        if nc -z localhost $port 2>/dev/null; then
            echo -e "${GREEN}✓ $name is ready${NC}"
        else
            echo -e "${RED}❌ $name failed to start${NC}"
            exit 1
        fi
    done
    
    echo ""
}

# Build the project
build_project() {
    echo -e "${YELLOW}🔨 Building RDE components...${NC}"
    cargo build --release --bin rde-cli
    echo -e "${GREEN}✓ Build completed${NC}"
    echo ""
}

# Create Kafka topics for crypto data sources
create_kafka_topics() {
    echo -e "${YELLOW}📡 Creating Kafka topics for crypto data sources...${NC}"
    
    for source in "${DATA_SOURCES[@]}"; do
        echo -e "${CYAN}  Creating topic: $source${NC}"
        
        # Create topic with appropriate configuration
        docker exec rde-kafka-1 kafka-topics \
            --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 3 \
            --topic "$source" \
            --if-not-exists
        
        echo -e "${GREEN}  ✓ Topic '$source' created${NC}"
    done
    
    echo -e "${GREEN}✓ All Kafka topics created${NC}"
    echo ""
}

# Start the RDE pipeline
start_rde_pipeline() {
    echo -e "${YELLOW}🔄 Starting RDE pipeline...${NC}"
    
    # Run the pipeline in the background
    cargo run --release --bin rde-cli -- \
        -p examples/supreme-pipeline.yml &
    
    local pipeline_pid=$!
    echo $pipeline_pid > "/tmp/rde_pipeline.pid"
    
    echo -e "${GREEN}✓ RDE pipeline started (PID: $pipeline_pid)${NC}"
    echo -e "${CYAN}  Pipeline configuration: examples/supreme-pipeline.yml${NC}"
    echo ""
}

# Start data ingestion services
start_data_ingestion() {
    echo -e "${YELLOW}📥 Starting data ingestion services...${NC}"
    
    # Note: In the real implementation, these would be separate services
    # that connect to blockchain.com and cryptopanic.com APIs
    echo -e "${CYAN}  Bitcoin market data: Ready to ingest from blockchain.com APIs${NC}"
    echo -e "${CYAN}  Crypto news data: Ready to ingest from cryptopanic.com APIs${NC}"
    echo -e "${YELLOW}  ⚠️  Data ingestion services need to be implemented${NC}"
    echo -e "${YELLOW}  ⚠️  See docs/api-integration.md for implementation details${NC}"
    echo ""
}

# Monitor pipeline status
monitor_pipeline() {
    echo -e "${YELLOW}📊 Monitoring pipeline status...${NC}"
    
    # Check if pipeline is running
    if [[ -f "/tmp/rde_pipeline.pid" ]]; then
        local pid=$(cat "/tmp/rde_pipeline.pid")
        if kill -0 $pid 2>/dev/null; then
            echo -e "${GREEN}✓ RDE pipeline is running (PID: $pid)${NC}"
        else
            echo -e "${RED}❌ RDE pipeline is not running${NC}"
        fi
    else
        echo -e "${RED}❌ RDE pipeline PID file not found${NC}"
    fi
    
    # Check Kafka topics
    echo -e "${CYAN}📡 Kafka topics:${NC}"
    for source in "${DATA_SOURCES[@]}"; do
        local count=$(docker exec rde-kafka-1 kafka-get-offsets \
            --bootstrap-server localhost:9092 \
            --topic "$source" \
            --time latest 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}')
        echo -e "${CYAN}  $source: $count messages${NC}"
    done
    
    echo ""
}

# Show next steps
show_next_steps() {
    echo -e "${PURPLE}🎯 Next Steps${NC}"
    echo "================================================================="
    echo -e "${CYAN}1. Implement API integrations:${NC}"
    echo -e "${YELLOW}   - Create Bitcoin market data collector (blockchain.com API)${NC}"
    echo -e "${YELLOW}   - Create crypto news collector (cryptopanic.com API)${NC}"
    echo ""
    echo -e "${CYAN}2. Start data ingestion:${NC}"
    echo -e "${YELLOW}   - Run Bitcoin data collector: ./scripts/start-bitcoin-collector.sh${NC}"
    echo -e "${YELLOW}   - Run news data collector: ./scripts/start-news-collector.sh${NC}"
    echo ""
    echo -e "${CYAN}3. Query your data:${NC}"
    echo -e "${YELLOW}   - Run analytics: ./scripts/query-crypto-data.py${NC}"
    echo -e "${YELLOW}   - Interactive mode: ./scripts/query-crypto-data.py --interactive${NC}"
    echo ""
    echo -e "${CYAN}4. Monitor the pipeline:${NC}"
    echo -e "${YELLOW}   - View logs: tail -f logs/crypto-pipeline.log${NC}"
    echo -e "${YELLOW}   - Check MinIO: http://localhost:9001 (minioadmin/minioadmin)${NC}"
    echo -e "${YELLOW}   - Query data with Trino: http://localhost:8080${NC}"
    echo -e "${YELLOW}   - View metrics: http://localhost:9090 (Prometheus)${NC}"
    echo -e "${YELLOW}   - View dashboards: http://localhost:3000 (admin/admin)${NC}"
    echo ""
}

# Cleanup function
cleanup() {
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"
    
    # Stop pipeline
    if [[ -f "/tmp/rde_pipeline.pid" ]]; then
        local pid=$(cat "/tmp/rde_pipeline.pid")
        kill $pid 2>/dev/null || true
        rm -f "/tmp/rde_pipeline.pid"
    fi
    
    # Stop infrastructure
    docker-compose down 2>/dev/null || true
    
    echo -e "${GREEN}✓ Cleanup completed${NC}"
}

# Main execution
main() {
    echo -e "${BLUE}🚀 Starting Crypto Real-time RDE Pipeline${NC}"
    echo ""
    
    # Set up signal handlers
    trap cleanup EXIT
    trap 'echo -e "\n${RED}⚠️  Interrupted${NC}"; cleanup; exit 1' INT TERM
    
    # Execute pipeline steps
    start_infrastructure
    build_project
    create_kafka_topics
    start_rde_pipeline
    start_data_ingestion
    
    echo -e "${GREEN}✅ Pipeline setup completed!${NC}"
    echo ""
    
    # Monitor for a bit
    monitor_pipeline
    
    # Show next steps
    show_next_steps
    
    echo -e "${PURPLE}🎉 Crypto Real-time RDE Pipeline is ready!${NC}"
    echo -e "${CYAN}Press Ctrl+C to stop the pipeline${NC}"
    
    # Keep running until interrupted
    while true; do
        sleep 30
        monitor_pipeline
    done
}

# Run main function
main "$@"

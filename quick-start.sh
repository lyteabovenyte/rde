#!/bin/bash

# Quick start script for RDE Pipeline with your JSON datasets

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 RDE Pipeline Quick Start${NC}"
echo "==========================="
echo ""
echo -e "${YELLOW}This script will set up and run the complete data pipeline:${NC}"
echo -e "${BLUE}📊 flights.json   → Kafka → flights_data Iceberg table${NC}"
echo -e "${BLUE}📊 retail.json    → Kafka → retail_products Iceberg table${NC}"
echo -e "${BLUE}📊 spotify.json   → Kafka → spotify_audio_features Iceberg table${NC}"
echo ""

# Check if JSON files exist
echo -e "${YELLOW}🔍 Checking your JSON files...${NC}"
if [[ ! -f "data/json-samples/flights.json" ]]; then
    echo -e "${RED}❌ data/json-samples/flights.json not found${NC}"
    exit 1
fi
if [[ ! -f "data/json-samples/retail.json" ]]; then
    echo -e "${RED}❌ data/json-samples/retail.json not found${NC}"
    exit 1
fi
if [[ ! -f "data/json-samples/spotify.json" ]]; then
    echo -e "${RED}❌ data/json-samples/spotify.json not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ All JSON files found${NC}"

# Show file sizes
echo -e "${CYAN}📏 Dataset sizes:${NC}"
echo -e "  flights.json: $(wc -l < data/json-samples/flights.json | xargs) records"
echo -e "  retail.json: $(wc -l < data/json-samples/retail.json | xargs) records"
echo -e "  spotify.json: $(wc -l < data/json-samples/spotify.json | xargs) lines"
echo ""

# Ask user what they want to do
echo -e "${YELLOW}What would you like to do?${NC}"
echo "1) 🧪 Quick Test (1000 records from each dataset, ~2 minutes)"
echo "2) 🚀 Full Pipeline (all data, ~20-30 minutes)"
echo "3) 📊 Monitor existing pipelines"
echo "4) 🛠️  Setup only (infrastructure + topics)"
echo "0) 🚪 Exit"
echo ""
read -p "Enter your choice: " choice

case $choice in
    1)
        echo -e "${GREEN}🧪 Starting Quick Test Mode${NC}"
        echo ""
        
        # Check if infrastructure is running
        echo -e "${YELLOW}📋 Checking infrastructure...${NC}"
        if ! nc -z localhost 9092 2>/dev/null; then
            echo -e "${RED}❌ Kafka not running. Please run: docker-compose -f docker/docker-compose.yml up -d${NC}"
            exit 1
        fi
        if ! nc -z localhost 9000 2>/dev/null; then
            echo -e "${RED}❌ MinIO not running. Please run: ./scripts/start-minio.sh${NC}"
            exit 1
        fi
        echo -e "${GREEN}✓ Infrastructure ready${NC}"
        
        # Run quick test
        echo -e "${YELLOW}🔨 Building project...${NC}"
        cargo build --release --bin rde-cli --bin kafka-producer > /dev/null 2>&1
        
        echo -e "${YELLOW}📡 Creating topics and streaming test data...${NC}"
        ./scripts/stream-individual-datasets.sh test > /dev/null 2>&1 &
        STREAM_PID=$!
        
        # Wait a moment for data to start flowing
        sleep 5
        
        echo -e "${YELLOW}🔄 Starting pipelines...${NC}"
        ./scripts/setup-and-run-pipelines.sh pipelines > /dev/null 2>&1
        
        echo -e "${GREEN}✅ Quick test started!${NC}"
        echo ""
        echo -e "${CYAN}🔍 Monitor progress:${NC}"
        echo -e "  ./scripts/monitor-pipeline-health.sh"
        echo ""
        echo -e "${CYAN}🌐 Check results in MinIO:${NC}"
        echo -e "  http://localhost:9001 (minioadmin/minioadmin)"
        echo ""
        echo -e "${CYAN}📊 View Kafka topics:${NC}"
        echo -e "  docker exec rde-kafka-1 kafka-topics --list --bootstrap-server localhost:9092"
        echo ""
        ;;
        
    2)
        echo -e "${GREEN}🚀 Starting Full Pipeline${NC}"
        echo -e "${RED}⚠️  This will process millions of records and take 20-30 minutes!${NC}"
        echo ""
        read -p "Continue? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Cancelled."
            exit 0
        fi
        
        echo -e "${YELLOW}🎯 Running complete pipeline...${NC}"
        ./scripts/setup-and-run-pipelines.sh all
        ;;
        
    3)
        echo -e "${GREEN}📊 Starting Pipeline Monitor${NC}"
        ./scripts/monitor-pipeline-health.sh
        ;;
        
    4)
        echo -e "${GREEN}🛠️  Setup Mode${NC}"
        ./scripts/setup-and-run-pipelines.sh setup
        echo ""
        echo -e "${CYAN}📝 Next steps:${NC}"
        echo -e "  1. Stream data: ./scripts/stream-individual-datasets.sh"
        echo -e "  2. Run pipelines: ./scripts/setup-and-run-pipelines.sh pipelines"
        echo -e "  3. Monitor: ./scripts/monitor-pipeline-health.sh"
        ;;
        
    0)
        echo -e "${GREEN}👋 Goodbye!${NC}"
        exit 0
        ;;
        
    *)
        echo -e "${RED}❌ Invalid choice${NC}"
        exit 1
        ;;
esac

#!/bin/bash

# Supreme RDE Pipeline - Auto-discovery and processing of all JSON datasets
# This script discovers JSON files in data/json-samples/ and automatically:
# 1. Creates Kafka topics for each dataset
# 2. Streams data to appropriate topics  
# 3. Runs RDE pipelines to process data into Iceberg tables
# 4. Sets up everything for SQL analytics

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

echo -e "${PURPLE}🚀 Supreme RDE Pipeline - Auto-Discovery Mode${NC}"
echo "================================================================="
echo -e "${CYAN}Auto-discovering JSON datasets and setting up complete pipeline${NC}"
echo ""

# Configuration
DATA_DIR="data/json-samples"
KAFKA_BROKER="localhost:9092"
MINIO_ENDPOINT="http://localhost:9000"

# Auto-discover JSON datasets
discover_datasets() {
    echo -e "${YELLOW}🔍 Auto-discovering JSON datasets...${NC}"
    
    DATASETS=()
    for file in "$DATA_DIR"/*.json; do
        if [[ -f "$file" ]]; then
            dataset_name=$(basename "$file" .json)
            DATASETS+=("$dataset_name")
            echo -e "${GREEN}✓ Found dataset: $dataset_name${NC}"
        fi
    done
    
    if [[ ${#DATASETS[@]} -eq 0 ]]; then
        echo -e "${RED}❌ No JSON datasets found in $DATA_DIR${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}📊 Discovered ${#DATASETS[@]} datasets: ${DATASETS[*]}${NC}"
    echo ""
}

# Start infrastructure
start_infrastructure() {
    echo -e "${YELLOW}🏗️ Starting RDE infrastructure...${NC}"
    
    # Stop any existing containers
    docker-compose down 2>/dev/null || true
    
    # Start the stack (without DuckDB for now)
    docker-compose up -d minio kafka zookeeper
    
    echo -e "${GREEN}✓ Infrastructure started${NC}"
    
    # Wait for services
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 15
    
    # Verify services
    for service in "MinIO:9000" "Kafka:9092"; do
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
    cargo build --release --bin rde-cli --bin kafka-producer
    echo -e "${GREEN}✓ Build completed${NC}"
    echo ""
}

# Create Kafka topics for all datasets
create_kafka_topics() {
    echo -e "${YELLOW}📡 Creating Kafka topics for all datasets...${NC}"
    
    for dataset in "${DATASETS[@]}"; do
        echo -e "${BLUE}Creating topic: $dataset${NC}"
        docker exec docker-kafka-1 kafka-topics \
            --create \
            --topic "$dataset" \
            --bootstrap-server localhost:9092 \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists 2>/dev/null || true
    done
    
    echo -e "${GREEN}✓ All Kafka topics created${NC}"
    echo ""
}

# Stream data for a specific dataset
stream_dataset() {
    local dataset=$1
    local file="$DATA_DIR/$dataset.json"
    
    echo -e "${BLUE}📤 Streaming $dataset data...${NC}"
    
    # Determine the format and handle accordingly
    # TODO: this is a placeholder for now, should be updated later for each
    # specific dataset for users
    if [[ "$dataset" == "spotify" ]]; then
        # Spotify has nested structure: extract audio_features array
        echo -e "${CYAN}  📝 Processing nested Spotify JSON...${NC}"
        jq -c '.audio_features[]' "$file" > "/tmp/${dataset}_stream.ndjson"
        
    elif [[ "$dataset" == "retail" ]]; then
        # Retail is a JSON array: convert to NDJSON
        echo -e "${CYAN}  📝 Processing JSON array format...${NC}"
        jq -c '.[]' "$file" > "/tmp/${dataset}_stream.ndjson"
        
    elif [[ "$dataset" == "flights" ]]; then
        # Flights is already NDJSON: use directly
        echo -e "${CYAN}  📝 Using NDJSON format directly...${NC}"
        cp "$file" "/tmp/${dataset}_stream.ndjson"
        
    else
        # Default: try to detect format automatically
        echo -e "${CYAN}  📝 Auto-detecting format...${NC}"
        first_char=$(head -c 1 "$file")
        if [[ "$first_char" == "[" ]]; then
            # JSON array
            jq -c '.[]' "$file" > "/tmp/${dataset}_stream.ndjson"
        else
            # Assume NDJSON
            cp "$file" "/tmp/${dataset}_stream.ndjson"
        fi
    fi
    
    # Count records
    record_count=$(wc -l < "/tmp/${dataset}_stream.ndjson")
    echo -e "${CYAN}  📊 $record_count records to stream${NC}"
    
    # Stream to Kafka
    cargo run --release --bin kafka-producer -- \
        --input "/tmp/${dataset}_stream.ndjson" \
        --topic "$dataset" \
        --format ndjson \
        --batch-size 100 \
        --delay-ms 100 \
        --progress-interval 1000 &
    
    local producer_pid=$!
    echo $producer_pid > "/tmp/${dataset}_producer.pid"
    
    echo -e "${GREEN}✓ Started streaming $dataset (PID: $producer_pid)${NC}"
}

# Generate pipeline config for a dataset
generate_pipeline_config() {
    local dataset=$1
    local config_file="/tmp/${dataset}-pipeline.yml"
    
    cat > "$config_file" << EOF
name: "${dataset}-auto-pipeline"
sources:
  - type: kafka
    id: "${dataset}-source"
    brokers: "$KAFKA_BROKER"
    group_id: "rde-${dataset}-group"
    topic: "$dataset"
    schema:
      auto_infer: true

transforms:
  - type: clean_data
    id: "clean-${dataset}"
    remove_nulls: true
    trim_strings: true

sinks:
  - type: iceberg
    id: "${dataset}-sink"
    table_name: "$dataset"
    bucket: "$dataset"
    endpoint: "$MINIO_ENDPOINT"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    region: "us-east-1"

edges:
  - ["${dataset}-source", "clean-${dataset}"]
  - ["clean-${dataset}", "${dataset}-sink"]
EOF
    
    echo "$config_file"
}

# Run RDE pipeline for a dataset
run_dataset_pipeline() {
    local dataset=$1
    local config_file=$(generate_pipeline_config "$dataset")
    
    echo -e "${BLUE}🔄 Starting RDE pipeline for $dataset...${NC}"
    
    # Create logs directory
    mkdir -p logs
    
    # Start pipeline in background
    RUST_LOG=info cargo run --release --bin rde-cli -- \
        --pipeline "$config_file" > "logs/${dataset}-pipeline.log" 2>&1 &
    
    local pipeline_pid=$!
    echo $pipeline_pid > "/tmp/${dataset}_pipeline.pid"
    
    echo -e "${GREEN}✓ Started $dataset pipeline (PID: $pipeline_pid)${NC}"
}

# Stream all datasets
stream_all_datasets() {
    echo -e "${YELLOW}📊 Starting data streaming for all datasets...${NC}"
    
    for dataset in "${DATASETS[@]}"; do
        stream_dataset "$dataset"
        sleep 2  # Brief pause between datasets
    done
    
    echo -e "${GREEN}✓ All data streaming started${NC}"
    echo ""
}

# Run all pipelines
run_all_pipelines() {
    echo -e "${YELLOW}🔄 Starting RDE pipelines for all datasets...${NC}"
    
    # Give producers time to start sending data
    sleep 5
    
    for dataset in "${DATASETS[@]}"; do
        run_dataset_pipeline "$dataset"
        sleep 2  # Brief pause between pipelines
    done
    
    echo -e "${GREEN}✓ All pipelines started${NC}"
    echo ""
}

# Monitor progress
monitor_progress() {
    echo -e "${YELLOW}📊 Monitoring pipeline progress...${NC}"
    echo -e "${CYAN}Press Ctrl+C to stop monitoring and view summary${NC}"
    echo ""
    
    local iteration=0
    while true; do
        iteration=$((iteration + 1))
        echo -e "${PURPLE}=== Pipeline Status (Check #$iteration) ===${NC}"
        
        for dataset in "${DATASETS[@]}"; do
            # Check producer status
            if [[ -f "/tmp/${dataset}_producer.pid" ]]; then
                local producer_pid=$(cat "/tmp/${dataset}_producer.pid")
                if kill -0 "$producer_pid" 2>/dev/null; then
                    echo -e "${GREEN}🟢 $dataset producer: RUNNING${NC}"
                else
                    echo -e "${YELLOW}🟡 $dataset producer: COMPLETED${NC}"
                fi
            fi
            
            # Check Kafka message count
            local count=$(docker exec docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:9092 \
                --topic "$dataset" --time -1 2>/dev/null | \
                awk -F ':' '{sum += $3} END {print sum}' || echo "0")
            echo -e "${BLUE}  📈 $dataset messages: $count${NC}"
            
            # Check MinIO data
            if docker exec docker-minio-1 mc ls "local/$dataset/" >/dev/null 2>&1; then
                local files=$(docker exec docker-minio-1 mc ls "local/$dataset/" 2>/dev/null | wc -l || echo "0")
                echo -e "${CYAN}  🗄️  $dataset MinIO files: $files${NC}"
            fi
        done
        
        echo ""
        sleep 10
    done
}

# Generate DuckDB query templates
generate_sql_templates() {
    echo -e "${YELLOW}📝 Generating DuckDB query templates...${NC}"
    
    mkdir -p sql/templates
    
    for dataset in "${DATASETS[@]}"; do
        cat > "sql/templates/${dataset}_duckdb_queries.sql" << EOF
-- $(echo "${dataset}" | sed 's/./\U&/') Dataset Analysis Queries
-- Auto-generated SQL templates for ${dataset} data

-- 1. Basic data overview
SELECT COUNT(*) as total_records
FROM 's3://${dataset}/**/*.parquet';

-- 2. Sample data preview  
SELECT *
FROM 's3://${dataset}/**/*.parquet'
LIMIT 10;

-- 3. Schema information
DESCRIBE SELECT * FROM 's3://${dataset}/**/*.parquet';

-- 4. Column analysis
SELECT * FROM (
    DESCRIBE SELECT * FROM 's3://${dataset}/**/*.parquet'
) LIMIT 20;

-- TODO: Add your custom ${dataset} analysis queries below
-- Examples:
-- - Time-based analysis
-- - Aggregations and grouping
-- - Business metrics calculations
-- - Data trends and patterns

EOF
        echo -e "${GREEN}✓ Generated DuckDB template: sql/templates/${dataset}_duckdb_queries.sql${NC}"
    done
    
    # Create master analytics file
    cat > "sql/templates/analytics_master.sql" << EOF
-- Master Analytics Queries
-- Cross-dataset analysis and reporting

-- Available datasets: ${DATASETS[*]}

-- 1. Dataset summary
$(for dataset in "${DATASETS[@]}"; do
    echo "SELECT '$dataset' as dataset, COUNT(*) as record_count FROM 's3://$dataset/**/*.parquet'"
    if [[ "$dataset" != "${DATASETS[-1]}" ]]; then
        echo "UNION ALL"
    fi
done);

-- 2. Sample data from each dataset
$(for dataset in "${DATASETS[@]}"; do
    echo "-- Sample from $dataset:"
    echo "SELECT '$dataset' as source, * FROM 's3://$dataset/**/*.parquet' LIMIT 3;"
    echo ""
done)

-- TODO: Add cross-dataset join queries and advanced analytics

EOF
    
    echo -e "${GREEN}✓ Generated master analytics: sql/templates/analytics_master.sql${NC}"
    echo ""
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up processes...${NC}"
    
    for dataset in "${DATASETS[@]}"; do
        # Kill producers
        if [[ -f "/tmp/${dataset}_producer.pid" ]]; then
            local pid=$(cat "/tmp/${dataset}_producer.pid")
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "${BLUE}Stopping $dataset producer (PID: $pid)${NC}"
                kill "$pid" 2>/dev/null || true
            fi
            rm -f "/tmp/${dataset}_producer.pid"
        fi
        
        # Kill pipelines
        if [[ -f "/tmp/${dataset}_pipeline.pid" ]]; then
            local pid=$(cat "/tmp/${dataset}_pipeline.pid")
            if kill -0 "$pid" 2>/dev/null; then
                echo -e "${BLUE}Stopping $dataset pipeline (PID: $pid)${NC}"
                kill "$pid" 2>/dev/null || true
            fi
            rm -f "/tmp/${dataset}_pipeline.pid"
        fi
        
        # Clean temp files
        rm -f "/tmp/${dataset}_stream.ndjson"
        rm -f "/tmp/${dataset}-pipeline.yml"
    done
    
    echo -e "${GREEN}✓ Cleanup completed${NC}"
}

# Generate final summary
show_summary() {
    echo -e "\n${PURPLE}🎉 Supreme RDE Pipeline - Execution Summary${NC}"
    echo "=============================================="
    
    echo -e "${YELLOW}📊 Processed Datasets:${NC}"
    for dataset in "${DATASETS[@]}"; do
        echo -e "${GREEN}  ✓ $dataset${NC}"
        
        # Check final message count
        local count=$(docker exec docker-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
            --broker-list localhost:9092 \
            --topic "$dataset" --time -1 2>/dev/null | \
            awk -F ':' '{sum += $3} END {print sum}' || echo "0")
        echo -e "${BLUE}    📈 Messages processed: $count${NC}"
        
        # Check data in MinIO
        if docker exec docker-minio-1 mc ls "local/$dataset/" >/dev/null 2>&1; then
            echo -e "${CYAN}    🗄️  Data stored in MinIO bucket: $dataset${NC}"
        fi
    done
    
    echo ""
    echo -e "${YELLOW}🔗 Next Steps:${NC}"
    echo -e "${GREEN}  1. View generated SQL templates in sql/templates/${NC}"
    echo -e "${GREEN}  2. Query your data: ./scripts/query-data.py${NC}"
    echo -e "${GREEN}  3. Access MinIO console: http://localhost:9001 (minioadmin/minioadmin)${NC}"
    echo -e "${GREEN}  4. Interactive SQL: ./scripts/query-data.py --interactive${NC}"
    echo ""
    echo -e "${PURPLE}🚀 Your data engineering pipeline is ready for DuckDB analytics!${NC}"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Main execution
main() {
    discover_datasets
    start_infrastructure
    build_project
    create_kafka_topics
    stream_all_datasets
    run_all_pipelines
    generate_sql_templates
    
    echo -e "${GREEN}🚀 All systems operational! Starting monitoring...${NC}"
    echo -e "${CYAN}💡 Tip: Check logs/ directory for detailed pipeline logs${NC}"
    echo ""
    
    # Monitor for a bit, then show summary
    timeout 60 monitor_progress || true
    
    show_summary
}

# Run main function
main "$@"

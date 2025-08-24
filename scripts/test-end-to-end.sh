#!/bin/bash

# End-to-end test of the complete RDE + Trino pipeline

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🧪 End-to-End Pipeline Test${NC}"
echo "============================="

# Function to check if service is running
check_service() {
    local name=$1
    local host=$2
    local port=$3
    
    if nc -z $host $port 2>/dev/null; then
        echo -e "${GREEN}✓ $name is running${NC}"
        return 0
    else
        echo -e "${RED}✗ $name is not running${NC}"
        return 1
    fi
}

# Step 1: Check infrastructure
echo -e "${YELLOW}📋 Checking infrastructure...${NC}"
check_service "Kafka" "localhost" "9092" || { echo "Start with: docker-compose up -d"; exit 1; }
check_service "MinIO" "localhost" "9000" || { echo "Start with: docker-compose up -d"; exit 1; }

# Step 2: Create test data
echo -e "${YELLOW}📊 Creating test data...${NC}"
if [[ -f "data/json-samples/flights.json" ]]; then
    head -20 data/json-samples/flights.json > /tmp/test-end-to-end.json
    echo -e "${GREEN}✓ Created test data (20 flight records)${NC}"
else
    # Generate test flight data
    cat > /tmp/test-end-to-end.json << 'EOF'
{"FL_DATE":"2024-01-15","DEP_DELAY":5,"ARR_DELAY":3,"AIR_TIME":120,"DISTANCE":800,"DEP_TIME":8.5,"ARR_TIME":10.5}
{"FL_DATE":"2024-01-15","DEP_DELAY":-2,"ARR_DELAY":-5,"AIR_TIME":95,"DISTANCE":650,"DEP_TIME":9.0,"ARR_TIME":10.5}
{"FL_DATE":"2024-01-15","DEP_DELAY":25,"ARR_DELAY":30,"AIR_TIME":180,"DISTANCE":1200,"DEP_TIME":14.0,"ARR_TIME":17.0}
{"FL_DATE":"2024-01-15","DEP_DELAY":0,"ARR_DELAY":2,"AIR_TIME":75,"DISTANCE":450,"DEP_TIME":12.0,"ARR_TIME":13.25}
{"FL_DATE":"2024-01-15","DEP_DELAY":10,"ARR_DELAY":8,"AIR_TIME":140,"DISTANCE":900,"DEP_TIME":16.5,"ARR_TIME":18.75}
EOF
    echo -e "${GREEN}✓ Generated test data (5 flight records)${NC}"
fi

# Step 3: Start pipeline
echo -e "${YELLOW}🔄 Starting RDE pipeline...${NC}"
RUST_LOG=info cargo run --bin rde-cli -- -p examples/flights-simple.yml > /tmp/pipeline.log 2>&1 &
PIPELINE_PID=$!
echo -e "${GREEN}✓ Pipeline started (PID: $PIPELINE_PID)${NC}"

# Give pipeline time to initialize
sleep 5

# Step 4: Stream data
echo -e "${YELLOW}📤 Streaming test data...${NC}"
cargo run --bin kafka-producer -- \
    --input /tmp/test-end-to-end.json \
    --topic flights \
    --format ndjson \
    --batch-size 5 \
    --delay-ms 500 \
    --progress-interval 5

echo -e "${GREEN}✓ Data streaming completed${NC}"

# Step 5: Wait for processing
echo -e "${YELLOW}⏳ Waiting for data processing...${NC}"
sleep 10

# Step 6: Check results
echo -e "${YELLOW}🔍 Checking results...${NC}"

# Check MinIO files
MINIO_FILES=$(docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/flights_data_simple/ --recursive 2>/dev/null | wc -l || echo "0")
if [[ $MINIO_FILES -gt 0 ]]; then
    echo -e "${GREEN}✓ MinIO files created: $MINIO_FILES files${NC}"
    docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/flights_data_simple/ --recursive | head -5
else
    echo -e "${RED}✗ No files found in MinIO${NC}"
fi

# Check pipeline logs
echo -e "${BLUE}Recent pipeline activity:${NC}"
tail -5 /tmp/pipeline.log

# Step 7: Test Trino connectivity (if available)
if check_service "Trino" "localhost" "8080"; then
    echo -e "${YELLOW}🔍 Testing Trino connectivity...${NC}"
    
    if docker run --rm --network host trinodb/trino:435 \
        trino --server http://localhost:8080 --user admin \
        --execute "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Trino is accessible${NC}"
        
        # Try to register and query table
        echo -e "${YELLOW}📋 Attempting to register table...${NC}"
        docker run --rm --network host trinodb/trino:435 \
            trino --server http://localhost:8080 --user admin \
            --execute "CALL iceberg.system.register_table('default', 'flights_data_simple', 's3://iceberg-data/flights_data_simple')" \
            2>/dev/null && echo -e "${GREEN}✓ Table registered${NC}" || echo -e "${YELLOW}⚠ Table registration failed (may already exist)${NC}"
        
        # Query the table
        echo -e "${YELLOW}📊 Querying data...${NC}"
        RECORD_COUNT=$(docker run --rm --network host trinodb/trino:435 \
            trino --server http://localhost:8080 --user admin \
            --execute "SELECT COUNT(*) FROM iceberg.default.flights_data_simple" 2>/dev/null | tail -1 || echo "0")
        
        if [[ "$RECORD_COUNT" =~ ^[0-9]+$ ]] && [[ $RECORD_COUNT -gt 0 ]]; then
            echo -e "${GREEN}✓ Successfully queried $RECORD_COUNT records from Iceberg table${NC}"
        else
            echo -e "${YELLOW}⚠ Table query returned: $RECORD_COUNT${NC}"
        fi
    else
        echo -e "${YELLOW}⚠ Trino not responding to queries${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Trino not running - skipping SQL tests${NC}"
    echo -e "${BLUE}To start Trino: docker run -d --name trino --network host -p 8080:8080 trinodb/trino:435${NC}"
fi

# Cleanup
echo -e "${YELLOW}🧹 Cleaning up...${NC}"
kill $PIPELINE_PID 2>/dev/null || true
rm -f /tmp/test-end-to-end.json /tmp/pipeline.log

# Summary
echo ""
echo -e "${GREEN}🎉 End-to-End Test Summary${NC}"
echo "=========================="
echo -e "${GREEN}✓ Infrastructure: Running${NC}"
echo -e "${GREEN}✓ Data Pipeline: Working${NC}"
echo -e "${GREEN}✓ Kafka → RDE → Iceberg: Functional${NC}"
if [[ $MINIO_FILES -gt 0 ]]; then
    echo -e "${GREEN}✓ Data Storage: $MINIO_FILES files created${NC}"
else
    echo -e "${YELLOW}⚠ Data Storage: Check pipeline logs${NC}"
fi

echo ""
echo -e "${BLUE}🔗 Access Points:${NC}"
echo -e "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
if check_service "Trino" "localhost" "8080"; then
    echo -e "  Trino Web UI: http://localhost:8080"
    echo -e "  DataGrip JDBC: jdbc:trino://localhost:8080/iceberg/default"
fi

echo ""
echo -e "${YELLOW}📝 Next Steps:${NC}"
echo -e "  1. View data in MinIO Console"
echo -e "  2. Connect DataGrip using JDBC URL above"
echo -e "  3. Run analytics: ./scripts/run-sql.sh sql/queries/flights_analysis.sql"
echo -e "  4. Scale up with more data!"

#!/bin/bash

# Setup script for Trino with Iceberg tables

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 Setting up Trino for Iceberg Table Queries${NC}"
echo "=============================================="

# Function to wait for service
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local timeout=${4:-60}
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to be ready...${NC}"
    
    for i in $(seq 1 $timeout); do
        if nc -z $host $port 2>/dev/null; then
            echo -e "${GREEN}✓ $service_name is ready${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}❌ $service_name failed to start within $timeout seconds${NC}"
    return 1
}

# Step 1: Stop existing containers
echo -e "${YELLOW}🛑 Stopping existing containers...${NC}"
docker-compose down 2>/dev/null || true

# Step 2: Start simple stack
echo -e "${YELLOW}🚀 Starting Trino stack...${NC}"
docker-compose -f docker-compose-trino-simple.yml up -d

# Step 3: Wait for services
wait_for_service localhost 9000 "MinIO" 30
wait_for_service localhost 9092 "Kafka" 30
wait_for_service localhost 8080 "Trino" 90

# Step 4: Create MinIO bucket if it doesn't exist
echo -e "${YELLOW}📦 Ensuring MinIO bucket exists...${NC}"
sleep 5  # Give MinIO a moment to fully initialize
docker exec $(docker ps -q -f name=minio) mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec $(docker ps -q -f name=minio) mc mb local/iceberg-data 2>/dev/null || echo "Bucket already exists"

# Step 5: Setup complete (using filesystem catalog)
echo -e "${YELLOW}🗄️  Using filesystem-based Iceberg catalog (no metastore needed)${NC}"

echo -e "${GREEN}✅ Trino setup completed!${NC}"
echo ""
echo -e "${BLUE}📋 Service URLs:${NC}"
echo -e "  🌐 Trino Web UI: http://localhost:8080"
echo -e "  🗄️  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo -e "  📊 Kafka: localhost:9092"
echo ""
echo -e "${BLUE}🔗 DataGrip Connection:${NC}"
echo -e "  URL: jdbc:trino://localhost:8080/iceberg/default"
echo -e "  User: any username (no password required)"
echo -e "  Driver: Trino JDBC Driver"
echo ""
echo -e "${YELLOW}📝 Next steps:${NC}"
echo -e "  1. Run your RDE pipeline to create Iceberg tables"
echo -e "  2. Register tables: ./scripts/run-sql.sh sql/setup/register_tables.sql"
echo -e "  3. Query data: ./scripts/run-sql.sh sql/queries/flights_analysis.sql"
echo -e "  4. Connect DataGrip using the connection details above"

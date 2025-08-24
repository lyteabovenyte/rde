#!/bin/bash

# Health check script for the complete Trino + Iceberg stack

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🏥 RDE + Trino Stack Health Check${NC}"
echo "=================================="

# Function to check service
check_service() {
    local name=$1
    local host=$2
    local port=$3
    local endpoint=${4:-""}
    
    echo -n -e "${YELLOW}Checking $name ($host:$port)... ${NC}"
    
    if nc -z $host $port 2>/dev/null; then
        if [[ -n "$endpoint" ]]; then
            if curl -s -f "$endpoint" >/dev/null 2>&1; then
                echo -e "${GREEN}✓ HEALTHY${NC}"
            else
                echo -e "${YELLOW}⚠ PORT OK, ENDPOINT FAILED${NC}"
            fi
        else
            echo -e "${GREEN}✓ HEALTHY${NC}"
        fi
    else
        echo -e "${RED}✗ UNHEALTHY${NC}"
    fi
}

# Function to check Trino queries
check_trino_queries() {
    echo -e "${YELLOW}🔍 Testing Trino SQL queries...${NC}"
    
    # Basic connectivity test
    if docker run --rm --network host trinodb/trino:435 \
        trino --server http://localhost:8080 --user admin \
        --execute "SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Trino SQL connectivity works${NC}"
    else
        echo -e "${RED}✗ Trino SQL connectivity failed${NC}"
        return 1
    fi
    
    # Test catalog access
    if docker run --rm --network host trinodb/trino:435 \
        trino --server http://localhost:8080 --user admin \
        --execute "SHOW CATALOGS" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Trino catalogs accessible${NC}"
    else
        echo -e "${RED}✗ Trino catalogs not accessible${NC}"
        return 1
    fi
    
    # Test Iceberg catalog
    if docker run --rm --network host trinodb/trino:435 \
        trino --server http://localhost:8080 --user admin \
        --execute "SHOW SCHEMAS FROM iceberg" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Iceberg catalog works${NC}"
    else
        echo -e "${YELLOW}⚠ Iceberg catalog not ready (may need table registration)${NC}"
    fi
}

# Function to check MinIO bucket
check_minio_bucket() {
    echo -e "${YELLOW}📦 Checking MinIO bucket...${NC}"
    
    if docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/ >/dev/null 2>&1; then
        local file_count=$(docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/ --recursive 2>/dev/null | wc -l)
        echo -e "${GREEN}✓ MinIO bucket accessible ($file_count files)${NC}"
    else
        echo -e "${RED}✗ MinIO bucket not accessible${NC}"
    fi
}

# Function to show container status
show_container_status() {
    echo -e "${BLUE}🐳 Container Status:${NC}"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(kafka|minio|trino|postgres|hive)" || echo "No relevant containers found"
}

# Function to show service URLs
show_service_urls() {
    echo -e "${BLUE}🌐 Service URLs:${NC}"
    echo -e "  Trino Web UI: http://localhost:8080"
    echo -e "  MinIO Console: http://localhost:9001"
    echo -e "  Kafka: localhost:9092"
    echo -e "  Hive Metastore: localhost:9083"
}

# Main health checks
echo -e "${BLUE}🔍 Checking core services...${NC}"
check_service "Kafka" "localhost" "9092"
check_service "MinIO" "localhost" "9000" "http://localhost:9000/minio/health/live"
check_service "Hive Metastore" "localhost" "9083"
check_service "Trino" "localhost" "8080" "http://localhost:8080/v1/status"

echo ""
check_minio_bucket
echo ""
check_trino_queries
echo ""
show_container_status
echo ""
show_service_urls

echo ""
echo -e "${GREEN}🎉 Health check completed!${NC}"
echo ""
echo -e "${YELLOW}📝 Quick commands:${NC}"
echo -e "  Interactive SQL: ./scripts/trino-cli.sh"
echo -e "  Run queries: ./scripts/run-sql.sh sql/queries/flights_analysis.sql"
echo -e "  Register tables: ./scripts/run-sql.sh sql/setup/register_tables.sql"

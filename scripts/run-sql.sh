#!/bin/bash

# Script to run SQL queries against Trino

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8080}
TRINO_USER=${TRINO_USER:-admin}

if [[ $# -eq 0 ]]; then
    echo -e "${RED}Usage: $0 <sql-file> [additional-sql-files...]${NC}"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 sql/setup/register_tables.sql"
    echo -e "  $0 sql/queries/flights_analysis.sql"
    echo -e "  $0 'SELECT COUNT(*) FROM iceberg.default.flights_data_simple'"
    echo ""
    echo -e "${BLUE}Available SQL files:${NC}"
    find sql/ -name "*.sql" 2>/dev/null | sort
    exit 1
fi

# Function to check if Trino is available
check_trino() {
    if ! nc -z $TRINO_HOST $TRINO_PORT 2>/dev/null; then
        echo -e "${RED}❌ Trino is not running on $TRINO_HOST:$TRINO_PORT${NC}"
        echo -e "${YELLOW}Start Trino with: ./scripts/setup-trino.sh${NC}"
        exit 1
    fi
}

# Function to run SQL file or statement
run_sql() {
    local sql_input="$1"
    
    if [[ -f "$sql_input" ]]; then
        echo -e "${YELLOW}📄 Running SQL file: $sql_input${NC}"
        echo -e "${BLUE}Contents:${NC}"
        echo "$(head -5 "$sql_input")..."
        echo ""
        
        # Use Trino CLI to execute the file
        docker run --rm --network host \
            -v "$(pwd):/workspace" \
            trinodb/trino:435 \
            trino --server http://$TRINO_HOST:$TRINO_PORT \
                  --user $TRINO_USER \
                  --file "/workspace/$sql_input"
    else
        # Treat as SQL statement
        echo -e "${YELLOW}💻 Running SQL statement: $sql_input${NC}"
        
        docker run --rm --network host \
            trinodb/trino:435 \
            trino --server http://$TRINO_HOST:$TRINO_PORT \
                  --user $TRINO_USER \
                  --execute "$sql_input"
    fi
}

# Check Trino availability
echo -e "${BLUE}🔍 Checking Trino connection...${NC}"
check_trino
echo -e "${GREEN}✓ Trino is available${NC}"
echo ""

# Run each SQL file/statement
for sql_input in "$@"; do
    echo -e "${GREEN}🚀 Executing: $sql_input${NC}"
    echo "=================================================="
    
    if run_sql "$sql_input"; then
        echo -e "${GREEN}✅ Successfully executed: $sql_input${NC}"
    else
        echo -e "${RED}❌ Failed to execute: $sql_input${NC}"
        exit 1
    fi
    
    echo ""
    echo "=================================================="
    echo ""
done

echo -e "${GREEN}🎉 All SQL executions completed!${NC}"

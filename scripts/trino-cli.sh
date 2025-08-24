#!/bin/bash

# Interactive Trino CLI session

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

TRINO_HOST=${TRINO_HOST:-localhost}
TRINO_PORT=${TRINO_PORT:-8080}
TRINO_USER=${TRINO_USER:-admin}

echo -e "${GREEN}🔗 Starting interactive Trino CLI session${NC}"
echo -e "${BLUE}Connection: trino://$TRINO_HOST:$TRINO_PORT${NC}"
echo -e "${YELLOW}Type 'exit' or Ctrl+D to quit${NC}"
echo ""

# Start interactive Trino CLI
docker run --rm -it --network host \
    trinodb/trino:435 \
    trino --server http://$TRINO_HOST:$TRINO_PORT \
          --user $TRINO_USER \
          --catalog iceberg \
          --schema default

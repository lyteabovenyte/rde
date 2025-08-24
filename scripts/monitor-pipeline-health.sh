#!/bin/bash

# Script to monitor the health and status of RDE pipelines

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}📊 RDE Pipeline Health Monitor${NC}"
echo "=============================="

# Check if services are running
check_services() {
    echo -e "${YELLOW}🔍 Checking infrastructure services...${NC}"
    
    # Check Kafka
    if nc -z localhost 9092 2>/dev/null; then
        echo -e "${GREEN}✓ Kafka: RUNNING${NC}"
    else
        echo -e "${RED}✗ Kafka: NOT RUNNING${NC}"
    fi
    
    # Check MinIO
    if nc -z localhost 9000 2>/dev/null; then
        echo -e "${GREEN}✓ MinIO: RUNNING${NC}"
    else
        echo -e "${RED}✗ MinIO: NOT RUNNING${NC}"
    fi
    
    # Check Zookeeper
    if nc -z localhost 2181 2>/dev/null; then
        echo -e "${GREEN}✓ Zookeeper: RUNNING${NC}"
    else
        echo -e "${RED}✗ Zookeeper: NOT RUNNING${NC}"
    fi
}

# Check Kafka topics and message counts
check_kafka_topics() {
    echo -e "${YELLOW}📡 Checking Kafka topics...${NC}"
    
    local topics=("flights" "retail" "spotify")
    
    for topic in "${topics[@]}"; do
        echo -e "${BLUE}Topic: $topic${NC}"
        
        # Check if topic exists
        if docker exec rde-kafka-1 kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -q "^$topic$"; then
            # Get message count
            local count=$(docker exec rde-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:9092 \
                --topic "$topic" --time -1 2>/dev/null | \
                awk -F ':' '{sum += $3} END {print sum}' || echo "0")
            
            echo -e "  📈 Messages: $count"
            
            # Get partition info
            local partitions=$(docker exec rde-kafka-1 kafka-topics \
                --describe --topic "$topic" --bootstrap-server localhost:9092 2>/dev/null | \
                grep "PartitionCount" | awk '{print $4}' || echo "0")
            echo -e "  🔀 Partitions: $partitions"
            
            # Get latest message timestamp (if any)
            if [[ $count -gt 0 ]]; then
                local latest=$(docker exec rde-kafka-1 kafka-console-consumer \
                    --bootstrap-server localhost:9092 \
                    --topic "$topic" \
                    --max-messages 1 \
                    --timeout-ms 1000 2>/dev/null | head -1 || echo "")
                
                if [[ -n "$latest" ]]; then
                    echo -e "  ⏰ Latest message preview: ${latest:0:100}..."
                fi
            fi
        else
            echo -e "  ${RED}✗ Topic does not exist${NC}"
        fi
        echo ""
    done
}

# Check MinIO buckets and Iceberg tables
check_minio_tables() {
    echo -e "${YELLOW}🗄️  Checking MinIO/Iceberg tables...${NC}"
    
    local tables=("flights_data" "retail_products" "spotify_audio_features")
    
    # Check if MinIO console is accessible
    if curl -s -f http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        echo -e "${GREEN}✓ MinIO API accessible${NC}"
        
        for table in "${tables[@]}"; do
            echo -e "${BLUE}Table: $table${NC}"
            
            # Check if bucket exists (using mc client if available, otherwise just indicate)
            if command -v mc >/dev/null 2>&1; then
                # Use MinIO client if available
                if mc ls minio/iceberg-data/$table >/dev/null 2>&1; then
                    local file_count=$(mc ls --recursive minio/iceberg-data/$table | wc -l)
                    echo -e "  📁 Files: $file_count"
                    
                    # Check for metadata
                    if mc ls minio/iceberg-data/$table/metadata/ >/dev/null 2>&1; then
                        echo -e "  ${GREEN}✓ Metadata exists${NC}"
                    else
                        echo -e "  ${RED}✗ No metadata found${NC}"
                    fi
                    
                    # Check for data files
                    if mc ls minio/iceberg-data/$table/data/ >/dev/null 2>&1; then
                        local data_files=$(mc ls --recursive minio/iceberg-data/$table/data/ | grep -c "\.parquet" || echo "0")
                        echo -e "  📊 Parquet files: $data_files"
                    else
                        echo -e "  ${YELLOW}⚠ No data files yet${NC}"
                    fi
                else
                    echo -e "  ${RED}✗ Table directory not found${NC}"
                fi
            else
                echo -e "  ${YELLOW}⚠ MinIO client (mc) not available for detailed inspection${NC}"
                echo -e "  ${CYAN}ℹ Access MinIO Console: http://localhost:9001 (minioadmin/minioadmin)${NC}"
            fi
            echo ""
        done
    else
        echo -e "${RED}✗ MinIO API not accessible${NC}"
    fi
}

# Check running processes
check_processes() {
    echo -e "${YELLOW}🔄 Checking running processes...${NC}"
    
    # Check for RDE CLI processes
    local rde_processes=$(pgrep -f "rde-cli" || echo "")
    if [[ -n "$rde_processes" ]]; then
        echo -e "${GREEN}✓ RDE pipelines running:${NC}"
        ps -p $rde_processes -o pid,pcpu,pmem,time,cmd --no-headers | while read line; do
            echo -e "  📋 $line"
        done
    else
        echo -e "${RED}✗ No RDE pipeline processes found${NC}"
    fi
    
    # Check for Kafka producer processes
    local producer_processes=$(pgrep -f "kafka-producer" || echo "")
    if [[ -n "$producer_processes" ]]; then
        echo -e "${GREEN}✓ Kafka producers running:${NC}"
        ps -p $producer_processes -o pid,pcpu,pmem,time,cmd --no-headers | while read line; do
            echo -e "  📤 $line"
        done
    else
        echo -e "${YELLOW}⚠ No Kafka producer processes found${NC}"
    fi
    
    echo ""
}

# Check log files
check_logs() {
    echo -e "${YELLOW}📄 Checking log files...${NC}"
    
    if [[ -d "logs" ]]; then
        local log_files=("flights-pipeline.log" "retail-pipeline.log" "spotify-pipeline.log")
        
        for log_file in "${log_files[@]}"; do
            if [[ -f "logs/$log_file" ]]; then
                local size=$(stat -f%z "logs/$log_file" 2>/dev/null || stat -c%s "logs/$log_file" 2>/dev/null || echo "0")
                local lines=$(wc -l < "logs/$log_file" 2>/dev/null || echo "0")
                echo -e "${BLUE}$log_file${NC}: $size bytes, $lines lines"
                
                # Show last few lines
                echo -e "  ${CYAN}Recent entries:${NC}"
                tail -3 "logs/$log_file" 2>/dev/null | sed 's/^/    /' || echo "    (no recent entries)"
            else
                echo -e "${YELLOW}⚠ $log_file: not found${NC}"
            fi
        done
    else
        echo -e "${YELLOW}⚠ No logs directory found${NC}"
    fi
    echo ""
}

# Performance metrics
show_performance() {
    echo -e "${YELLOW}📈 Performance metrics...${NC}"
    
    # System load
    if command -v uptime >/dev/null 2>&1; then
        echo -e "${BLUE}System load:${NC} $(uptime)"
    fi
    
    # Memory usage
    if command -v free >/dev/null 2>&1; then
        echo -e "${BLUE}Memory usage:${NC}"
        free -h | sed 's/^/  /'
    elif command -v vm_stat >/dev/null 2>&1; then
        echo -e "${BLUE}Memory pressure:${NC}"
        vm_stat | head -5 | sed 's/^/  /'
    fi
    
    # Disk usage for data directories
    echo -e "${BLUE}Disk usage:${NC}"
    if [[ -d "data" ]]; then
        echo -e "  data/: $(du -sh data/ 2>/dev/null || echo 'unknown')"
    fi
    if [[ -d "logs" ]]; then
        echo -e "  logs/: $(du -sh logs/ 2>/dev/null || echo 'unknown')"
    fi
    
    echo ""
}

# Show comprehensive status
comprehensive_status() {
    echo -e "${CYAN}==== COMPREHENSIVE STATUS ====${NC}"
    check_services
    echo ""
    check_kafka_topics
    check_minio_tables
    check_processes
    check_logs
    show_performance
    echo -e "${CYAN}=============================${NC}"
}

# Continuous monitoring
continuous_monitor() {
    echo -e "${YELLOW}🔄 Starting continuous monitoring (Ctrl+C to stop)...${NC}"
    echo ""
    
    while true; do
        clear
        echo -e "${GREEN}📊 RDE Pipeline Health Monitor - $(date)${NC}"
        echo "======================================================"
        comprehensive_status
        echo ""
        echo -e "${BLUE}Refreshing in 10 seconds...${NC}"
        sleep 10
    done
}

# Show menu
show_menu() {
    echo ""
    echo -e "${YELLOW}Select monitoring option:${NC}"
    echo "1) 🔍 Quick status check"
    echo "2) 📡 Kafka topics status"
    echo "3) 🗄️  MinIO/Iceberg status"
    echo "4) 🔄 Running processes"
    echo "5) 📄 Log files"
    echo "6) 📈 Performance metrics"
    echo "7) 📊 Comprehensive status"
    echo "8) 🔄 Continuous monitoring"
    echo "0) 🚪 Exit"
    echo ""
}

# Main execution
main() {
    if [[ $# -gt 0 ]]; then
        case "$1" in
            "services") check_services ;;
            "topics") check_kafka_topics ;;
            "tables") check_minio_tables ;;
            "processes") check_processes ;;
            "logs") check_logs ;;
            "performance") show_performance ;;
            "status") comprehensive_status ;;
            "continuous") continuous_monitor ;;
            *) 
                echo -e "${RED}Invalid option: $1${NC}"
                echo "Usage: $0 [services|topics|tables|processes|logs|performance|status|continuous]"
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
            1) check_services ;;
            2) check_kafka_topics ;;
            3) check_minio_tables ;;
            4) check_processes ;;
            5) check_logs ;;
            6) show_performance ;;
            7) comprehensive_status ;;
            8) continuous_monitor ;;
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

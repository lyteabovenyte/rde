#!/bin/bash

echo "Testing Kafka to Iceberg Sink Pipeline with Dynamic Schema"
echo "=========================================================="

# Check if MinIO is running
if ! docker ps | grep -q minio-server; then
    echo "Starting MinIO..."
    ./start-minio.sh
    sleep 5
fi

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "Starting Kafka..."
    docker compose -f docker/docker-compose.yml up -d
    sleep 10
fi

# Create Kafka topic if it doesn't exist
echo "Creating Kafka topic 'sales'..."
docker exec -it $(docker ps -q --filter "name=kafka") kafka-topics --create --topic sales --bootstrap-server localhost:9092 --if-not-exists

echo ""
echo "Starting Kafka to Iceberg pipeline with dynamic schema..."
echo "This will read JSON messages from Kafka and automatically infer the schema"
echo ""

# Run the pipeline in the background
RUST_LOG=info cargo run -p rde-cli -- --pipeline examples/kafka-iceberg-dynamic.yml &
PIPELINE_PID=$!

echo "Pipeline started with PID: $PIPELINE_PID"
echo ""
echo "Now you can send JSON messages to Kafka:"
echo "docker exec -it \$(docker ps -q --filter \"name=kafka\") kafka-console-producer --broker-list localhost:9092 --topic sales"
echo ""
echo "Example messages with different schemas:"
echo ""
echo "Basic sales record:"
echo '{"id": 1, "amount": 300.50, "product": "laptop", "customer": "john_doe"}'
echo ""
echo "Sales record with additional fields:"
echo '{"id": 2, "amount": 150.25, "product": "mouse", "customer": "jane_smith", "region": "US", "timestamp": "2024-01-15T10:30:00Z"}'
echo ""
echo "Sales record with nested data:"
echo '{"id": 3, "amount": 500.75, "product": "keyboard", "customer": "bob_wilson", "metadata": {"color": "black", "warranty": "2 years"}}'
echo ""
echo "Sales record with array data:"
echo '{"id": 4, "amount": 750.00, "product": "monitor", "customer": "alice_brown", "tags": ["gaming", "4k", "curved"]}'
echo ""
echo "The pipeline will automatically:"
echo "- Infer the schema from the first message"
echo "- Handle schema evolution as new fields appear"
echo "- Convert all data types appropriately"
echo "- Store everything as Parquet files in MinIO"
echo ""
echo "Check MinIO Console at http://localhost:9001 to see the files"
echo ""
echo "Press Ctrl+C to stop the pipeline"

# Wait for user to stop
wait $PIPELINE_PID

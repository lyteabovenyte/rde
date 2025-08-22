#!/bin/bash

echo "Testing Kafka to Iceberg Sink Pipeline"
echo "======================================"

# Check if MinIO is running
if ! docker ps | grep -q minio-server; then
    echo "Starting MinIO..."
    ./start-minio.sh
    sleep 5
fi

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "Starting Kafka..."
    docker compose up -d
    sleep 10
fi

# Create Kafka topic if it doesn't exist
echo "Creating Kafka topic 'sales'..."
docker exec -it $(docker ps -q --filter "name=kafka") kafka-topics --create --topic sales --bootstrap-server localhost:9092 --if-not-exists

echo ""
echo "Starting Kafka to Iceberg pipeline..."
echo "This will read JSON messages from Kafka and write them as Parquet files to MinIO"
echo ""

# Run the pipeline in the background
RUST_LOG=info cargo run -p rde-cli -- --pipeline examples/kafka-iceberg.yml &
PIPELINE_PID=$!

echo "Pipeline started with PID: $PIPELINE_PID"
echo ""
echo "Now you can send JSON messages to Kafka:"
echo "docker exec -it \$(docker ps -q --filter \"name=kafka\") kafka-console-producer --broker-list localhost:9092 --topic sales"
echo ""
echo "Example messages:"
echo '{"id": 1, "amount": 300, "product": "laptop"}'
echo '{"id": 2, "amount": 150, "product": "mouse"}'
echo '{"id": 3, "amount": 500, "product": "keyboard"}'
echo ""
echo "The pipeline will convert these to Parquet files and store them in MinIO"
echo "Check MinIO Console at http://localhost:9001 to see the files"
echo ""
echo "Press Ctrl+C to stop the pipeline"

# Wait for user to stop
wait $PIPELINE_PID

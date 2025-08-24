# Testing the RDE Pipeline with Kafka Producer

This guide demonstrates how to use the Kafka producer to test the complete RDE pipeline with various data scenarios.

## Prerequisites

1. Ensure Docker is running
2. Build the project:
   ```bash
   cargo build --release
   ```

## Step 1: Start Infrastructure

```bash
# Start Kafka and MinIO
docker-compose up -d

# Start MinIO (if not using docker-compose)
./scripts/start-minio.sh
```

## Step 2: Generate Test Data (Optional)

Generate large datasets for stress testing:

```bash
# Generate 10,000 user events in NDJSON format
./scripts/generate-test-data.py -t user-events -n 10000 -f ndjson

# Generate 5,000 products split into 5 files
./scripts/generate-test-data.py -t products -n 5000 --split 1000

# Generate all types with 1,000 records each
./scripts/generate-test-data.py -t all -n 1000
```

## Step 3: Start the RDE Pipeline

Choose a pipeline configuration based on your test scenario:

### Basic Kafka to Iceberg
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg.yml
```

### With Topic Mapping and Schema Evolution
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg-topic-mapping.yml
```

### With SQL Transformations
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg-with-transforms.yml
```

## Step 4: Stream Data to Kafka

### Interactive Mode (Recommended for Testing)
```bash
./scripts/stream-test-data.sh
```

### Direct Producer Usage

#### Stream User Events (Array Format)
```bash
cargo run --bin kafka-producer -- \
  -i data/json-samples/user-events.json \
  -t user-events \
  -d 500  # 500ms delay between messages
```

#### Stream Product Analytics (NDJSON Format)
```bash
cargo run --bin kafka-producer -- \
  -i data/json-samples/product-analytics.ndjson \
  -t products \
  --progress-interval 5
```

#### Test Schema Evolution
```bash
cargo run --bin kafka-producer -- \
  -i data/json-samples/schema-evolution-demo.ndjson \
  -t evolution-test \
  -d 2000  # 2 second delay to observe schema changes
```

#### Batch Processing Large Files
```bash
cargo run --bin kafka-producer -- \
  -i data/generated/user-events-large.ndjson \
  -t user-events \
  --batch-size 100 \
  --progress-interval 1000
```

#### Process Multiple Files
```bash
cargo run --bin kafka-producer -- \
  -i data/generated \
  -t mixed-events \
  --glob-pattern "*.ndjson" \
  --continue-on-error
```

## Step 5: Monitor Results

### Check Kafka Messages
```bash
# List topics
docker exec -it rde-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Consume messages from a topic
docker exec -it rde-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --from-beginning \
  --max-messages 10
```

### Check MinIO/Iceberg Tables
1. Open MinIO Console: http://localhost:9001
2. Login: minioadmin/minioadmin
3. Browse to: iceberg-data bucket
4. Look for Parquet files and metadata

### View Pipeline Logs
The RDE pipeline will show:
- Schema inference/evolution
- Message processing
- Iceberg table updates

## Test Scenarios

### 1. Schema Evolution Test
```bash
# Start pipeline
cargo run --bin rde-cli -- -p examples/kafka-iceberg-topic-mapping.yml

# In another terminal, stream schema evolution demo
cargo run --bin kafka-producer -- \
  -i data/json-samples/schema-evolution-demo.ndjson \
  -t evolution-test \
  -d 3000  # 3 second delay
```

Watch as the pipeline detects new fields and updates the Iceberg table schema.

### 2. High Volume Test
```bash
# Generate 100,000 events
./scripts/generate-test-data.py -t user-events -n 100000

# Stream with minimal delay
cargo run --bin kafka-producer -- \
  -i data/generated/user-events-large.ndjson \
  -t user-events \
  --batch-size 1000 \
  -d 10  # 10ms delay
```

### 3. Mixed Schema Test
```bash
# Create a directory with different event types
mkdir -p data/mixed-test
./scripts/generate-test-data.py -t user-events -n 1000 -o data/mixed-test
./scripts/generate-test-data.py -t products -n 1000 -o data/mixed-test
./scripts/generate-test-data.py -t transactions -n 1000 -o data/mixed-test

# Stream all files to same topic (tests schema merging)
cargo run --bin kafka-producer -- \
  -i data/mixed-test \
  -t mixed-events \
  --continue-on-error
```

### 4. Error Recovery Test
```bash
# Create some invalid JSON files
echo '{"valid": "json"}' > data/test-invalid/good1.json
echo 'invalid json' > data/test-invalid/bad1.json
echo '{"another": "valid"}' > data/test-invalid/good2.json

# Stream with error handling
cargo run --bin kafka-producer -- \
  -i data/test-invalid \
  -t error-test \
  --continue-on-error
```

## Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker ps | grep kafka

# Test connection
nc -zv localhost 9092
```

### Producer Errors
- Enable debug logging: `RUST_LOG=debug cargo run --bin kafka-producer ...`
- Use dry-run mode first: `--dry-run`
- Check message format with small batches

### Pipeline Issues
- Check pipeline logs for schema errors
- Verify MinIO credentials in pipeline config
- Ensure topic names match between producer and pipeline

# Getting Started with RDE

Welcome to RDE (Rust Data Engineering)! This guide will walk you through setting up your first data processing pipeline.

## Prerequisites

Before you begin, make sure you have:

- **Rust 1.70+** - [Install Rust](https://rustup.rs/)
- **Docker** - [Install Docker](https://docs.docker.com/get-docker/)
- **Git** - For cloning the repository

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/lyteabovenyte/rde.git
cd rde
```

### 2. Build the Project

```bash
# Build all components
cargo build --release

# This will build:
# - rde-cli: Main pipeline runner
# - kafka-producer: Data streaming utility
# - All library crates
```

### 3. Start Infrastructure

```bash
# Start Kafka and MinIO
docker-compose -f docker/docker-compose.yml up -d

# Verify services are running
docker ps
```

You should see containers for:
- Kafka (port 9092)
- Zookeeper (port 2181) 
- MinIO (ports 9000, 9001)

## Your First Pipeline

Let's create a simple pipeline that reads JSON data from Kafka and writes it to an Iceberg table.

### 1. Create Test Data

First, let's create some sample JSON data:

```bash
# Create a test data file
cat > test-data.ndjson << 'EOF'
{"id": 1, "name": "Alice", "age": 30, "city": "New York"}
{"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"}
{"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"}
EOF
```

### 2. Create Pipeline Configuration

Create a file called `my-first-pipeline.yml`:

```yaml
name: "my-first-pipeline"

sources:
  - type: kafka
    id: "kafka-source"
    brokers: "localhost:9092"
    group_id: "tutorial-group"
    topic: "user-events"

transforms:
  - type: clean_data
    id: "clean-data"
    remove_nulls: true
    trim_strings: true

sinks:
  - type: iceberg
    id: "iceberg-sink"
    table_name: "users"
    bucket: "iceberg-data"
    endpoint: "http://localhost:9000"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    region: "us-east-1"

edges:
  - ["kafka-source", "clean-data"]
  - ["clean-data", "iceberg-sink"]
```

### 3. Create Kafka Topic

```bash
# Create the topic
docker exec -it $(docker ps -q -f name=kafka) kafka-topics.sh \
  --create \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Start the Pipeline

In one terminal, start your pipeline:

```bash
# Start the pipeline with info-level logging
RUST_LOG=info cargo run --bin rde-cli -- --pipeline my-first-pipeline.yml
```

You should see output like:
```
INFO rde_io::source_kafka: Kafka source started for topic: user-events
INFO rde_io::sink_iceberg: Iceberg sink initialized for table: users
```

### 5. Stream Test Data

In another terminal, stream your test data:

```bash
# Stream the test data to Kafka
cargo run --bin kafka-producer -- \
  --input test-data.ndjson \
  --topic user-events \
  --format ndjson \
  --progress-interval 1
```

You should see:
```
Streaming from: test-data.ndjson
Format: ndjson
Topic: user-events
Progress: Sent 3 messages
```

### 6. Verify Results

Check that your data was processed:

```bash
# Check MinIO for created files
docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/users/ --recursive
```

You should see Parquet files and Iceberg metadata files.

## What Just Happened?

Congratulations! You've just:

1. ✅ Set up a complete data engineering infrastructure
2. ✅ Created a streaming data pipeline
3. ✅ Processed JSON data with schema inference
4. ✅ Stored data in an Iceberg table with ACID guarantees

## Next Steps

### Add SQL Transformations

Enhance your pipeline with SQL transformations:

```yaml
transforms:
  - type: sql_transform
    id: "enrich-data"
    query: |
      SELECT 
        *,
        CASE 
          WHEN age < 30 THEN 'young'
          WHEN age < 50 THEN 'middle'
          ELSE 'senior'
        END as age_group,
        CURRENT_TIMESTAMP as processed_at
      FROM input_data
```

### Set Up Analytics

Query your data using Trino:

```bash
# Start Trino for SQL analytics
./scripts/setup-trino.sh

# Query your data
./scripts/run-sql.sh "SELECT age_group, COUNT(*) FROM iceberg.default.users GROUP BY age_group"
```

### Scale Up

Process larger datasets:

```bash
# Generate larger test datasets
./scripts/generate-test-data.py --format ndjson --records 100000 --output large-dataset.ndjson

# Stream with higher throughput
cargo run --bin kafka-producer -- \
  --input large-dataset.ndjson \
  --topic user-events \
  --format ndjson \
  --batch-size 1000 \
  --delay-ms 10
```

## Common Patterns

### Real-Time Analytics Pipeline

```yaml
name: "analytics-pipeline"
sources:
  - type: kafka
    id: "events"
    topic: "user-events"
    topic_mapping:
      iceberg_table: "user_events"
      auto_schema_evolution: true
      sql_transform: |
        SELECT 
          *,
          DATE(timestamp) as partition_date,
          HOUR(timestamp) as hour
        FROM input_data
      partition_by: ["partition_date", "hour"]
```

### Batch ETL Pipeline

```yaml
name: "batch-etl"
sources:
  - type: file_csv
    id: "sales-data"
    path: "data/sales/*.csv"
    has_header: true
    batch_rows: 50000

transforms:
  - type: sql_transform
    id: "calculate-metrics"
    query: |
      SELECT 
        *,
        amount * 1.1 as amount_with_tax,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY timestamp) as order_sequence
      FROM input_data
```

## Troubleshooting

### Pipeline Won't Start

1. **Check infrastructure**: `docker ps` - ensure Kafka and MinIO are running
2. **Check logs**: `RUST_LOG=debug cargo run --bin rde-cli -- --pipeline config.yml`
3. **Validate config**: Check YAML syntax and required fields

### No Data Flowing

1. **Check Kafka topic**: Verify topic exists and has messages
2. **Check consumer group**: Ensure group ID is unique or reset offsets
3. **Check schema**: Enable auto-inference if data structure is changing

### Performance Issues

1. **Increase batch sizes**: Use larger `batch_rows` for sources
2. **Tune channel capacity**: Use `--channel-capacity 1000` 
3. **Monitor memory**: Use `RUST_LOG=info` to track processing rates

### Schema Evolution Issues

1. **Enable auto-evolution**: Set `auto_schema_evolution: true`
2. **Check data types**: Ensure consistent field types in JSON
3. **Monitor schema changes**: Look for schema evolution log messages

## Learning Resources

- **[Configuration Reference](configuration.md)** - Complete YAML configuration guide
- **[SQL Transformations](sql-transforms.md)** - DataFusion SQL reference
- **[Iceberg Integration](iceberg.md)** - Working with data lakes
- **[Performance Tuning](performance.md)** - Optimization guidelines
- **[Examples](../examples/)** - Sample pipeline configurations

## Getting Help

- **Documentation**: Browse the `docs/` directory
- **Examples**: Check the `examples/` directory for sample configurations
- **Issues**: [Report bugs on GitHub](https://github.com/lyteabovenyte/rde/issues)
- **Discussions**: [Ask questions on GitHub Discussions](https://github.com/lyteabovenyte/rde/discussions)

---

Ready to build more complex pipelines? Check out the [Configuration Reference](configuration.md) for all available options and features!

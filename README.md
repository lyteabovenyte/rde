# RDE - Rust Data Engineering

A high-performance, real-time data engineering pipeline built in Rust using Apache Arrow and DataFusion.

## Features

### Sources
- **Kafka**: Real-time streaming from Kafka topics with JSON schema inference and **topic-to-table mapping**
- **CSV**: Batch processing from CSV files with automatic schema detection

### Transforms
- **Schema Evolution**: Dynamic schema inference and evolution for changing data structures
- **JSON Flattening**: Convert nested JSON structures to flat relational format
- **Data Cleaning**: Remove nulls, trim strings, normalize case, and more
- **Partitioning**: Add partition columns for optimized downstream analytics
- **SQL Transformations**: Complex business logic using DataFusion SQL engine
- **Passthrough**: Simple pass-through transformation (no changes)

### Sinks
- **Iceberg**: Write to Apache Iceberg tables in MinIO/S3 with Parquet format
- **Parquet Directory**: Write partitioned Parquet files to local filesystem
- **Stdout**: Pretty-print data for debugging and development

## 🚀 New: Topic-to-Table Mapping

RDE now supports **topic-to-table mapping** that links Kafka topics directly to Iceberg tables with automatic schema evolution and topic-specific transformations:

### Key Benefits:
1. **Automatic Schema Evolution**: When new fields are detected in Kafka messages, the Iceberg table schema is automatically updated without overwriting existing data
2. **Topic-Specific Transformations**: Each topic can have its own SQL transformation logic using DataFusion
3. **Direct Mapping**: No need for separate sink configurations - the mapping handles everything
4. **Schema Consistency**: Ensures the Kafka message schema matches the Iceberg table schema

### Example Configuration:

```yaml
sources:
  - type: kafka
    id: "user-events-source"
    brokers: "localhost:9092"
    group_id: "rde-user-events-group"
    topic: "user-events"
    topic_mapping:
      iceberg_table: "user_events"
      bucket: "iceberg-data"
      endpoint: "http://localhost:9000"
      access_key: "minioadmin"
      secret_key: "minioadmin"
      region: "us-east-1"
      auto_schema_evolution: true
      sql_transform: |
        SELECT 
          user_id,
          event_type,
          timestamp,
          CASE 
            WHEN event_type = 'purchase' THEN amount
            ELSE 0 
          END as purchase_amount,
          metadata->>'page' as page_url
        FROM input_data
        WHERE user_id IS NOT NULL
      partition_by: ["event_type", "date(timestamp)"]
```

### How It Works:

1. **Schema Loading**: On startup, RDE loads the existing schema from the mapped Iceberg table
2. **Message Processing**: Each Kafka message is parsed and compared against the current schema
3. **Schema Evolution**: If new fields are detected, the Iceberg table schema is automatically updated
4. **SQL Transformation**: The topic-specific SQL query is applied to transform the data
5. **Data Writing**: Transformed data is written to the mapped Iceberg table

## Quick Start

### Prerequisites
- Rust 1.70+
- Docker and Docker Compose (for Kafka and MinIO)
- Apache Kafka broker running on `localhost:9092`
- MinIO running on `localhost:9000`

### Installation
```bash
git clone <repository>
cd rde
cargo build --release
```

### Running a Pipeline

1. **Start infrastructure services:**
```bash
docker-compose up -d
scripts/start-minio.sh
```

2. **Run a simple Kafka to Iceberg pipeline:**
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg.yml
```

3. **Run with topic mapping (recommended):**
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg-topic-mapping.yml
```

4. **Run with advanced transformations:**
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg-with-transforms.yml
```

## Configuration

### Pipeline Configuration

Pipelines are defined in YAML format with sources, transforms, and sinks connected by edges:

```yaml
name: my-pipeline
sources:
  - type: kafka
    id: kafka-source
    brokers: localhost:9092
    group_id: my-group
    topic: my-topic
    topic_mapping:
      iceberg_table: "my_table"
      bucket: "my-bucket"
      endpoint: "http://localhost:9000"
      access_key: "minioadmin"
      secret_key: "minioadmin"
      region: "us-east-1"
      auto_schema_evolution: true
      sql_transform: |
        SELECT 
          *,
          CASE 
          WHEN amount > 1000 THEN 'high_value'
          ELSE 'low_value'
        END as value_tier
```

### Transform Types

#### Schema Evolution
Handles dynamic schema changes in streaming data:
```yaml
- type: schema_evolution
  id: schema-evolution
  auto_infer: true      # Automatically infer schema from data
  strict_mode: false    # Allow schema changes (false) or fail (true)
```

#### JSON Flattening
Converts nested JSON structures to flat relational format:
```yaml
- type: json_flatten
  id: flatten
  separator: "_"        # Separator for nested field names
  max_depth: 3          # Maximum nesting depth to flatten
```

#### Data Cleaning
Cleans and normalizes data:
```yaml
- type: clean_data
  id: cleaner
  remove_nulls: false   # Remove rows with null values
  trim_strings: true    # Trim whitespace from strings
  normalize_case: "lower"  # "lower", "upper", "title", or null
```

#### Partitioning
Adds partition columns for optimized storage and querying:
```yaml
- type: partition
  id: partitioner
  partition_by: ["region", "category"]  # Fields to partition by
  partition_format: "region={0}/category={1}"  # Custom format string
```

#### SQL Transformations
Apply complex business logic using SQL:
```yaml
- type: sql_transform
  id: business-logic
  query: |
    SELECT 
      *,
      amount * 0.1 as commission,
      CASE 
        WHEN amount > 1000 THEN 'high_value'
        ELSE 'low_value'
      END as value_tier
    FROM input_data
    WHERE amount > 0
  window_size: 100      # Number of batches to buffer before processing
```

## Examples

### Basic Kafka to Iceberg
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg.yml
```

### JSON Flattening Pipeline
```bash
cargo run --bin rde-cli -- -p examples/json-flattening-example.yml
```

### SQL Transformation Pipeline
```bash
cargo run --bin rde-cli -- -p examples/sql-transformation-example.yml
```

### Full Transformation Pipeline
```bash
cargo run --bin rde-cli -- -p examples/kafka-iceberg-with-transforms.yml
```

## Architecture

RDE uses a modular architecture with:

- **Sources**: Generate data streams from external systems
- **Transforms**: Process and transform data using Arrow and DataFusion
- **Sinks**: Write processed data to storage systems
- **Channels**: Connect components with bounded async channels
- **Schema Evolution**: Handle dynamic schema changes in streaming data

### Key Components

- **Arrow**: Columnar memory format for efficient data processing
- **DataFusion**: SQL query engine for complex transformations
- **Tokio**: Async runtime for high-performance streaming
- **Serde**: Serialization for configuration and data handling

## Development

### Building
```bash
cargo build
cargo test
```

### Running Tests
```bash
cargo test --workspace
```

### Code Structure
```
rde/
├── crates/
│   ├── rde-core/     # Core types and traits
│   ├── rde-io/       # Sources and sinks
│   └── rde-tx/       # Transformations
├── bins/
│   └── rde-cli/      # Command-line interface
├── examples/         # Pipeline configurations
└── scripts/          # Utility scripts
```

## Performance

RDE is designed for high-performance streaming with:

- **Zero-copy data processing** using Arrow
- **Async/await** for non-blocking I/O
- **Bounded channels** to prevent memory overflow
- **Batch processing** for efficient throughput
- **Schema evolution** for handling changing data structures

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

# RDE - Rust Data Engineering

A high-performance, real-time data engineering pipeline built in Rust using Apache Arrow and DataFusion.

## Features

### Sources
- **Kafka**: Real-time streaming from Kafka topics with JSON schema inference
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

3. **Run with advanced transformations:**
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
    schema:
      auto_infer: true

transforms:
  - type: schema_evolution
    id: schema-evolution
    auto_infer: true
    strict_mode: false
  
  - type: json_flatten
    id: flatten
    separator: "_"
    max_depth: 3
  
  - type: clean_data
    id: cleaner
    trim_strings: true
    normalize_case: "lower"
  
  - type: partition
    id: partitioner
    partition_by: ["region", "category"]
    partition_format: "region={0}/category={1}"
  
  - type: sql_transform
    id: business-logic
    query: |
      SELECT 
        *,
        CASE 
          WHEN amount > 1000 THEN 'high_value'
          ELSE 'low_value'
        END as value_tier
      FROM input_data
    window_size: 100

sinks:
  - type: iceberg
    id: iceberg-sink
    table_name: my_table
    bucket: my-bucket
    endpoint: http://localhost:9000
    access_key: minioadmin
    secret_key: minioadmin
    region: us-east-1

edges:
  - [kafka-source, schema-evolution]
  - [schema-evolution, flatten]
  - [flatten, cleaner]
  - [cleaner, partitioner]
  - [partitioner, business-logic]
  - [business-logic, iceberg-sink]
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

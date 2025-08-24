# RDE Pipeline Configuration Reference

This document provides a comprehensive reference for configuring RDE data processing pipelines using YAML files.

## Overview

RDE pipelines are defined using YAML configuration files that specify:
- **Sources**: Where data comes from (Kafka, CSV files, etc.)
- **Transforms**: How to process and modify the data
- **Sinks**: Where to write the processed data (Iceberg, Parquet, etc.)
- **Edges**: How operators connect to form a processing graph

## Basic Structure

```yaml
name: "my-pipeline"
sources:
  - type: kafka
    id: "source-1"
    # ... source configuration
transforms:
  - type: passthrough  
    id: "transform-1"
    # ... transform configuration
sinks:
  - type: iceberg
    id: "sink-1" 
    # ... sink configuration
edges:
  - ["source-1", "transform-1"]
  - ["transform-1", "sink-1"]
```

## Sources

### Kafka Source

Stream data from Apache Kafka topics with automatic schema inference and evolution.

```yaml
sources:
  - type: kafka
    id: "kafka-source"
    brokers: "localhost:9092"
    group_id: "my-consumer-group"
    topic: "input-topic"
    
    # Optional: Schema configuration
    schema:
      auto_infer: true
      fields:
        - name: "id"
          data_type: "int64"
          nullable: false
        - name: "name"
          data_type: "string"
          nullable: true
    
    # Optional: Direct topic-to-table mapping
    topic_mapping:
      iceberg_table: "my_table"
      bucket: "iceberg-data"
      endpoint: "http://localhost:9000"
      access_key: "minioadmin"
      secret_key: "minioadmin" 
      region: "us-east-1"
      auto_schema_evolution: true
      sql_transform: |
        SELECT 
          *,
          CURRENT_TIMESTAMP as ingestion_time,
          DATE(timestamp) as partition_date
        FROM input_data
      partition_by: ["partition_date"]
```

### CSV Source

Process CSV files with configurable parsing options.

```yaml
sources:
  - type: file_csv
    id: "csv-source"
    path: "data/input.csv"  # or glob pattern like "data/*.csv"
    has_header: true
    batch_rows: 10000
```

## Transforms

### Passthrough Transform

No-op transformation for testing and simple data flow.

```yaml
transforms:
  - type: passthrough
    id: "passthrough-1"
```

### Schema Evolution Transform

Handle dynamic schema changes and evolution.

```yaml
transforms:
  - type: schema_evolution
    id: "schema-evolution"
    auto_infer: true
    strict_mode: false
```

### JSON Flattening Transform

Convert nested JSON structures to flat relational tables.

```yaml
transforms:
  - type: json_flatten
    id: "flatten-json"
    separator: "_"
    max_depth: 5
```

### Data Cleaning Transform

Clean and normalize data with various options.

```yaml
transforms:
  - type: clean_data
    id: "clean-data"
    remove_nulls: true
    trim_strings: true
    normalize_case: "lower"  # "lower", "upper", "title"
```

### SQL Transform

Execute complex SQL transformations using DataFusion.

```yaml
transforms:
  - type: sql_transform
    id: "sql-transform"
    query: |
      SELECT 
        user_id,
        event_type,
        timestamp,
        CASE 
          WHEN event_type = 'purchase' THEN amount
          ELSE 0 
        END as revenue,
        DATE(timestamp) as partition_date
      FROM input_data
      WHERE user_id IS NOT NULL
    window_size: 1000
```

### Partitioning Transform

Add partition columns for optimized storage.

```yaml
transforms:
  - type: partition
    id: "add-partitions"
    partition_by: ["year", "month", "day"]
    partition_format: "yyyy/MM/dd"
```

## Sinks

### Iceberg Sink

Write to Apache Iceberg tables with ACID guarantees.

```yaml
sinks:
  - type: iceberg
    id: "iceberg-sink"
    table_name: "my_table"
    bucket: "iceberg-data"
    endpoint: "http://localhost:9000"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    region: "us-east-1"
```

### Parquet Sink

Write to Parquet files in a directory.

```yaml
sinks:
  - type: parquet_dir
    id: "parquet-sink"
    path: "output/data"
```

### Stdout Sink

Print data to console for debugging.

```yaml
sinks:
  - type: stdout_pretty
    id: "debug-output"
```

## Advanced Configuration

### Environment Variables

Use environment variable substitution in configurations:

```yaml
sources:
  - type: kafka
    brokers: "${KAFKA_BROKERS:localhost:9092}"
    topic: "${KAFKA_TOPIC:default-topic}"
```

### Multiple Pipelines

Define complex processing graphs with multiple branches:

```yaml
name: "complex-pipeline"
sources:
  - type: kafka
    id: "raw-events"
    topic: "raw-events"
    
transforms:
  - type: clean_data
    id: "clean-raw"
    
  - type: sql_transform
    id: "user-events"
    query: "SELECT * FROM input_data WHERE event_type = 'user'"
    
  - type: sql_transform
    id: "system-events" 
    query: "SELECT * FROM input_data WHERE event_type = 'system'"
    
sinks:
  - type: iceberg
    id: "user-sink"
    table_name: "user_events"
    
  - type: iceberg
    id: "system-sink"
    table_name: "system_events"
    
edges:
  - ["raw-events", "clean-raw"]
  - ["clean-raw", "user-events"]
  - ["clean-raw", "system-events"] 
  - ["user-events", "user-sink"]
  - ["system-events", "system-sink"]
```

### Performance Tuning

Configure performance-related settings:

```yaml
# In your pipeline configuration
name: "high-performance-pipeline"

# Use larger batch sizes for better throughput
sources:
  - type: file_csv
    batch_rows: 100000  # Default: 65536
    
# Configure channel capacity in CLI
# rde-cli --pipeline config.yml --channel-capacity 1000
```

## Validation

Validate your configuration before running:

```bash
# Check configuration syntax
rde-cli --pipeline my-pipeline.yml --validate

# Dry run to test connections
rde-cli --pipeline my-pipeline.yml --dry-run
```

## Examples

See the `examples/` directory for complete pipeline configurations:

- `examples/kafka-iceberg-simple.yml` - Basic Kafka to Iceberg pipeline
- `examples/kafka-iceberg-with-transforms.yml` - Pipeline with SQL transformations
- `examples/kafka-iceberg-topic-mapping.yml` - Direct topic-to-table mapping
- `examples/csv-to-parquet.yml` - Batch CSV processing
- `examples/json-flattening-example.yml` - JSON structure flattening

## Best Practices

1. **Use meaningful IDs**: Make operator IDs descriptive for better debugging
2. **Start simple**: Begin with passthrough transforms and add complexity gradually
3. **Monitor performance**: Use appropriate batch sizes and channel capacities
4. **Handle schema evolution**: Enable auto-inference for dynamic data sources
5. **Test incrementally**: Validate each pipeline component before full deployment
6. **Use environment variables**: Make configurations portable across environments
7. **Document transformations**: Add comments explaining complex SQL transforms

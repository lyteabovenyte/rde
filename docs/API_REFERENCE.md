# RDE API Reference

Complete API reference for RDE v2.0.0

## Core Crates

### rde-core

Foundation traits and types for building data pipelines.

#### Traits

- **`Operator`** - Base trait for all pipeline operators
- **`Source`** - Data ingestion operators  
- **`Transform`** - Data processing operators
- **`Sink`** - Data output operators

#### Types

- **`Message`** - Inter-operator communication
- **`Batch`** - Apache Arrow RecordBatch alias
- **`BatchTx/BatchRx`** - Channel types for message passing

#### Configuration

- **`PipelineSpec`** - Complete pipeline configuration
- **`SourceSpec`** - Source operator configurations
- **`TransformSpec`** - Transform operator configurations  
- **`SinkSpec`** - Sink operator configurations

### rde-io

Source and sink implementations for various data systems.

#### Sources

- **`KafkaPipelineSource`** - Apache Kafka streaming source
- **`CsvSource`** - CSV file batch source

#### Sinks

- **`IcebergSink`** - Apache Iceberg table sink
- **`ParquetDirSink`** - Parquet file directory sink
- **`StdoutSink`** - Console output sink

#### Topic Mapping

- **`TopicMappingManager`** - Kafka-to-Iceberg direct streaming

### rde-tx

Transform operator implementations for data processing.

#### Basic Transforms

- **`Passthrough`** - No-op transformation
- **`CleanData`** - Data cleaning and normalization

#### Advanced Transforms

- **`SqlTransform`** - DataFusion SQL transformations
- **`JsonFlatten`** - Nested JSON flattening
- **`SchemaEvolution`** - Dynamic schema handling

## Binaries

### rde-cli

Main pipeline runner with YAML configuration support.

```bash
rde-cli --pipeline config.yml [--channel-capacity N]
```

### kafka-producer

Data streaming utility for testing and data ingestion.

```bash
kafka-producer -i input.json -t topic -f format [options]
```

## Configuration Reference

### Pipeline Structure

```yaml
name: "pipeline-name"
sources: [...]      # Data sources
transforms: [...]   # Data transformations  
sinks: [...]        # Data outputs
edges: [...]        # Operator connections
```

### Source Types

#### Kafka Source

```yaml
type: kafka
id: "source-id"
brokers: "localhost:9092"
group_id: "consumer-group"
topic: "topic-name"
schema:                    # Optional
  auto_infer: true
topic_mapping:             # Optional
  iceberg_table: "table"
  auto_schema_evolution: true
```

#### CSV Source

```yaml
type: file_csv
id: "csv-source"
path: "data/*.csv"
has_header: true
batch_rows: 10000
```

### Transform Types

#### Passthrough

```yaml
type: passthrough
id: "passthrough-id"
```

#### SQL Transform

```yaml
type: sql_transform
id: "sql-id"
query: "SELECT * FROM input_data WHERE condition"
```

#### Data Cleaning

```yaml
type: clean_data
id: "clean-id"
remove_nulls: true
trim_strings: true
normalize_case: "lower"
```

### Sink Types

#### Iceberg Sink

```yaml
type: iceberg
id: "iceberg-id"
table_name: "my_table"
bucket: "iceberg-data"
endpoint: "http://localhost:9000"
access_key: "minioadmin"
secret_key: "minioadmin"
region: "us-east-1"
```

#### Parquet Sink

```yaml
type: parquet_dir
id: "parquet-id"
path: "output/data"
```

## Error Types

### RdeError

```rust
pub enum RdeError {
    ChannelClosed(&'static str),
    Other(anyhow::Error),
}
```

## Environment Variables

- **`RUST_LOG`** - Logging level (trace, debug, info, warn, error)
- **`BATCH_SIZE`** - Default batch size for processing
- **`CHANNEL_CAPACITY`** - Default channel buffer size

## Examples

### Basic Pipeline

```rust
use rde_core::*;
use rde_io::*;
use rde_tx::*;

let spec = PipelineSpec {
    name: "example".to_string(),
    sources: vec![SourceSpec::Kafka(/* config */)],
    transforms: vec![TransformSpec::Passthrough { 
        id: "pass".to_string() 
    }],
    sinks: vec![SinkSpec::Stdout { 
        id: "out".to_string() 
    }],
    edges: vec![
        ("kafka".to_string(), "pass".to_string()),
        ("pass".to_string(), "out".to_string()),
    ],
};
```

### Custom Operator

```rust
use async_trait::async_trait;
use rde_core::*;

pub struct MySource {
    id: String,
    schema: SchemaRef,
}

#[async_trait]
impl Operator for MySource {
    fn name(&self) -> &str { &self.id }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}

#[async_trait]
impl Source for MySource {
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()> {
        // Implementation
        Ok(())
    }
}
```

## Best Practices

1. **Error Handling**: Always use `Result<T>` and proper error propagation
2. **Schema Management**: Enable auto-inference for dynamic data
3. **Performance**: Tune batch sizes and channel capacities
4. **Monitoring**: Use structured logging with tracing
5. **Testing**: Write comprehensive unit and integration tests

## Version Compatibility

- **Rust**: 1.70+ required
- **Apache Arrow**: 50.x series
- **DataFusion**: 49.x series  
- **Kafka**: librdkafka 0.36+

## Migration Guide

### From v1.x to v2.0

Major changes in v2.0:

1. **New trait system**: Updated operator traits with better error handling
2. **Enhanced schema evolution**: Improved automatic schema inference
3. **Topic mapping**: Direct Kafka-to-Iceberg streaming
4. **Performance improvements**: Better memory management and throughput
5. **Configuration changes**: Updated YAML format with new options

See [MIGRATION.md](MIGRATION.md) for detailed migration instructions.

---

For more information, see the complete documentation at [docs/](.).

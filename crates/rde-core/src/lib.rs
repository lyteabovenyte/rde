//! # RDE Core - Rust Data Engineering Core Library
//!
//! This crate provides the foundational types, traits, and abstractions for building
//! high-performance data engineering pipelines in Rust. It defines the core messaging
//! system, operator interfaces, and configuration structures used throughout the RDE ecosystem.
//!
//! ## Key Components
//!
//! - **Message System**: Type-safe message passing between pipeline operators
//! - **Operator Traits**: Source, Transform, and Sink abstractions for building pipelines
//! - **Configuration**: YAML-based pipeline specification and configuration
//! - **Error Handling**: Comprehensive error types for pipeline operations
//!
//! ## Example Usage
//!
//! ```rust
//! use rde_core::{Source, Transform, Sink, PipelineSpec};
//! use tokio::sync::mpsc;
//! use tokio_util::sync::CancellationToken;
//!
//! // Define a simple pipeline with source -> transform -> sink
//! # async fn example() -> anyhow::Result<()> {
//! let (tx, rx) = mpsc::channel(100);
//! let cancel = CancellationToken::new();
//! 
//! // Pipeline operators would implement the Source, Transform, and Sink traits
//! # Ok(())
//! # }
//! ```

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Type alias for Apache Arrow RecordBatch, representing a batch of columnar data
pub type Batch = RecordBatch;

/// Type alias for the sender side of a message channel between pipeline operators
pub type BatchTx = mpsc::Sender<Message>;

/// Type alias for the receiver side of a message channel between pipeline operators
pub type BatchRx = mpsc::Receiver<Message>;

/// Messages passed between pipeline operators in the RDE system
///
/// This enum represents the different types of messages that can flow through
/// a data pipeline, enabling proper coordination and data flow control.
#[derive(Debug, Clone)]
pub enum Message {
    /// A batch of data records in Apache Arrow format
    /// 
    /// Contains a RecordBatch with columnar data that represents a chunk
    /// of the data stream being processed.
    Batch(Batch),
    
    /// A watermark indicating event time progress
    /// 
    /// Used for time-based operations and windowing. The i64 value represents
    /// the timestamp in epoch milliseconds.
    Watermark(i64),
    
    /// End-of-stream marker
    /// 
    /// Signals that no more data will be sent through this channel,
    /// allowing downstream operators to finalize their processing.
    Eos,
}

/// Error types specific to RDE pipeline operations
///
/// This enum defines the various error conditions that can occur during
/// pipeline execution, providing structured error handling throughout the system.
#[derive(Debug, thiserror::Error)]
pub enum RdeError {
    /// Channel communication error
    /// 
    /// Occurs when a message channel between operators is unexpectedly closed,
    /// typically indicating a downstream operator has failed or been terminated.
    #[error("channel closed: {0}")]
    ChannelClosed(&'static str),
    
    /// Generic error wrapper
    /// 
    /// Wraps any other error type using anyhow::Error for flexible error handling
    /// while maintaining error context and stack traces.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Base trait for all pipeline operators
///
/// This trait defines the common interface that all pipeline operators (Sources, Transforms, and Sinks)
/// must implement. It provides basic metadata about the operator and its output schema.
#[async_trait]
pub trait Operator: Send + Sync {
    /// Returns the unique name/identifier of this operator
    /// 
    /// Used for logging, monitoring, and pipeline graph construction.
    fn name(&self) -> &str;
    
    /// Returns the output schema of this operator
    /// 
    /// Defines the structure of the data that this operator produces,
    /// using Apache Arrow's schema representation for type safety.
    fn schema(&self) -> SchemaRef;
}

/// Trait for data source operators
///
/// Sources are the entry points of data pipelines, responsible for ingesting data
/// from external systems and converting it into the internal message format.
/// 
/// ## Examples
/// - Kafka consumer
/// - CSV file reader
/// - Database connector
/// - HTTP API poller
#[async_trait]
pub trait Source: Operator {
    /// Run the source operator
    /// 
    /// This method starts the source and begins producing data messages.
    /// 
    /// # Arguments
    /// * `tx` - Channel sender for publishing messages to downstream operators
    /// * `cancel` - Cancellation token for graceful shutdown
    /// 
    /// # Returns
    /// Result indicating success or failure of the source operation
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()>;
}

/// Trait for data transformation operators
///
/// Transforms are the processing units of pipelines, responsible for modifying,
/// filtering, aggregating, or enriching data as it flows through the system.
/// 
/// ## Examples
/// - Data cleaning and validation
/// - JSON flattening
/// - SQL transformations
/// - Aggregations and windowing
/// - Schema evolution handling
#[async_trait]
pub trait Transform: Operator {
    /// Run the transform operator
    /// 
    /// This method processes incoming messages and produces transformed output.
    /// 
    /// # Arguments
    /// * `rx` - Channel receiver for consuming input messages
    /// * `tx` - Channel sender for publishing transformed messages
    /// * `cancel` - Cancellation token for graceful shutdown
    /// 
    /// # Returns
    /// Result indicating success or failure of the transform operation
    async fn run(&mut self, rx: BatchRx, tx: BatchTx, cancel: CancellationToken) -> Result<()>;
}

/// Trait for data sink operators
///
/// Sinks are the exit points of data pipelines, responsible for writing processed
/// data to external storage systems or endpoints.
/// 
/// ## Examples
/// - Iceberg table writer
/// - Parquet file writer
/// - Database inserter
/// - Message queue publisher
/// - Console output for debugging
#[async_trait]
pub trait Sink: Operator {
    /// Run the sink operator
    /// 
    /// This method consumes processed messages and writes them to the target system.
    /// 
    /// # Arguments
    /// * `rx` - Channel receiver for consuming input messages
    /// * `cancel` - Cancellation token for graceful shutdown
    /// 
    /// # Returns
    /// Result indicating success or failure of the sink operation
    async fn run(&mut self, rx: BatchRx, cancel: CancellationToken) -> Result<()>;
}

/// Complete pipeline specification loaded from YAML configuration
///
/// This structure defines an entire data processing pipeline, including all operators
/// and their connections. It serves as the blueprint for constructing and executing
/// data pipelines in the RDE system.
/// 
/// # Example YAML Configuration
/// 
/// ```yaml
/// name: "example-pipeline"
/// sources:
///   - type: kafka
///     id: "kafka-source"
///     brokers: "localhost:9092"
///     topic: "input-topic"
/// transforms:
///   - type: passthrough
///     id: "passthrough"
/// sinks:
///   - type: iceberg
///     id: "iceberg-sink"
///     table_name: "output_table"
/// edges:
///   - ["kafka-source", "passthrough"]
///   - ["passthrough", "iceberg-sink"]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    /// Human-readable name of the pipeline
    pub name: String,
    
    /// List of data source specifications
    pub sources: Vec<SourceSpec>,
    
    /// List of data transformation specifications  
    pub transforms: Vec<TransformSpec>,
    
    /// List of data sink specifications
    pub sinks: Vec<SinkSpec>,
    
    /// Directed edges defining data flow between operators
    /// 
    /// Each tuple represents (from_operator_id, to_operator_id)
    pub edges: Vec<(String, String)>,
}

/// Configuration for CSV file data sources
///
/// Defines how to read and process CSV files as input to the pipeline.
/// Supports various CSV formats and batch processing configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvSourceSpec {
    /// Unique identifier for this source operator
    pub id: String,
    
    /// File path or glob pattern for CSV files to process
    /// 
    /// Can be a single file path or a glob pattern to process multiple files.
    /// Examples: "data.csv", "data/*.csv", "s3://bucket/data/*.csv"
    pub path: String,
    
    /// Whether the CSV file has a header row
    /// 
    /// If true, the first row will be used for column names.
    /// If false, columns will be named automatically (col_0, col_1, etc.)
    #[serde(default)]
    pub has_header: bool,
    
    /// Number of rows to process in each batch
    /// 
    /// Controls memory usage and processing granularity.
    /// Larger batches are more efficient but use more memory.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
}

/// Configuration for Kafka data sources
///
/// Defines how to consume messages from Apache Kafka topics and process them
/// as input to the pipeline. Supports schema inference, topic mapping to Iceberg
/// tables, and various Kafka consumer configurations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceSpec {
    /// Unique identifier for this source operator
    pub id: String,
    
    /// Comma-separated list of Kafka broker addresses
    /// 
    /// Example: "localhost:9092" or "broker1:9092,broker2:9092,broker3:9092"
    pub brokers: String,
    
    /// Kafka consumer group ID
    /// 
    /// Used for managing consumer offset tracking and load balancing
    /// across multiple consumer instances.
    pub group_id: String,
    
    /// Name of the Kafka topic to consume from
    pub topic: String,
    
    /// Optional schema configuration for message parsing
    /// 
    /// If not provided, schema will be automatically inferred from incoming messages.
    #[serde(default)]
    pub schema: Option<SchemaConfig>,
    
    /// Optional topic-to-Iceberg table mapping configuration
    /// 
    /// Enables direct streaming from Kafka topics to Iceberg tables with
    /// automatic schema evolution and SQL transformations.
    #[serde(default)]
    pub topic_mapping: Option<TopicMapping>,
}

/// Configuration for direct Kafka topic to Iceberg table mapping
///
/// This powerful feature enables streaming data directly from Kafka topics to Iceberg tables
/// with automatic schema evolution, SQL transformations, and partitioning strategies.
/// It bypasses the need for separate transform and sink operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMapping {
    /// Name of the target Iceberg table
    /// 
    /// The table will be created if it doesn't exist, with schema automatically
    /// inferred from the incoming Kafka messages.
    pub iceberg_table: String,
    
    /// S3-compatible bucket name for storing Iceberg data
    /// 
    /// Example: "iceberg-data" or "my-data-lake"
    pub bucket: String,
    
    /// S3-compatible endpoint URL
    /// 
    /// Examples: 
    /// - Local MinIO: "http://localhost:9000"
    /// - AWS S3: "https://s3.amazonaws.com"
    /// - Custom S3: "https://s3.my-company.com"
    pub endpoint: String,
    
    /// S3 access key for authentication
    pub access_key: String,
    
    /// S3 secret key for authentication
    pub secret_key: String,
    
    /// AWS region for S3 operations
    /// 
    /// Examples: "us-east-1", "eu-west-1", "ap-southeast-1"
    pub region: String,
    
    /// Enable automatic schema evolution
    /// 
    /// When true, new fields in JSON messages will automatically be added to the
    /// Iceberg table schema. Existing field types can be safely promoted
    /// (e.g., int32 to int64, float32 to float64).
    #[serde(default)]
    pub auto_schema_evolution: bool,
    
    /// Optional SQL transformation applied to each message
    /// 
    /// Uses DataFusion SQL syntax. The input data is available as `input_data`.
    /// 
    /// Example:
    /// ```sql
    /// SELECT 
    ///   *,
    ///   CURRENT_TIMESTAMP as ingestion_time,
    ///   DATE(timestamp) as partition_date
    /// FROM input_data
    /// WHERE amount > 0
    /// ```
    #[serde(default)]
    pub sql_transform: Option<String>,
    
    /// Partition columns for optimized queries
    /// 
    /// Specifies which columns to use for partitioning the Iceberg table.
    /// Partitioning improves query performance by allowing predicate pushdown.
    /// 
    /// Example: ["partition_date", "region", "event_type"]
    #[serde(default)]
    pub partition_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    #[serde(default)]
    pub fields: Vec<FieldConfig>,
    #[serde(default)]
    pub auto_infer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    pub data_type: String, // "int64", "float64", "string", "boolean"
    #[serde(default)]
    pub nullable: bool,
}

/// Default batch size for processing operations
/// 
/// Returns the default number of rows to process in a single batch.
/// This provides a good balance between memory usage and processing efficiency.
fn default_batch_rows() -> usize {
    65536
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceSpec {
    #[serde(rename = "file_csv")]
    Csv(CsvSourceSpec),
    #[serde(rename = "kafka")]
    Kafka(KafkaSourceSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransformSpec {
    // fill later; placeholder for wiring
    #[serde(rename = "passthrough")]
    Passthrough { id: String },
    #[serde(rename = "schema_evolution")]
    SchemaEvolution { 
        id: String,
        #[serde(default)]
        auto_infer: bool,
        #[serde(default)]
        strict_mode: bool,
    },
    #[serde(rename = "json_flatten")]
    JsonFlatten { 
        id: String,
        #[serde(default)]
        separator: String,
        #[serde(default)]
        max_depth: usize,
    },
    #[serde(rename = "partition")]
    Partition { 
        id: String,
        partition_by: Vec<String>,
        #[serde(default)]
        partition_format: String,
    },
    #[serde(rename = "sql_transform")]
    SqlTransform { 
        id: String,
        query: String,
        #[serde(default)]
        window_size: usize,
    },
    #[serde(rename = "clean_data")]
    CleanData { 
        id: String,
        #[serde(default)]
        remove_nulls: bool,
        #[serde(default)]
        trim_strings: bool,
        #[serde(default)]
        normalize_case: Option<String>, // "lower", "upper", "title"
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergSinkSpec {
    pub id: String,
    pub table_name: String,
    pub bucket: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkSpec {
    #[serde(rename = "stdout_pretty")]
    Stdout { id: String },
    #[serde(rename = "parquet_dir")]
    ParquetDir { id: String, path: String },
    #[serde(rename = "iceberg")]
    Iceberg(IcebergSinkSpec),
}

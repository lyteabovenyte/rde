use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub type Batch = RecordBatch;
pub type BatchTx = mpsc::Sender<Message>;
pub type BatchRx = mpsc::Receiver<Message>;

#[derive(Debug, Clone)]
pub enum Message {
    Batch(Batch),
    Watermark(i64), // epoch millis
    Eos,            // end-of-stream
}

#[derive(Debug, thiserror::Error)]
pub enum RdeError {
    #[error("channel closed: {0}")]
    ChannelClosed(&'static str),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[async_trait]
pub trait Operator: Send + Sync {
    fn name(&self) -> &str;
    fn schema(&self) -> SchemaRef;
}

#[async_trait]
pub trait Source: Operator {
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()>;
}

#[async_trait]
pub trait Transform: Operator {
    async fn run(&mut self, rx: BatchRx, tx: BatchTx, cancel: CancellationToken) -> Result<()>;
}

#[async_trait]
pub trait Sink: Operator {
    async fn run(&mut self, rx: BatchRx, cancel: CancellationToken) -> Result<()>;
}

/// Graph spec for YAML config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub name: String,
    pub sources: Vec<SourceSpec>,
    pub transforms: Vec<TransformSpec>,
    pub sinks: Vec<SinkSpec>,
    pub edges: Vec<(String, String)>, // from, to
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvSourceSpec {
    pub id: String,
    pub path: String,
    #[serde(default)]
    pub has_header: bool,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceSpec {
    pub id: String,
    pub brokers: String,
    pub group_id: String,
    pub topic: String,
    #[serde(default)]
    pub schema: Option<SchemaConfig>,
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

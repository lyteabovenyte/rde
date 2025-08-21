use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
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

fn default_batch_rows() -> usize {
    65536
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceSpec {
    #[serde(rename = "file_csv")]
    Csv(CsvSourceSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransformSpec {
    // fill later; placeholder for wiring
    #[serde(rename = "passthrough")]
    Passthrough { id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkSpec {
    #[serde(rename = "stdout_pretty")]
    Stdout { id: String },
    #[serde(rename = "parquet_dir")]
    ParquetDir { id: String, path: String },
}

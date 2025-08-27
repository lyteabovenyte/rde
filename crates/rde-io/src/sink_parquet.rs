#![allow(unused)]

use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use async_trait::async_trait;
use rde_core::{BatchRx, Message, Operator, Sink};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;
pub struct ParquetDirSink {
    id: String,
    dir: std::path::PathBuf,
    schema: SchemaRef,
}
impl ParquetDirSink {
    pub fn new(id: String, dir: std::path::PathBuf, schema: SchemaRef) -> Self {
        Self { id, dir, schema }
    }
}
#[async_trait]
impl Operator for ParquetDirSink {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
#[async_trait]
impl Sink for ParquetDirSink {
    async fn run(&mut self, mut rx: BatchRx, _cancel: CancellationToken) -> Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let file_path = self.dir.join(format!("{}.parquet", self.id));
        let file = std::fs::File::create(&file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Batch(b) => {
                    info!("Writing batch with {} rows", b.num_rows());
                    writer.write(&b)?;
                }
                Message::Watermark(_) => {}
                Message::Eos => break,
            }
        }
        writer.close()?; // to ensure that all data is flushed to disk
        Ok(())
    }
}

#![allow(unused)]

use anyhow::{Context, Result};
use datafusion::arrow::csv::reader::ReaderBuilder;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use glob::glob;
use rde_core::CsvSourceSpec;
use rde_core::{BatchTx, Message, Operator, Source};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct CsvSource {
    pub id: String,
    pub schema: SchemaRef,
    pub spec: CsvSourceSpec,
}

impl CsvSource {
    pub fn try_new(spec: CsvSourceSpec) -> Result<Self> {
        Ok(Self {
            id: spec.id.clone(),
            schema: Arc::new(Schema::empty()),
            spec,
        })
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }
}

#[async_trait]
impl Operator for CsvSource {
    fn name(&self) -> &str {
        &self.id
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Source for CsvSource {
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()> {
        let mut paths: Vec<String> = vec![];

        for entry in glob(&self.spec.path).context("glob")? {
            paths.push(entry?.display().to_string());
        }

        if paths.is_empty() {
            anyhow::bail!("no files matched: {}", self.spec.path);
        }

        for (i, p) in paths.iter().enumerate() {
            if cancel.is_cancelled() {
                break;
            }

            let file = std::fs::File::open(&p).with_context(|| format!("open {}", p))?;

            // Use the schema that was already inferred in main.rs
            if i == 0 {
                info!("Using schema: {:?}", self.schema);
            }

            let mut reader = ReaderBuilder::new(self.schema.clone())
                .with_batch_size(self.spec.batch_rows)
                .with_header(self.spec.has_header)
                .build(file)?; // requires schema

            loop {
                if cancel.is_cancelled() {
                    break;
                }
                match reader.next() {
                    Some(Ok(batch)) => {
                        info!("Read batch with {} rows", batch.num_rows());
                        if tx.send(Message::Batch(batch)).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        warn!(error=?e, "csv read error");
                    }
                    None => {
                        info!("CSV reader finished");
                        break;
                    }
                }
            }
        }

        let _ = tx.send(Message::Eos).await; // best-effort
        Ok(())
    }
}

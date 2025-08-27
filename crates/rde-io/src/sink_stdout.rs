#![allow(unused)]

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::StringArray;
use async_trait::async_trait;
use rde_core::{BatchRx, Message, Operator, Sink};
use tokio_util::sync::CancellationToken;
use tracing::info;
pub struct StdoutSink {
    id: String,
    schema: SchemaRef,
}
impl StdoutSink {
    pub fn new(id: String, schema: SchemaRef) -> Self {
        Self { id, schema }
    }
}
#[async_trait]
impl Operator for StdoutSink {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
#[async_trait]
impl Sink for StdoutSink {
    async fn run(&mut self, mut rx: BatchRx, _cancel: CancellationToken) -> anyhow::Result<()> {
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Batch(b) => {
                    // Pretty printing is in arrow’s util; keep minimal here
                    // TODO: Implement pretty printing for batch messages
                    // Display the actual data content
                    for row in 0..b.num_rows() {
                        for col in 0..b.num_columns() {
                            let array = b.column(col);
                            if let Some(value) = array.as_any().downcast_ref::<StringArray>() {
                                let str_val = value.value(row);
                                println!("{}", str_val);
                            }
                        }
                    }
                }
                Message::Watermark(ts) => {
                    println!("watermark={}", ts);
                }
                Message::Eos => break,
            }
        }
        Ok(())
    }
}

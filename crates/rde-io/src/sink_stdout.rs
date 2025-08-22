use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rde_core::{BatchRx, Message, Operator, Sink};
use tokio_util::sync::CancellationToken;
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
                    println!("batch rows={} cols={}", b.num_rows(), b.num_columns());
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

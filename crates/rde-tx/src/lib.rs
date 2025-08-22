use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rde_core::{BatchRx, BatchTx, Message, Operator, Transform};
use tokio_util::sync::CancellationToken;
use tracing::info;
pub struct Passthrough {
    id: String,
    schema: SchemaRef,
}

// no transformation yet -> just passing through
impl Passthrough {
    pub fn new(id: String, schema: SchemaRef) -> Self {
        Self { id, schema }
    }
}

#[async_trait]
impl Operator for Passthrough {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
#[async_trait]
impl Transform for Passthrough {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("Passthrough transform started");
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("Passthrough: received batch with {} rows", batch.num_rows());
                }
                Message::Watermark(_) => {
                    info!("Passthrough: received watermark");
                }
                Message::Eos => {
                    info!("Passthrough: received EOS");
                }
            }
            if tx.send(msg).await.is_err() {
                info!("Passthrough: failed to send message to sink");
                break;
            }
            info!("Passthrough: successfully forwarded message");
        }
        info!("Passthrough transform finished");
        Ok(())
    }
}

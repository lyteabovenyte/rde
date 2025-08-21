use arrow_schema::SchemaRef;
use async_trait::async_trait;
use rde_core::{BatchRx, BatchTx, Message, Operator, Transform};
use tokio_util::sync::CancellationToken;
pub struct Passthrough {
    id: String,
    schema: SchemaRef,
}
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
        while let Some(msg) = rx.recv().await {
            tx.send(msg).await.ok();
        }
        Ok(())
    }
}

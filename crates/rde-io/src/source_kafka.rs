use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message as KafkaMessage;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;
use rde_core::{BatchTx, Message, Operator, Source};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Represents a stream of incoming Kafka messages.
/// For now, we assume JSON payloads (common in data engineering),
/// but this can be generic over Avro/Protobuf/etc.
pub struct KafkaSource {
    pub brokers: String,
    pub group_id: String,
    pub topic: String,
}

impl KafkaSource {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Self {
        KafkaSource {
            brokers: brokers.to_string(),
            group_id: group_id.to_string(),
            topic: topic.to_string(),
        }
    }

    // start consuming messages and yield them as serde_json::Value
    pub async fn stream(&self) -> Result<impl futures::Stream<Item = Value>, KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&[&self.topic])?;

        let (tx, rx) = mpsc::channel(100);

        // Spawn consumer task
        let consumer_task = consumer;
        tokio::spawn(async move {
            let mut stream = consumer_task.stream();
            while let Some(result) = stream.next().await {
                let value = match result {
                    Ok(m) => parse_message(&m).ok(),
                    Err(e) => {
                        warn!("kafka error on consuming message from broker: {:?}", e);
                        None
                    }
                };
                if let Some(val) = value {
                    if tx.send(val).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}

/// Kafka source that implements the Source trait for the pipeline
pub struct KafkaPipelineSource {
    pub id: String,
    pub schema: SchemaRef,
    pub spec: rde_core::KafkaSourceSpec,
}

impl KafkaPipelineSource {
    pub fn new(spec: rde_core::KafkaSourceSpec) -> Self {
        Self {
            id: spec.id.clone(),
            schema: Arc::new(Schema::empty()), // Kafka doesn't have a fixed schema
            spec,
        }
    }
    
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }
}

#[async_trait]
impl Operator for KafkaPipelineSource {
    fn name(&self) -> &str {
        &self.id
    }
    
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Source for KafkaPipelineSource {
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()> {
        let kafka_source = KafkaSource::new(&self.spec.brokers, &self.spec.group_id, &self.spec.topic);
        let mut stream = kafka_source.stream().await?;
        
        while let Some(value) = stream.next().await {
            if cancel.is_cancelled() {
                break;
            }
            
            // Convert JSON value to a simple RecordBatch
            // For now, create a single-column batch with the JSON string
            // TODO
            let json_str = value.to_string();
            let array = arrow_array::StringArray::from(vec![json_str]);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![arrow_schema::Field::new("json_data", arrow_schema::DataType::Utf8, true)])),
                vec![Arc::new(array)],
            )?;
            
            if tx.send(Message::Batch(batch)).await.is_err() {
                break;
            }
        }
        
        let _ = tx.send(Message::Eos).await;
        Ok(())
    }
}

// helper function to parse payload as JSON
// TODO: add transformation logic for the messages
fn parse_message(m: &BorrowedMessage) -> Result<Value, serde_json::Error> {
    if let Some(payload) = m.payload() {
        serde_json::from_slice(payload)
    } else {
        Ok(Value::Null)
    }
}

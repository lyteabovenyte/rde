use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;

use futures::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
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
            .set("bootstrap.server", &self.brokers)
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

// helper function to parse payload as JSON
// TODO: add transformation logic for the messages
fn parse_message(m: &BorrowedMessage) -> Result<Value, serde_json::Error> {
    if let Some(payload) = m.payload() {
        serde_json::from_slice(payload)
    } else {
        Ok(Value::Null)
    }
}

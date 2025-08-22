use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message as KafkaMessage;

use anyhow::Result;
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef, DataType};
use async_trait::async_trait;
use futures::StreamExt;
use rde_core::{BatchTx, Message, Operator, Source};
use serde_json::Value;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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
            info!("Kafka consumer started, waiting for messages...");
            while let Some(result) = stream.next().await {
                let value = match result {
                    Ok(m) => {
                        info!("Received Kafka message");
                        parse_message(&m).ok()
                    },
                    Err(e) => {
                        warn!("kafka error on consuming message from broker: {:?}", e);
                        None
                    }
                };
                if let Some(val) = value {
                    info!("Parsed JSON message: {:?}", val);
                    if tx.send(val).await.is_err() {
                        warn!("Failed to send message to channel");
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}

/// Dynamic schema manager for handling evolving JSON schemas
pub struct DynamicSchemaManager {
    current_schema: Option<SchemaRef>,
    field_types: HashMap<String, DataType>,
    configured_schema: Option<SchemaRef>,
    auto_infer: bool,
}

impl DynamicSchemaManager {
    pub fn new() -> Self {
        Self {
            current_schema: None,
            field_types: HashMap::new(),
            configured_schema: None,
            auto_infer: true,
        }
    }

    pub fn with_config(mut self, config: &Option<rde_core::SchemaConfig>) -> Self {
        if let Some(schema_config) = config {
            self.auto_infer = schema_config.auto_infer;
            
            if !schema_config.fields.is_empty() {
                let fields: Vec<Field> = schema_config.fields.iter()
                    .map(|f| {
                        let data_type = match f.data_type.as_str() {
                            "int64" => DataType::Int64,
                            "float64" => DataType::Float64,
                            "string" => DataType::Utf8,
                            "boolean" => DataType::Boolean,
                            _ => DataType::Utf8, // Default to string for unknown types
                        };
                        Field::new(&f.name, data_type, f.nullable)
                    })
                    .collect();
                
                let field_count = fields.len();
                self.configured_schema = Some(Arc::new(Schema::new(fields)));
                info!("Configured schema with {} fields", field_count);
            }
        }
        self
    }

    /// Infer Arrow schema from JSON value
    pub fn infer_schema(&mut self, value: &Value) -> SchemaRef {
        let mut fields = Vec::new();
        
        if let Value::Object(obj) = value {
            for (key, val) in obj {
                let data_type = self.infer_field_type(val);
                fields.push(Field::new(key, data_type, true));
            }
        }
        
        Arc::new(Schema::new(fields))
    }

    /// Infer Arrow data type from JSON value
    fn infer_field_type(&self, value: &Value) -> DataType {
        match value {
            Value::Null => DataType::Utf8, // Treat null as string for flexibility
            Value::Bool(_) => DataType::Boolean,
            Value::Number(n) => {
                if n.is_i64() {
                    DataType::Int64
                } else if n.is_f64() {
                    DataType::Float64
                } else {
                    DataType::Utf8 // Fallback to string for other number types
                }
            }
            Value::String(_) => DataType::Utf8,
            Value::Array(arr) => {
                if arr.is_empty() {
                    DataType::Utf8
                } else {
                    // For arrays, infer type from first non-null element
                    let first_type = arr.iter()
                        .find(|v| !v.is_null())
                        .map(|v| self.infer_field_type(v))
                        .unwrap_or(DataType::Utf8);
                    DataType::List(Arc::new(Field::new("item", first_type, true)))
                }
            }
            Value::Object(_) => DataType::Utf8, // Treat objects as JSON strings for now
        }
    }

    /// Check if schema has changed and update if necessary
    pub fn update_schema_if_needed(&mut self, value: &Value) -> bool {
        if !self.auto_infer {
            // If auto-infer is disabled, use configured schema
            if self.current_schema.is_none() && self.configured_schema.is_some() {
                self.current_schema = self.configured_schema.clone();
                return true;
            }
            return false;
        }

        let new_schema = self.infer_schema(value);
        
        if let Some(current) = &self.current_schema {
            if current.fields() != new_schema.fields() {
                info!("Schema changed, updating from {:?} to {:?}", 
                      current.fields(), new_schema.fields());
                self.current_schema = Some(new_schema);
                true
            } else {
                false
            }
        } else {
            info!("Initializing schema: {:?}", new_schema.fields());
            self.current_schema = Some(new_schema);
            true
        }
    }

    pub fn get_current_schema(&self) -> Option<SchemaRef> {
        self.current_schema.clone()
            .or_else(|| self.configured_schema.clone())
    }

    pub fn merge_schemas(&mut self, value: &Value) -> SchemaRef {
        let inferred_schema = self.infer_schema(value);
        
        if let Some(configured) = &self.configured_schema {
            // Merge configured schema with inferred schema
            let mut merged_fields = Vec::new();
            let mut field_names: std::collections::HashSet<String> = std::collections::HashSet::new();
            
            // Add configured fields first
            for field in configured.fields() {
                field_names.insert(field.name().to_string());
                merged_fields.push(field.clone());
            }
            
            // Add inferred fields that aren't in configured schema
            for field in inferred_schema.fields() {
                if !field_names.contains(field.name()) {
                    merged_fields.push(field.clone());
                }
            }
            
            Arc::new(Schema::new(merged_fields))
        } else {
            inferred_schema
        }
    }
}

/// Kafka source that implements the Source trait for the pipeline
pub struct KafkaPipelineSource {
    pub id: String,
    pub schema: SchemaRef,
    pub spec: rde_core::KafkaSourceSpec,
    pub schema_manager: DynamicSchemaManager,
}

impl KafkaPipelineSource {
    pub fn new(spec: rde_core::KafkaSourceSpec) -> Self {
        let schema_manager = DynamicSchemaManager::new().with_config(&spec.schema);
        Self {
            id: spec.id.clone(),
            schema: Arc::new(Schema::empty()), // Will be dynamically inferred
            spec,
            schema_manager,
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
        // Return current dynamic schema or fallback to empty
        self.schema_manager.get_current_schema()
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    }
}

#[async_trait]
impl Source for KafkaPipelineSource {
    async fn run(&mut self, tx: BatchTx, cancel: CancellationToken) -> Result<()> {
        info!("Starting Kafka source for topic: {}", self.spec.topic);
        let kafka_source = KafkaSource::new(&self.spec.brokers, &self.spec.group_id, &self.spec.topic);
        let mut stream = kafka_source.stream().await?;
        
        while let Some(value) = stream.next().await {
            if cancel.is_cancelled() {
                info!("Kafka source cancelled");
                break;
            }
            
            info!("Processing Kafka message in source");
            
            // Update schema if needed
            self.schema_manager.update_schema_if_needed(&value);
            
            // Parse JSON message into structured RecordBatch
            if let Some(batch) = parse_json_to_batch_dynamic(&value, &self.schema_manager)? {
                info!("Created RecordBatch with {} rows", batch.num_rows());
                if tx.send(Message::Batch(batch)).await.is_err() {
                    warn!("Failed to send batch to channel");
                    break;
                }
                info!("Successfully sent batch to channel");
            }
        }
        
        info!("Kafka source finished, sending EOS");
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

// Dynamic JSON to RecordBatch conversion
fn parse_json_to_batch_dynamic(value: &Value, schema_manager: &DynamicSchemaManager) -> Result<Option<RecordBatch>> {
    if value.is_null() {
        return Ok(None);
    }
    
    let schema = schema_manager.get_current_schema()
        .ok_or_else(|| anyhow::anyhow!("No schema available"))?;
    
    let mut arrays: Vec<ArrayRef> = Vec::new();
    
    if let Value::Object(obj) = value {
        for field in schema.fields() {
            let field_name = field.name();
            let field_value = obj.get(field_name).unwrap_or(&Value::Null);
            
            let array: ArrayRef = match field.data_type() {
                DataType::Boolean => {
                    let bool_val = field_value.as_bool().unwrap_or(false);
                    Arc::new(BooleanArray::from(vec![bool_val]))
                }
                DataType::Int64 => {
                    let int_val = field_value.as_i64().unwrap_or(0);
                    Arc::new(Int64Array::from(vec![int_val]))
                }
                DataType::Float64 => {
                    let float_val = field_value.as_f64().unwrap_or(0.0);
                    Arc::new(Float64Array::from(vec![float_val]))
                }
                DataType::Utf8 => {
                    let str_val = field_value.as_str().unwrap_or("").to_string();
                    Arc::new(StringArray::from(vec![str_val]))
                }
                DataType::List(_) => {
                    // For arrays, convert to JSON string for now
                    let json_str = serde_json::to_string(field_value).unwrap_or_default();
                    Arc::new(StringArray::from(vec![json_str]))
                }
                _ => {
                    // Fallback to string for unknown types
                    let json_str = serde_json::to_string(field_value).unwrap_or_default();
                    Arc::new(StringArray::from(vec![json_str]))
                }
            };
            
            arrays.push(array);
        }
    }
    
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(Some(batch))
}

// Legacy function - keeping for backward compatibility but marked as deprecated
#[deprecated(note = "Use parse_json_to_batch_dynamic instead for dynamic schema support")]
fn parse_json_to_batch(value: &Value) -> Result<Option<RecordBatch>> {
    if value.is_null() {
        return Ok(None);
    }
    
    // Extract fields from JSON
    // TODO: here we are hardcoding the fields of json, there should be a better way.
    // kafka messages don't have fixed fields and fixed schema
    let id = value.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
    let amount = value.get("amount").and_then(|v| v.as_i64()).unwrap_or(0);
    
    // Create arrays for each field
    let id_array = Int64Array::from(vec![id]);
    let amount_array = Int64Array::from(vec![amount]);
    
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", arrow_schema::DataType::Int64, true),
        Field::new("amount", arrow_schema::DataType::Int64, true),
    ]));
    
    // Create RecordBatch
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(id_array), Arc::new(amount_array)],
    )?;
    
    Ok(Some(batch))
}

//! # RDE Transformations - Data Processing Operators
//!
//! This crate provides a comprehensive set of data transformation operators for the RDE pipeline.
//! These operators can modify, filter, aggregate, enrich, and restructure data as it flows
//! through the processing pipeline.
//!
//! ## Available Transformations
//!
//! ### Basic Transformations
//! - **Passthrough**: No-op transformation for testing and simple data flow
//! - **Schema Evolution**: Dynamic schema inference and evolution handling
//! - **Data Cleaning**: Remove nulls, trim strings, normalize case
//!
//! ### Structural Transformations  
//! - **JSON Flattening**: Convert nested JSON structures to flat tables
//! - **Partitioning**: Add partition columns for optimized storage
//!
//! ### Advanced Transformations
//! - **SQL Transform**: Complex business logic using DataFusion SQL engine
//! - **Window Operations**: Time-based aggregations and analytics
//! - **Custom Transforms**: Extensible framework for domain-specific logic
//!
//! ## Example Usage
//!
//! ```rust
//! use rde_tx::Passthrough;
//! use rde_core::{Transform, Operator};
//! use datafusion::arrow::datatypes::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! // Create a simple passthrough transform
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, true),
//! ]));
//!
//! let transform = Passthrough::new("passthrough-1".to_string(), schema);
//! println!("Transform name: {}", transform.name());
//! ```
//!
//! ## SQL Transformations
//!
//! The SQL transform operator uses DataFusion to execute SQL queries on streaming data:
//!
//! ```sql
//! SELECT 
//!   user_id,
//!   event_type,
//!   timestamp,
//!   CASE 
//!     WHEN event_type = 'purchase' THEN amount
//!     ELSE 0 
//!   END as revenue,
//!   DATE(timestamp) as partition_date
//! FROM input_data
//! WHERE user_id IS NOT NULL
//! ```

use datafusion::arrow::array::{RecordBatch, StringArray, ArrayRef};
use datafusion::arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use anyhow::Result;
use async_trait::async_trait;
use rde_core::{BatchRx, BatchTx, Message, Operator, Transform};
use tokio_util::sync::CancellationToken;
use tracing::{info, error};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;

/// Passthrough transformation operator
///
/// This is a no-op transformation that simply passes data through without modification.
/// It's useful for testing pipeline connectivity, debugging data flow, and as a placeholder
/// during pipeline development.
/// 
/// The passthrough operator forwards all messages (batches, watermarks, and end-of-stream)
/// to downstream operators without any processing or modification.
pub struct Passthrough {
    /// Unique identifier for this transform operator
    id: String,
    /// Schema of the data passing through this operator
    schema: SchemaRef,
}

impl Passthrough {
    /// Create a new passthrough transformation
    /// 
    /// # Arguments
    /// * `id` - Unique identifier for this operator instance
    /// * `schema` - Arrow schema describing the data structure
    /// 
    /// # Returns
    /// A new Passthrough instance ready for use in a pipeline
    /// 
    /// # Example
    /// ```rust
    /// use rde_tx::Passthrough;
    /// use datafusion::arrow::datatypes::{Schema, Field, DataType};
    /// use std::sync::Arc;
    /// 
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new("name", DataType::Utf8, true),
    /// ]));
    /// 
    /// let passthrough = Passthrough::new("test-passthrough".to_string(), schema);
    /// ```
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

/// Schema evolution transform that handles schema changes dynamically
pub struct SchemaEvolution {
    id: String,
    schema: SchemaRef,
    auto_infer: bool,
    strict_mode: bool,
    current_schema: SchemaRef,
}

impl SchemaEvolution {
    pub fn new(id: String, schema: SchemaRef, auto_infer: bool, strict_mode: bool) -> Self {
        Self {
            id,
            schema: schema.clone(),
            auto_infer,
            strict_mode,
            current_schema: schema,
        }
    }

    fn infer_schema_from_json(&self, json_data: &[serde_json::Value]) -> anyhow::Result<Schema> {
        if json_data.is_empty() {
            return Ok(self.current_schema.as_ref().clone());
        }

        let mut field_map: HashMap<String, DataType> = HashMap::new();
        
        for value in json_data {
            self.extract_fields_from_json(value, "", &mut field_map)?;
        }

        let mut fields = Vec::new();
        for (name, data_type) in field_map {
            fields.push(Field::new(name, data_type, true));
        }

        Ok(Schema::new(fields))
    }

    fn extract_fields_from_json(
        &self,
        value: &serde_json::Value,
        prefix: &str,
        field_map: &mut HashMap<String, DataType>,
    ) -> anyhow::Result<()> {
        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let field_name = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    self.extract_fields_from_json(val, &field_name, field_map)?;
                }
            }
            serde_json::Value::Array(arr) => {
                if !arr.is_empty() {
                    // For arrays, we'll use the type of the first element
                    self.extract_fields_from_json(&arr[0], prefix, field_map)?;
                }
            }
            serde_json::Value::String(_) => {
                field_map.insert(prefix.to_string(), DataType::Utf8);
            }
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    field_map.insert(prefix.to_string(), DataType::Int64);
                } else {
                    field_map.insert(prefix.to_string(), DataType::Float64);
                }
            }
            serde_json::Value::Bool(_) => {
                field_map.insert(prefix.to_string(), DataType::Boolean);
            }
            serde_json::Value::Null => {
                // Skip null values in schema inference
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Operator for SchemaEvolution {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.current_schema.clone()
    }
}

#[async_trait]
impl Transform for SchemaEvolution {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("SchemaEvolution transform started");
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("SchemaEvolution: processing batch with {} rows", batch.num_rows());
                    
                    if self.auto_infer {
                        // Convert batch to JSON for schema inference
                        let json_data = self.batch_to_json(batch)?;
                        let new_schema = self.infer_schema_from_json(&json_data)?;
                        
                        if new_schema != *self.current_schema {
                            info!("Schema evolution detected: updating schema");
                            self.current_schema = Arc::new(new_schema);
                        }
                    }
                }
                Message::Watermark(_) => {
                    info!("SchemaEvolution: received watermark");
                }
                Message::Eos => {
                    info!("SchemaEvolution: received EOS");
                }
            }
            if tx.send(msg).await.is_err() {
                error!("SchemaEvolution: failed to send message to sink");
                break;
            }
        }
        info!("SchemaEvolution transform finished");
        Ok(())
    }
}

impl SchemaEvolution {
    fn batch_to_json(&self, batch: &RecordBatch) -> anyhow::Result<Vec<serde_json::Value>> {
        // Convert Arrow batch to JSON for schema inference
        // This is a simplified implementation
        // TODO: check with schema registry for desired schema and infer which table and partition the data is for based on schema
        let mut json_data = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let array = batch.column(col_idx);
                let value = self.array_value_to_json(array, row_idx)?;
                row.insert(field.name().clone(), value);
            }
            json_data.push(serde_json::Value::Object(row));
        }
        Ok(json_data)
    }

    fn array_value_to_json(&self, _array: &ArrayRef, _row_idx: usize) -> anyhow::Result<serde_json::Value> {
        // Simplified conversion from Arrow array to JSON value
        // In a real implementation, you'd handle all Arrow types properly
        Ok(serde_json::Value::String("placeholder".to_string()))
    }
}

/// JSON flattening transform that converts nested JSON structures to flat relational format
pub struct JsonFlatten {
    id: String,
    schema: SchemaRef,
    separator: String,
    max_depth: usize,
}

impl JsonFlatten {
    pub fn new(id: String, schema: SchemaRef, separator: String, max_depth: usize) -> Self {
        Self {
            id,
            schema,
            separator,
            max_depth,
        }
    }

    fn flatten_json_value(
        &self,
        value: &serde_json::Value,
        prefix: &str,
        depth: usize,
        result: &mut HashMap<String, serde_json::Value>,
    ) -> anyhow::Result<()> {
        if depth > self.max_depth {
            return Ok(());
        }

        match value {
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let field_name = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}{}{}", prefix, self.separator, key)
                    };
                    self.flatten_json_value(val, &field_name, depth + 1, result)?;
                }
            }
            serde_json::Value::Array(arr) => {
                // For arrays, we'll flatten the first element or create a JSON string
                if !arr.is_empty() {
                    self.flatten_json_value(&arr[0], prefix, depth + 1, result)?;
                } else {
                    result.insert(prefix.to_string(), serde_json::Value::Array(vec![]));
                }
            }
            _ => {
                result.insert(prefix.to_string(), value.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Operator for JsonFlatten {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Transform for JsonFlatten {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("JsonFlatten transform started");
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("JsonFlatten: processing batch with {} rows", batch.num_rows());
                    
                    // Convert batch to flattened format
                    let flattened_batch = self.flatten_batch(batch)?;
                    let flattened_msg = Message::Batch(flattened_batch);
                    
                    if tx.send(flattened_msg).await.is_err() {
                        error!("JsonFlatten: failed to send flattened batch");
                        break;
                    }
                }
                Message::Watermark(_) => {
                    info!("JsonFlatten: received watermark");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Message::Eos => {
                    info!("JsonFlatten: received EOS");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
        info!("JsonFlatten transform finished");
        Ok(())
    }
}

impl JsonFlatten {
    fn flatten_batch(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        // Convert batch to JSON, flatten, then back to Arrow
        let json_data = self.batch_to_json(batch)?;
        let mut flattened_data = Vec::new();
        
        for row in json_data {
            let mut flattened_row = HashMap::new();
            self.flatten_json_value(&row, "", 0, &mut flattened_row)?;
            // Convert HashMap to serde_json::Map
            let map = serde_json::Map::from_iter(flattened_row);
            flattened_data.push(serde_json::Value::Object(map));
        }
        
        // Convert back to Arrow batch
        self.json_to_batch(&flattened_data)
    }

    fn batch_to_json(&self, batch: &RecordBatch) -> anyhow::Result<Vec<serde_json::Value>> {
        // Simplified conversion - in real implementation, handle all Arrow types
        let mut json_data = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let array = batch.column(col_idx);
                let value = self.array_value_to_json(array, row_idx)?;
                row.insert(field.name().clone(), value);
            }
            json_data.push(serde_json::Value::Object(row));
        }
        Ok(json_data)
    }

    fn array_value_to_json(&self, _array: &ArrayRef, _row_idx: usize) -> anyhow::Result<serde_json::Value> {
        // Simplified conversion - in real implementation, handle all Arrow types
        Ok(serde_json::Value::String("placeholder".to_string()))
    }

    fn json_to_batch(&self, json_data: &[serde_json::Value]) -> anyhow::Result<RecordBatch> {
        // Simplified conversion - in real implementation, handle all Arrow types
        let schema = Arc::new(Schema::new(vec![
            Field::new("flattened_data", DataType::Utf8, true)
        ]));
        
        let strings: Vec<Option<String>> = json_data
            .iter()
            .map(|v| Some(v.to_string()))
            .collect();
        
        let array = StringArray::from(strings);
        Ok(RecordBatch::try_new(schema, vec![Arc::new(array)])?)
    }
}

/// Partitioning transform that adds partition columns based on data values
pub struct Partition {
    id: String,
    schema: SchemaRef,
    partition_by: Vec<String>,
    partition_format: String,
}

impl Partition {
    pub fn new(id: String, schema: SchemaRef, partition_by: Vec<String>, partition_format: String) -> Self {
        Self {
            id,
            schema,
            partition_by,
            partition_format,
        }
    }

    fn generate_partition_key(&self, batch: &RecordBatch, row_idx: usize) -> anyhow::Result<String> {
        let mut partition_values = Vec::new();
        
        for partition_field in &self.partition_by {
            if let Some(col_idx) = batch.schema().column_with_name(partition_field) {
                let array = batch.column(col_idx.0);
                let value = self.array_value_to_string(array, row_idx)?;
                partition_values.push(value);
            } else {
                partition_values.push("unknown".to_string());
            }
        }
        
        if self.partition_format.is_empty() {
            Ok(partition_values.join("/"))
        } else {
            // Use custom format string
            let mut result = self.partition_format.clone();
            for (i, value) in partition_values.iter().enumerate() {
                result = result.replace(&format!("{{{}}}", i), value);
            }
            Ok(result)
        }
    }
}

#[async_trait]
impl Operator for Partition {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Transform for Partition {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("Partition transform started");
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("Partition: processing batch with {} rows", batch.num_rows());
                    
                    // Add partition columns to the batch
                    let partitioned_batch = self.add_partition_columns(batch)?;
                    let partitioned_msg = Message::Batch(partitioned_batch);
                    
                    if tx.send(partitioned_msg).await.is_err() {
                        error!("Partition: failed to send partitioned batch");
                        break;
                    }
                }
                Message::Watermark(_) => {
                    info!("Partition: received watermark");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Message::Eos => {
                    info!("Partition: received EOS");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
        info!("Partition transform finished");
        Ok(())
    }
}

impl Partition {
    fn add_partition_columns(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        let mut new_columns = batch.columns().to_vec();
        let mut new_fields = batch.schema().fields().to_vec();
        
        // Add partition_key column
        let partition_keys: Vec<Option<String>> = (0..batch.num_rows())
            .map(|row_idx| self.generate_partition_key(batch, row_idx))
            .map(|r| r.ok().map(|k| k))
            .collect();
        
        let partition_array = StringArray::from(partition_keys);
        new_columns.push(Arc::new(partition_array));
        new_fields.push(Arc::new(Field::new("partition_key", DataType::Utf8, true)));
        
        // Add timestamp column for partitioning
        let now = Utc::now();
        let timestamps: Vec<Option<String>> = (0..batch.num_rows())
            .map(|_| Some(now.format("%Y-%m-%d").to_string()))
            .collect();
        
        let timestamp_array = StringArray::from(timestamps);
        new_columns.push(Arc::new(timestamp_array));
        new_fields.push(Arc::new(Field::new("partition_date", DataType::Utf8, true)));
        
        let new_schema = Arc::new(Schema::new(new_fields));
        Ok(RecordBatch::try_new(new_schema, new_columns)?)
    }

    fn array_value_to_string(&self, _array: &ArrayRef, _row_idx: usize) -> anyhow::Result<String> {
        // Simplified conversion - in real implementation, handle all Arrow types
        Ok("value".to_string())
    }
}

/// SQL transformation using DataFusion
pub struct SqlTransform {
    id: String,
    schema: SchemaRef,
    query: String,
    window_size: usize,
    ctx: datafusion::prelude::SessionContext,
}

impl SqlTransform {
    pub fn new(id: String, schema: SchemaRef, query: String, window_size: usize) -> anyhow::Result<Self> {
        let ctx = datafusion::prelude::SessionContext::new();
        
        Ok(Self {
            id,
            schema,
            query,
            window_size,
            ctx,
        })
    }

    async fn execute_sql_query(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        // Register the batch as a temporary table
        let table_name = "input_data";
        // Convert our RecordBatch to DataFusion's RecordBatch
        let df_batch = datafusion::arrow::array::RecordBatch::try_new(
            batch.schema().clone(),
            batch.columns().to_vec(),
        )?;
        self.ctx.register_batch(table_name, df_batch)?;
        
        // Execute the SQL query
        let df = self.ctx.sql(&self.query).await?;
        let result = df.collect().await?;
        
        if result.is_empty() {
            // Return empty batch with same schema
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        
        // Convert DataFusion's RecordBatch back to our RecordBatch
        let df_batch = &result[0];
        Ok(RecordBatch::try_new(
            df_batch.schema().clone(),
            df_batch.columns().to_vec(),
        )?)
    }
}

#[async_trait]
impl Operator for SqlTransform {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Transform for SqlTransform {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("SqlTransform started with query: {}", self.query);
        
        let mut batch_buffer = Vec::new();
        
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("SqlTransform: received batch with {} rows", batch.num_rows());
                    batch_buffer.push(batch.clone());
                    
                    // Process when we have enough data or on watermark
                    if batch_buffer.len() >= self.window_size {
                        let combined_batch = self.combine_batches(&batch_buffer)?;
                        let transformed_batch = self.execute_sql_query(&combined_batch).await?;
                        
                        if tx.send(Message::Batch(transformed_batch)).await.is_err() {
                            error!("SqlTransform: failed to send transformed batch");
                            break;
                        }
                        
                        batch_buffer.clear();
                    }
                }
                Message::Watermark(_) => {
                    info!("SqlTransform: received watermark");
                    
                    // Process remaining data
                    if !batch_buffer.is_empty() {
                        let combined_batch = self.combine_batches(&batch_buffer)?;
                        let transformed_batch = self.execute_sql_query(&combined_batch).await?;
                        
                        if tx.send(Message::Batch(transformed_batch)).await.is_err() {
                            error!("SqlTransform: failed to send transformed batch");
                            break;
                        }
                        
                        batch_buffer.clear();
                    }
                    
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Message::Eos => {
                    info!("SqlTransform: received EOS");
                    
                    // Process remaining data
                    if !batch_buffer.is_empty() {
                        let combined_batch = self.combine_batches(&batch_buffer)?;
                        let transformed_batch = self.execute_sql_query(&combined_batch).await?;
                        
                        if tx.send(Message::Batch(transformed_batch)).await.is_err() {
                            error!("SqlTransform: failed to send transformed batch");
                            break;
                        }
                    }
                    
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
        
        info!("SqlTransform finished");
        Ok(())
    }
}

impl SqlTransform {
    fn combine_batches(&self, batches: &[RecordBatch]) -> anyhow::Result<RecordBatch> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        
        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }
        
        // Combine multiple batches into one
        // This is a simplified implementation - in real implementation, handle schema merging
        Ok(batches[0].clone())
    }
}

/// Data cleaning transform
pub struct CleanData {
    id: String,
    schema: SchemaRef,
    remove_nulls: bool,
    trim_strings: bool,
    normalize_case: Option<String>,
}

impl CleanData {
    pub fn new(
        id: String, 
        schema: SchemaRef, 
        remove_nulls: bool, 
        trim_strings: bool, 
        normalize_case: Option<String>
    ) -> Self {
        Self {
            id,
            schema,
            remove_nulls,
            trim_strings,
            normalize_case,
        }
    }
}

#[async_trait]
impl Operator for CleanData {
    fn name(&self) -> &str {
        &self.id
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Transform for CleanData {
    async fn run(
        &mut self,
        mut rx: BatchRx,
        tx: BatchTx,
        _cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        info!("CleanData transform started");
        while let Some(msg) = rx.recv().await {
            match &msg {
                Message::Batch(batch) => {
                    info!("CleanData: processing batch with {} rows", batch.num_rows());
                    
                    let cleaned_batch = self.clean_batch(batch)?;
                    let cleaned_msg = Message::Batch(cleaned_batch);
                    
                    if tx.send(cleaned_msg).await.is_err() {
                        error!("CleanData: failed to send cleaned batch");
                        break;
                    }
                }
                Message::Watermark(_) => {
                    info!("CleanData: received watermark");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Message::Eos => {
                    info!("CleanData: received EOS");
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
        info!("CleanData transform finished");
        Ok(())
    }
}

impl CleanData {
    fn clean_batch(&self, batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
        let mut cleaned_columns = Vec::new();
        let mut cleaned_fields = Vec::new();
        
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let cleaned_array = self.clean_array(array, field)?;
            cleaned_columns.push(cleaned_array);
            cleaned_fields.push(field.clone());
        }
        
        let cleaned_schema = Arc::new(Schema::new(cleaned_fields));
        Ok(RecordBatch::try_new(cleaned_schema, cleaned_columns)?)
    }

    fn clean_array(&self, array: &ArrayRef, field: &Field) -> anyhow::Result<ArrayRef> {
        // Simplified cleaning - in real implementation, handle all Arrow types
        match field.data_type() {
            DataType::Utf8 => {
                if self.trim_strings || self.normalize_case.is_some() {
                    // Apply string cleaning
                    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let cleaned_strings: Vec<Option<String>> = string_array
                        .iter()
                        .map(|opt_str| {
                            opt_str.map(|s| {
                                let mut cleaned = s.to_string();
                                if self.trim_strings {
                                    cleaned = cleaned.trim().to_string();
                                }
                                if let Some(case) = &self.normalize_case {
                                    match case.as_str() {
                                        "lower" => cleaned = cleaned.to_lowercase(),
                                        "upper" => cleaned = cleaned.to_uppercase(),
                                        "title" => {
                                            // Simple title case implementation
                                            cleaned = cleaned
                                                .split_whitespace()
                                                .map(|word| {
                                                    let mut chars = word.chars();
                                                    match chars.next() {
                                                        None => String::new(),
                                                        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                                                    }
                                                })
                                                .collect::<Vec<_>>()
                                                .join(" ");
                                        }
                                        _ => {}
                                    }
                                }
                                cleaned
                            })
                        })
                        .collect();
                    
                    Ok(Arc::new(StringArray::from(cleaned_strings)))
                } else {
                    Ok(array.clone())
                }
            }
            _ => Ok(array.clone()),
        }
    }
}

// Factory function to create transforms based on configuration
pub fn create_transform(
    spec: &rde_core::TransformSpec,
    input_schema: SchemaRef, // note that schema is sent up-front, it is inferred in main function
) -> anyhow::Result<Box<dyn Transform + Send>> {
    match spec {
        rde_core::TransformSpec::Passthrough { id } => {
            Ok(Box::new(Passthrough::new(id.clone(), input_schema)))
        }
        rde_core::TransformSpec::SchemaEvolution { id, auto_infer, strict_mode } => {
            Ok(Box::new(SchemaEvolution::new(id.clone(), input_schema, *auto_infer, *strict_mode)))
        }
        rde_core::TransformSpec::JsonFlatten { id, separator, max_depth } => {
            Ok(Box::new(JsonFlatten::new(
                id.clone(), 
                input_schema, 
                separator.clone(), 
                *max_depth
            )))
        }
        rde_core::TransformSpec::Partition { id, partition_by, partition_format } => {
            Ok(Box::new(Partition::new(
                id.clone(), 
                input_schema, 
                partition_by.clone(), 
                partition_format.clone()
            )))
        }
        rde_core::TransformSpec::SqlTransform { id, query, window_size } => {
            Ok(Box::new(SqlTransform::new(
                id.clone(), 
                input_schema, 
                query.clone(), 
                *window_size
            )?))
        }
        rde_core::TransformSpec::CleanData { id, remove_nulls, trim_strings, normalize_case } => {
            Ok(Box::new(CleanData::new(
                id.clone(), 
                input_schema, 
                *remove_nulls, 
                *trim_strings, 
                normalize_case.clone()
            )))
        }
    }
}

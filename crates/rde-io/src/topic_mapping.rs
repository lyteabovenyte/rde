use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef, DataType};
use datafusion::prelude::*;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser as SqlParser;
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore, PutOptions,
};
use rde_core::TopicMapping;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Manages the mapping between Kafka topics and Iceberg tables
pub struct TopicMappingManager {
    mapping: TopicMapping,
    object_store: Option<Arc<dyn ObjectStore>>,
    current_schema: Option<SchemaRef>,
    datafusion_ctx: SessionContext,
}

impl TopicMappingManager {
    pub fn new(mapping: TopicMapping) -> Self {
        let datafusion_ctx = SessionContext::new();
        Self {
            mapping,
            object_store: None,
            current_schema: None,
            datafusion_ctx,
        }
    }

    /// Initialize the object store connection
    pub async fn initialize(&mut self) -> Result<()> {
        let object_store = AmazonS3Builder::new()
            .with_endpoint(&self.mapping.endpoint)
            .with_access_key_id(&self.mapping.access_key)
            .with_secret_access_key(&self.mapping.secret_key)
            .with_region(&self.mapping.region)
            .with_allow_http(true)
            .build()?;

        self.object_store = Some(Arc::new(object_store));
        
        // Try to load existing table schema
        self.load_existing_schema().await?;
        
        info!("Topic mapping manager initialized for table: {}", self.mapping.iceberg_table);
        Ok(())
    }

    /// Load existing schema from Iceberg table metadata
    async fn load_existing_schema(&mut self) -> Result<()> {
        if let Some(object_store) = &self.object_store {
            let metadata_path = format!("{}/metadata/metadata.json", self.mapping.iceberg_table);
            let path = ObjectPath::from(metadata_path.as_str());
            
            match object_store.get(&path).await {
                Ok(data) => {
                    let metadata_json = String::from_utf8(data.bytes().await?.to_vec())?;
                    let metadata: IcebergTableMetadata = serde_json::from_str(&metadata_json)?;
                    
                    if let Some(schema) = metadata.schemas.get(&metadata.current_schema_id) {
                        self.current_schema = Some(self.convert_iceberg_schema_to_arrow(schema));
                        info!("Loaded existing schema with {} fields", schema.fields.len());
                    }
                }
                Err(_) => {
                    info!("No existing table metadata found, will create new table");
                }
            }
        }
        Ok(())
    }

    /// Convert Iceberg schema to Arrow schema
    fn convert_iceberg_schema_to_arrow(&self, iceberg_schema: &IcebergSchema) -> SchemaRef {
        let fields: Vec<Field> = iceberg_schema.fields.iter()
            .map(|field| {
                let data_type = self.convert_iceberg_type_to_arrow(&field.field_type);
                Field::new(&field.name, data_type, !field.required)
            })
            .collect();
        
        Arc::new(Schema::new(fields))
    }

    /// Convert Iceberg type to Arrow type
    fn convert_iceberg_type_to_arrow(&self, iceberg_type: &str) -> DataType {
        match iceberg_type {
            "long" => DataType::Int64,
            "int" => DataType::Int32,
            "double" => DataType::Float64,
            "float" => DataType::Float32,
            "boolean" => DataType::Boolean,
            "string" => DataType::Utf8,
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" => DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            _ => DataType::Utf8, // Default to string for unknown types
        }
    }

    /// Convert Arrow type to Iceberg type
    fn convert_arrow_type_to_iceberg(&self, arrow_type: &DataType) -> String {
        match arrow_type {
            DataType::Int64 => "long".to_string(),
            DataType::Int32 => "int".to_string(),
            DataType::Float64 => "double".to_string(),
            DataType::Float32 => "float".to_string(),
            DataType::Boolean => "boolean".to_string(),
            DataType::Utf8 => "string".to_string(),
            DataType::Binary => "binary".to_string(),
            DataType::Date32 => "date".to_string(),
            DataType::Timestamp(_, _) => "timestamp".to_string(),
            _ => "string".to_string(), // Default to string for unknown types
        }
    }

    /// Check if schema evolution is needed and update if necessary
    pub async fn evolve_schema_if_needed(&mut self, new_schema: &SchemaRef) -> Result<bool> {
        if !self.mapping.auto_schema_evolution {
            return Ok(false);
        }

        let schema_changed = if let Some(current) = &self.current_schema {
            current.fields() != new_schema.fields()
        } else {
            true // No existing schema, so it's a change
        };

        if schema_changed {
            info!("Schema evolution detected for table: {}", self.mapping.iceberg_table);
            
            // Create new schema by merging existing and new fields
            let merged_schema = self.merge_schemas(new_schema).await?;
            
            // Update Iceberg table metadata
            self.update_table_schema(&merged_schema).await?;
            
            self.current_schema = Some(merged_schema);
            info!("Schema evolution completed");
            return Ok(true);
        }

        Ok(false)
    }

    /// Merge existing schema with new schema
    async fn merge_schemas(&self, new_schema: &SchemaRef) -> Result<SchemaRef> {
        let mut field_map: HashMap<String, Field> = HashMap::new();
        
        // Add existing fields
        if let Some(current) = &self.current_schema {
            for field in current.fields() {
                field_map.insert(field.name().to_string(), field.as_ref().clone());
            }
        }
        
        // Add new fields or update existing ones
        for field in new_schema.fields() {
            if let Some(existing_field) = field_map.get(field.name()) {
                // If field exists, check if we need to promote the type
                let promoted_type = self.promote_field_type(existing_field.data_type(), field.data_type());
                field_map.insert(field.name().to_string(), Field::new(field.name(), promoted_type, field.is_nullable()));
            } else {
                // New field
                field_map.insert(field.name().to_string(), field.as_ref().clone());
            }
        }
        
        let merged_fields: Vec<Field> = field_map.into_values().collect();
        Ok(Arc::new(Schema::new(merged_fields)))
    }

    /// Promote field type to handle schema evolution
    fn promote_field_type(&self, existing: &DataType, new: &DataType) -> DataType {
        match (existing, new) {
            (DataType::Int32, DataType::Int64) => DataType::Int64,
            (DataType::Float32, DataType::Float64) => DataType::Float64,
            (DataType::Int32, DataType::Float64) => DataType::Float64,
            (DataType::Int64, DataType::Float64) => DataType::Float64,
            _ => existing.clone(), // Keep existing type if no promotion needed
        }
    }

    /// Update Iceberg table metadata with new schema
    async fn update_table_schema(&self, new_schema: &SchemaRef) -> Result<()> {
        if let Some(object_store) = &self.object_store {
            let metadata_path = format!("{}/metadata/metadata.json", self.mapping.iceberg_table);
            let path = ObjectPath::from(metadata_path.as_str());
            
            // Load existing metadata
            let metadata_json = match object_store.get(&path).await {
                Ok(data) => String::from_utf8(data.bytes().await?.to_vec())?,
                Err(_) => {
                    // Create new metadata if it doesn't exist
                    self.create_initial_metadata(new_schema).await?
                }
            };
            
            let mut metadata: IcebergTableMetadata = serde_json::from_str(&metadata_json)?;
            
            // Create new schema entry
            let new_schema_id = metadata.last_column_id + 1;
            let iceberg_fields: Vec<IcebergField> = new_schema.fields().iter()
                .enumerate()
                .map(|(i, field)| IcebergField {
                    id: i as i32 + 1,
                    name: field.name().to_string(),
                    field_type: self.convert_arrow_type_to_iceberg(field.data_type()),
                    required: !field.is_nullable(),
                    doc: None,
                })
                .collect();
            
            let iceberg_schema = IcebergSchema {
                schema_id: new_schema_id,
                fields: iceberg_fields,
            };
            
            // Update metadata
            metadata.schemas.insert(new_schema_id, iceberg_schema);
            metadata.current_schema_id = new_schema_id;
            metadata.last_column_id = new_schema.fields().len() as i32;
            metadata.last_updated_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as i64;
            
            // Write updated metadata
            let updated_metadata_json = serde_json::to_string_pretty(&metadata)?;
            let put_options = PutOptions::default();
            object_store.put_opts(&path, updated_metadata_json.into(), put_options).await?;
            
            info!("Updated table schema with {} fields", new_schema.fields().len());
        }
        
        Ok(())
    }

    /// Create initial table metadata
    async fn create_initial_metadata(&self, schema: &SchemaRef) -> Result<String> {
        let table_uuid = uuid::Uuid::new_v4().to_string();
        let location = format!("s3://{}/{}", self.mapping.bucket, self.mapping.iceberg_table);
        
        let iceberg_fields: Vec<IcebergField> = schema.fields().iter()
            .enumerate()
            .map(|(i, field)| IcebergField {
                id: i as i32 + 1,
                name: field.name().to_string(),
                field_type: self.convert_arrow_type_to_iceberg(field.data_type()),
                required: !field.is_nullable(),
                doc: None,
            })
            .collect();
        
        let iceberg_schema = IcebergSchema {
            schema_id: 1,
            fields: iceberg_fields,
        };
        
        let metadata = IcebergTableMetadata {
            format_version: 1,
            table_uuid,
            location,
            last_updated_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as i64,
            last_column_id: schema.fields().len() as i32,
            schema: iceberg_schema.clone(),
            partition_specs: HashMap::new(),
            properties: HashMap::new(),
            current_schema_id: 1,
            schemas: {
                let mut schemas = HashMap::new();
                schemas.insert(1, iceberg_schema);
                schemas
            },
            current_spec_id: 0,
            specs: HashMap::new(),
            last_partition_id: 0,
            default_spec_id: 0,
            default_sort_order_id: 0,
            sort_orders: HashMap::new(),
            snapshots: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            current_snapshot_id: None,
            refs: HashMap::new(),
        };
        
        Ok(serde_json::to_string_pretty(&metadata)?)
    }

    /// Apply topic-specific SQL transformation
    pub async fn apply_sql_transform(&self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if let Some(sql_query) = &self.mapping.sql_transform {
            info!("Applying SQL transform: {}", sql_query);
            
            // Register the batch as a temporary table
            let table_name = "input_data";
            self.datafusion_ctx.register_batch(table_name, batch)?;
            
            // Execute the SQL query
            let df = self.datafusion_ctx.sql(sql_query).await?;
            let result = df.collect().await?;
            
            if result.is_empty() {
                Ok(None)
            } else {
                // For now, just return the first batch
                // TODO: Implement proper batch concatenation
                Ok(Some(result[0].clone()))
            }
        } else {
            Ok(Some(batch))
        }
    }

    /// Get the current schema
    pub fn get_current_schema(&self) -> Option<SchemaRef> {
        self.current_schema.clone()
    }

    /// Get the table name
    pub fn get_table_name(&self) -> &str {
        &self.mapping.iceberg_table
    }

    /// Get the object store
    pub fn get_object_store(&self) -> Option<Arc<dyn ObjectStore>> {
        self.object_store.clone()
    }
}

// Iceberg metadata structures (copied from sink_iceberg.rs)
#[derive(Debug, Serialize, Deserialize)]
struct IcebergTableMetadata {
    format_version: i32,
    table_uuid: String,
    location: String,
    last_updated_ms: i64,
    last_column_id: i32,
    schema: IcebergSchema,
    partition_specs: HashMap<i32, IcebergPartitionSpec>,
    properties: HashMap<String, String>,
    current_schema_id: i32,
    schemas: HashMap<i32, IcebergSchema>,
    current_spec_id: i32,
    specs: HashMap<i32, IcebergPartitionSpec>,
    last_partition_id: i32,
    default_spec_id: i32,
    default_sort_order_id: i32,
    sort_orders: HashMap<i32, IcebergSortOrder>,
    snapshots: HashMap<i64, IcebergSnapshot>,
    snapshot_log: Vec<IcebergSnapshotLogEntry>,
    metadata_log: Vec<IcebergMetadataLogEntry>,
    current_snapshot_id: Option<i64>,
    refs: HashMap<String, IcebergSnapshotRef>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSchema {
    schema_id: i32,
    fields: Vec<IcebergField>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    required: bool,
    doc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergPartitionSpec {
    spec_id: i32,
    fields: Vec<IcebergPartitionField>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergPartitionField {
    source_id: i32,
    field_id: i32,
    name: String,
    transform: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSnapshot {
    snapshot_id: i64,
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    timestamp_ms: i64,
    manifest_list: String,
    summary: IcebergSnapshotSummary,
    schema_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSnapshotSummary {
    operation: String,
    #[serde(rename = "added-data-files")]
    added_data_files: i32,
    #[serde(rename = "deleted-data-files")]
    deleted_data_files: i32,
    #[serde(rename = "total-records")]
    total_records: i64,
    #[serde(rename = "added-records")]
    added_records: i64,
    #[serde(rename = "deleted-records")]
    deleted_records: i64,
    #[serde(rename = "added-files-size")]
    added_files_size: i64,
    #[serde(rename = "deleted-files-size")]
    deleted_files_size: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSnapshotLogEntry {
    timestamp_ms: i64,
    snapshot_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergMetadataLogEntry {
    timestamp_ms: i64,
    metadata_file: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSnapshotRef {
    snapshot_id: i64,
    #[serde(rename = "type")]
    ref_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSortOrder {
    order_id: i32,
    fields: Vec<IcebergSortField>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergSortField {
    source_id: i32,
    transform: String,
    direction: String,
    null_order: String,
}

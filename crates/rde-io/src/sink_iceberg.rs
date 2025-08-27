#![allow(unused)]

use anyhow::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore, PutOptions,
};
use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use rde_core::{BatchRx, Message, Operator, Sink};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;

// Iceberg metadata structures
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
    min_snapshots_to_keep: Option<i32>,
    max_snapshot_age_ms: Option<i64>,
    max_ref_age_ms: Option<i64>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergManifestList {
    manifests: Vec<IcebergManifestFile>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergManifestFile {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32, // 0 for data, 1 for deletes
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshots: Vec<i64>,
    added_data_files: i32,
    existing_data_files: i32,
    deleted_data_files: i32,
    added_rows: i64,
    existing_rows: i64,
    deleted_rows: i64,
    partitions: Vec<IcebergPartitionSummary>,
    key_metadata: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergPartitionSummary {
    contains_null: bool,
    contains_nan: bool,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergManifest {
    schema_id: i32,
    schema: IcebergSchema,
    partition_spec_id: i32,
    content: i32,
    sequence_number: i64,
    min_sequence_number: i64,
    entries: Vec<IcebergManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergManifestEntry {
    status: i32, // 1 for existing, 2 for added
    snapshot_id: i64,
    data_file: IcebergDataFile,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct IcebergDataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: HashMap<String, String>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: HashMap<i32, i64>,
    value_counts: HashMap<i32, i64>,
    null_value_counts: HashMap<i32, i64>,
    nan_value_counts: HashMap<i32, i64>,
    distinct_counts: HashMap<i32, i64>,
    lower_bounds: HashMap<i32, Vec<u8>>,
    upper_bounds: HashMap<i32, Vec<u8>>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Vec<i64>,
    equality_ids: Vec<i32>,
    sort_order_id: Option<i32>,
}

pub struct IcebergSink {
    id: String,
    schema: SchemaRef,
    table_name: String,
    bucket: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
    object_store: Option<Arc<dyn ObjectStore>>,
    table_metadata: Option<IcebergTableMetadata>,
    current_snapshot_id: i64,
    data_files: Vec<IcebergDataFile>,
}

impl IcebergSink {
    pub fn new(
        id: String,
        schema: SchemaRef,
        table_name: String,
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
        region: String,
    ) -> Self {
        Self {
            id,
            schema,
            table_name,
            bucket,
            endpoint,
            access_key,
            secret_key,
            region,
            object_store: None,
            table_metadata: None,
            current_snapshot_id: 1,
            data_files: Vec::new(),
        }
    }

    async fn initialize_object_store(&mut self) -> Result<()> {
        if self.object_store.is_none() {
            info!("Initializing object store for MinIO...");
            
            let object_store = AmazonS3Builder::new()
                .with_endpoint(&self.endpoint)
                .with_access_key_id(&self.access_key)
                .with_secret_access_key(&self.secret_key)
                .with_region(&self.region)
                .with_allow_http(true)
                .with_bucket_name(&self.bucket)
                .build()
                .map_err(|e| {
                    anyhow::anyhow!("Failed to create S3-compatible object store: {}", e)
                })?;

            self.object_store = Some(Arc::new(object_store));
            info!("Successfully initialized S3-compatible object store for MinIO");
        } else {
            info!("Object store already initialized");
        }
        Ok(())
    }

    async fn write_parquet_file(&self, batch: &RecordBatch, file_path: &str) -> Result<u64> {
        let object_store = self.object_store.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Object store not initialized")
        })?;

        // Create parquet writer
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buffer, self.schema.clone(), Some(props))?;
        
        writer.write(batch)?;
        writer.close()?;

        // Write to object store
        let path = ObjectPath::from(file_path);
        let put_options = PutOptions::default();
        object_store.put_opts(&path, buffer.clone().into(), put_options).await?;

        Ok(buffer.len() as u64)
    }

    fn convert_arrow_schema_to_iceberg(&self) -> IcebergSchema {
        let mut fields = Vec::new();
        let mut field_id = 1;
        
        for field in self.schema.fields() {
            fields.push(IcebergField {
                id: field_id,
                name: field.name().clone(),
                field_type: self.convert_arrow_type_to_iceberg(field.data_type()),
                required: !field.is_nullable(),
                doc: None,
            });
            field_id += 1;
        }

        IcebergSchema {
            schema_id: 0,
            fields,
        }
    }

    fn convert_arrow_type_to_iceberg(&self, arrow_type: &datafusion::arrow::datatypes::DataType) -> String {
        match arrow_type {
            datafusion::arrow::datatypes::DataType::Int64 => "long".to_string(),
            datafusion::arrow::datatypes::DataType::Int32 => "int".to_string(),
            datafusion::arrow::datatypes::DataType::Float64 => "double".to_string(),
            datafusion::arrow::datatypes::DataType::Float32 => "float".to_string(),
            datafusion::arrow::datatypes::DataType::Utf8 => "string".to_string(),
            datafusion::arrow::datatypes::DataType::Boolean => "boolean".to_string(),
            datafusion::arrow::datatypes::DataType::Timestamp(_, _) => "timestamp".to_string(),
            datafusion::arrow::datatypes::DataType::Date32 => "date".to_string(),
            _ => "string".to_string(), // Default fallback
        }
    }

    async fn create_or_load_table_metadata(&mut self) -> Result<()> {
        let metadata_path = format!("{}/metadata/metadata.json", self.table_name);
        
        if let Some(object_store) = &self.object_store {
            // Try to load existing metadata
            let path = ObjectPath::from(metadata_path.as_str());
            match object_store.get(&path).await {
                Ok(data) => {
                    let metadata_str = String::from_utf8(data.bytes().await?.to_vec())?;
                    self.table_metadata = Some(serde_json::from_str(&metadata_str)?);
                    info!("Loaded existing table metadata");
                }
                Err(_) => {
                    // Create new table metadata
                    info!("Creating new table metadata");
                    self.create_new_table_metadata().await?;
                }
            }
        }
        Ok(())
    }

    async fn create_new_table_metadata(&mut self) -> Result<()> {
        let table_uuid = Uuid::new_v4().to_string();
        let location = format!("{}/{}", self.bucket, self.table_name);
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        
        let schema = self.convert_arrow_schema_to_iceberg();
        let mut schemas = HashMap::new();
        schemas.insert(0, schema.clone());
        
        let partition_spec = IcebergPartitionSpec {
            spec_id: 0,
            fields: Vec::new(), // No partitioning for now
        };
        let mut specs = HashMap::new();
        specs.insert(0, partition_spec);
        
        let mut properties = HashMap::new();
        properties.insert("write.format.default".to_string(), "parquet".to_string());
        properties.insert("write.metadata.delete-after-commit.enabled".to_string(), "true".to_string());
        properties.insert("write.metadata.previous-versions-max".to_string(), "1".to_string());
        
        self.table_metadata = Some(IcebergTableMetadata {
            format_version: 2,
            table_uuid,
            location,
            last_updated_ms: now,
            last_column_id: self.schema.fields().len() as i32,
            schema: schema.clone(),
            partition_specs: specs.clone(),
            properties,
            current_schema_id: 0,
            schemas,
            current_spec_id: 0,
            specs,
            last_partition_id: 0,
            default_spec_id: 0,
            default_sort_order_id: 0,
            sort_orders: HashMap::new(),
            snapshots: HashMap::new(),
            snapshot_log: Vec::new(),
            metadata_log: Vec::new(),
            current_snapshot_id: None,
            refs: HashMap::new(),
        });

        // Write initial metadata
        self.write_table_metadata().await?;
        Ok(())
    }

    async fn write_table_metadata(&self) -> Result<()> {
        if let (Some(metadata), Some(object_store)) = (&self.table_metadata, &self.object_store) {
            let metadata_json = serde_json::to_string_pretty(metadata)?;
            let metadata_path = format!("{}/metadata/metadata.json", self.table_name);
            let path = ObjectPath::from(metadata_path.as_str());
            let put_options = PutOptions::default();
            
            object_store.put_opts(&path, metadata_json.into(), put_options).await?;
            info!("Written table metadata to: {}", metadata_path);
        }
        Ok(())
    }

    async fn create_manifest_file(&self, data_files: &[IcebergDataFile]) -> Result<String> {
        if let Some(object_store) = &self.object_store {
            let manifest_id = Uuid::new_v4();
            let manifest_path = format!("{}/metadata/{}.avro", self.table_name, manifest_id);
            
            let manifest = IcebergManifest {
                schema_id: 0,
                schema: self.convert_arrow_schema_to_iceberg(),
                partition_spec_id: 0,
                content: 0, // Data content
                sequence_number: self.current_snapshot_id,
                min_sequence_number: self.current_snapshot_id,
                entries: data_files.iter().map(|df| IcebergManifestEntry {
                    status: 2, // Added
                    snapshot_id: self.current_snapshot_id,
                    data_file: df.clone(),
                }).collect(),
            };

            // For now, write as JSON (in production, this should be Avro)
            let manifest_json = serde_json::to_string_pretty(&manifest)?;
            let path = ObjectPath::from(manifest_path.as_str());
            let put_options = PutOptions::default();
            
            object_store.put_opts(&path, manifest_json.into(), put_options).await?;
            info!("Created manifest file: {}", manifest_path);
            
            Ok(manifest_path)
        } else {
            Err(anyhow::anyhow!("Object store not initialized"))
        }
    }

    async fn create_snapshot(&mut self, manifest_path: String) -> Result<()> {
        if let Some(metadata) = &mut self.table_metadata {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
            
            let snapshot = IcebergSnapshot {
                snapshot_id: self.current_snapshot_id,
                parent_snapshot_id: metadata.current_snapshot_id,
                sequence_number: self.current_snapshot_id,
                timestamp_ms: now,
                manifest_list: manifest_path,
                summary: IcebergSnapshotSummary {
                    operation: "append".to_string(),
                    added_data_files: self.data_files.len() as i32,
                    deleted_data_files: 0,
                    total_records: self.data_files.iter().map(|df| df.record_count).sum(),
                    added_records: self.data_files.iter().map(|df| df.record_count).sum(),
                    deleted_records: 0,
                    added_files_size: self.data_files.iter().map(|df| df.file_size_in_bytes).sum(),
                    deleted_files_size: 0,
                },
                schema_id: 0,
            };

            metadata.snapshots.insert(self.current_snapshot_id, snapshot);
            metadata.current_snapshot_id = Some(self.current_snapshot_id);
            metadata.last_updated_ms = now;

            // Add to snapshot log
            metadata.snapshot_log.push(IcebergSnapshotLogEntry {
                timestamp_ms: now,
                snapshot_id: self.current_snapshot_id,
            });

            // Write updated metadata
            self.write_table_metadata().await?;
            
            info!("Created snapshot {} with {} data files", self.current_snapshot_id, self.data_files.len());
            self.current_snapshot_id += 1;
        }
        Ok(())
    }
}

#[async_trait]
impl Operator for IcebergSink {
    fn name(&self) -> &str {
        &self.id
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Sink for IcebergSink {
    async fn run(&mut self, mut rx: BatchRx, _cancel: CancellationToken) -> Result<()> {
        info!("Starting Iceberg sink for table: {}", self.table_name);
        info!("Iceberg sink: Waiting for messages...");
        
        // Initialize object store
        self.initialize_object_store().await?;
        info!("Iceberg sink: Object store initialized, ready to receive messages");
        
        // Create or load table metadata
        self.create_or_load_table_metadata().await?;
        
        let mut batch_count = 0;

        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Batch(batch) => {
                    batch_count += 1;
                    info!("Iceberg sink: Processing batch {} with {} rows", batch_count, batch.num_rows());

                    // Generate unique file path
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    let file_name = format!("data-{}-{}.parquet", timestamp, Uuid::new_v4());
                    let file_path = format!("{}/data/{}", self.table_name, file_name);

                    info!("Iceberg sink: Writing parquet file: {}", file_name);
                    
                    // Write parquet file to MinIO
                    let file_size = self.write_parquet_file(&batch, &file_path).await?;
                    
                    // Create Iceberg data file entry
                    let data_file = IcebergDataFile {
                        content: 0, // Data content
                        file_path: file_path.clone(),
                        file_format: "PARQUET".to_string(),
                        partition: HashMap::new(), // No partitioning for now
                        record_count: batch.num_rows() as i64,
                        file_size_in_bytes: file_size as i64,
                        column_sizes: HashMap::new(),
                        value_counts: HashMap::new(),
                        null_value_counts: HashMap::new(),
                        nan_value_counts: HashMap::new(),
                        distinct_counts: HashMap::new(),
                        lower_bounds: HashMap::new(),
                        upper_bounds: HashMap::new(),
                        key_metadata: None,
                        split_offsets: Vec::new(),
                        equality_ids: Vec::new(),
                        sort_order_id: None,
                    };
                    
                    self.data_files.push(data_file);
                    
                    info!("Iceberg sink: Successfully written parquet file: {} ({} bytes)", file_name, file_size);
                }
                Message::Watermark(_) => {
                    info!("Iceberg sink: Received watermark");
                    // Commit current batch of files
                    if !self.data_files.is_empty() {
                        let manifest_path = self.create_manifest_file(&self.data_files).await?;
                        self.create_snapshot(manifest_path).await?;
                        self.data_files.clear();
                        info!("Iceberg sink: Committed batch of files");
                    }
                }
                Message::Eos => {
                    info!("Iceberg sink: End of stream, processing final batch");
                    // Commit any remaining files
                    if !self.data_files.is_empty() {
                        let manifest_path = self.create_manifest_file(&self.data_files).await?;
                        self.create_snapshot(manifest_path).await?;
                        info!("Iceberg sink: Committed final batch of {} files", self.data_files.len());
                    }
                    break;
                }
            }
        }

        info!("Successfully completed Iceberg sink operations for table: {}", self.table_name);
        Ok(())
    }
}

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH};
use object_store::{
    aws::AmazonS3Builder, path::Path as ObjectPath, ObjectStore, PutOptions,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rde_core::{BatchRx, Message, Operator, Sink};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

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
        }
    }

    async fn initialize_object_store(&mut self) -> Result<()> {
        info!("Initializing object store for MinIO...");
        if self.object_store.is_none() {
            info!("Creating S3-compatible object store with endpoint: {}", self.endpoint);
            info!("Using access key: {}, region: {}", self.access_key, self.region);
            
            let object_store = match AmazonS3Builder::new()
                .with_endpoint(&self.endpoint)
                .with_access_key_id(&self.access_key)
                .with_secret_access_key(&self.secret_key)
                .with_region(&self.region)
                .with_bucket_name(&self.bucket)
                .with_allow_http(true) // For local MinIO
                .build() {
                    Ok(store) => {
                        info!("Successfully created S3-compatible object store");
                        store
                    }
                    Err(e) => {
                        warn!("Failed to create object store: {:?}", e);
                        return Err(e.into());
                    }
                };

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
        
        let mut batch_count = 0;
        let mut data_files = Vec::new();

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
                    data_files.push(file_path.clone());
                    
                    info!("Iceberg sink: Successfully written parquet file: {} ({} bytes)", file_name, file_size);
                }
                Message::Watermark(_) => {
                    info!("Iceberg sink: Received watermark");
                    // Could trigger a commit here
                }
                Message::Eos => {
                    info!("Iceberg sink: End of stream, wrote {} data files", data_files.len());
                    break;
                }
            }
        }

        // TODO: Create Iceberg metadata (manifest files, table metadata)
        // For now, we're just writing Parquet files to MinIO
        info!("Successfully wrote {} parquet files to MinIO bucket: {}", 
              data_files.len(), self.bucket);

        Ok(())
    }
}

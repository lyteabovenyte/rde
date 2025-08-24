//! # RDE CLI - Real-Time Data Engineering Pipeline Runner
//!
//! This is the main command-line interface for running RDE data processing pipelines.
//! It loads YAML pipeline configurations and executes them with proper error handling,
//! logging, and graceful shutdown capabilities.
//!
//! ## Features
//!
//! - **YAML Configuration**: Load pipeline specifications from YAML files
//! - **Dynamic Schema Inference**: Automatic schema detection and evolution
//! - **Graceful Shutdown**: Proper cleanup on CTRL-C or termination signals
//! - **Comprehensive Logging**: Structured logging with configurable levels
//! - **Error Recovery**: Robust error handling and recovery mechanisms
//! - **Performance Monitoring**: Built-in metrics and performance tracking
//!
//! ## Usage
//!
//! ```bash
//! # Run a pipeline from a YAML configuration file
//! rde-cli --pipeline examples/kafka-to-iceberg.yml
//!
//! # Run with custom channel capacity for performance tuning
//! rde-cli --pipeline my-pipeline.yml --channel-capacity 1000
//!
//! # Enable debug logging
//! RUST_LOG=debug rde-cli --pipeline my-pipeline.yml
//! ```
//!
//! ## Pipeline Configuration
//!
//! The CLI expects a YAML configuration file that defines:
//! - Data sources (Kafka, CSV files, etc.)
//! - Transformations (SQL, cleaning, partitioning, etc.)  
//! - Data sinks (Iceberg, Parquet, stdout, etc.)
//! - Connections between operators (edges)
//!
//! See the `examples/` directory for sample pipeline configurations.

use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use clap::Parser;
use rde_core::PipelineSpec;
use rde_core::SourceSpec;
use glob;
use rde_io::{sink_parquet::ParquetDirSink, sink_stdout::StdoutSink, sink_iceberg::IcebergSink, source_csv::CsvSource, source_kafka::KafkaPipelineSource};
use rde_tx::create_transform;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::{signal, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use object_store::ObjectStore;


/// Command-line arguments for the RDE pipeline runner
#[derive(Parser, Debug)]
#[command(name = "rde-cli")]
#[command(about = "Real-Time Data Engineering Pipeline Runner")]
#[command(long_about = "
RDE CLI executes data processing pipelines defined in YAML configuration files.
It supports various data sources, transformations, and sinks with automatic
schema evolution and high-performance streaming processing.

Examples:
  rde-cli --pipeline examples/kafka-to-iceberg.yml
  rde-cli -p my-pipeline.yml --channel-capacity 1000
  RUST_LOG=debug rde-cli --pipeline complex-pipeline.yml
")]
struct Args {
    /// Path to the pipeline YAML configuration file
    /// 
    /// The YAML file should define sources, transforms, sinks, and their connections.
    /// See examples/ directory for sample configurations.
    #[arg(short, long)]
    #[arg(help = "Pipeline YAML configuration file")]
    pipeline: PathBuf,
    
    /// Channel capacity between pipeline operators
    /// 
    /// Controls the buffer size for message passing between operators.
    /// Higher values improve throughput but use more memory.
    /// Lower values reduce memory usage but may cause backpressure.
    #[arg(long, default_value_t = 8)]
    #[arg(help = "Buffer size for operator message channels")]
    channel_capacity: usize,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let spec: PipelineSpec = {
        let y = std::fs::read_to_string(&args.pipeline)?;
        serde_yaml::from_str(&y)?
    };

    // v0: assume single source -> transforms -> single sink
    let cancel = CancellationToken::new();
    
    // Create channels for the pipeline
    // We need: source -> transform1 -> transform2 -> ... -> sink
    // For n transforms, we need n+1 channels total:
    // - 1 channel: source -> transform1
    // - n-1 channels: transform1 -> transform2, transform2 -> transform3, etc.
    // - 1 channel: transformN -> sink
    let num_channels = spec.transforms.len() + 1;
    let mut channels = Vec::new();
    for _ in 0..num_channels {
        channels.push(mpsc::channel(args.channel_capacity));
    }

    // Infer schema upfront so we can pass it to transform and sink
    let schema: SchemaRef = match &spec.sources[0] {
        SourceSpec::Csv(csv) => {
            // Resolve glob pattern to get actual file paths
            let mut paths: Vec<String> = vec![];
            for entry in glob::glob(&csv.path)? {
                paths.push(entry?.display().to_string());
            }
            if paths.is_empty() {
                anyhow::bail!("no files matched: {}", csv.path);
            }
            
            let inferred_schema = datafusion::arrow::csv::reader::infer_schema_from_files(
                &paths,
                b',',
                Some(100),
                csv.has_header,
            )?;
            Arc::new(inferred_schema)
        }
        SourceSpec::Kafka(kafka) => {
            // For Kafka, use dynamic schema inference
            // The schema will be inferred from the first message or loaded from the mapped Iceberg table
            if let Some(mapping) = &kafka.topic_mapping {
                // Try to load schema from the mapped table
                let object_store = object_store::aws::AmazonS3Builder::new()
                    .with_endpoint(&mapping.endpoint)
                    .with_access_key_id(&mapping.access_key)
                    .with_secret_access_key(&mapping.secret_key)
                    .with_region(&mapping.region)
                    .with_allow_http(true)
                    .build()?;
                
                let metadata_path = format!("{}/metadata/metadata.json", mapping.iceberg_table);
                let path = object_store::path::Path::from(metadata_path.as_str());
                
                match object_store.get(&path).await {
                    Ok(data) => {
                        let metadata_json = String::from_utf8(data.bytes().await?.to_vec())?;
                        let metadata: serde_json::Value = serde_json::from_str(&metadata_json)?;
                        
                        if let Some(current_schema_id) = metadata["current_schema_id"].as_i64() {
                            if let Some(schemas) = metadata["schemas"].as_object() {
                                if let Some(schema_data) = schemas.get(&current_schema_id.to_string()) {
                                    // Convert Iceberg schema to Arrow schema
                                    let fields: Vec<datafusion::arrow::datatypes::Field> = schema_data["fields"]
                                        .as_array()
                                        .unwrap_or(&Vec::new())
                                        .iter()
                                        .map(|field| {
                                            let name = field["name"].as_str().unwrap_or("unknown");
                                            let field_type = field["type"].as_str().unwrap_or("string");
                                            let required = field["required"].as_bool().unwrap_or(false);
                                            
                                            let data_type = match field_type {
                                                "long" => datafusion::arrow::datatypes::DataType::Int64,
                                                "int" => datafusion::arrow::datatypes::DataType::Int32,
                                                "double" => datafusion::arrow::datatypes::DataType::Float64,
                                                "float" => datafusion::arrow::datatypes::DataType::Float32,
                                                "boolean" => datafusion::arrow::datatypes::DataType::Boolean,
                                                "string" => datafusion::arrow::datatypes::DataType::Utf8,
                                                "binary" => datafusion::arrow::datatypes::DataType::Binary,
                                                "date" => datafusion::arrow::datatypes::DataType::Date32,
                                                "timestamp" => datafusion::arrow::datatypes::DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                                                _ => datafusion::arrow::datatypes::DataType::Utf8,
                                            };
                                            
                                            datafusion::arrow::datatypes::Field::new(name, data_type, !required)
                                        })
                                        .collect();
                                    
                                    Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
                                } else {
                                    Arc::new(datafusion::arrow::datatypes::Schema::empty())
                                }
                            } else {
                                Arc::new(datafusion::arrow::datatypes::Schema::empty())
                            }
                        } else {
                            Arc::new(datafusion::arrow::datatypes::Schema::empty())
                        }
                    }
                    Err(_) => {
                        // No existing table, start with empty schema
                        Arc::new(datafusion::arrow::datatypes::Schema::empty())
                    }
                }
            } else {
                // No topic mapping, start with empty schema for dynamic inference
                Arc::new(datafusion::arrow::datatypes::Schema::empty())
            }
        }
    };
    
    // Build source from spec with the inferred schema
    let mut source: Box<dyn rde_core::Source> = match &spec.sources[0] {
        SourceSpec::Csv(csv) => Box::new(CsvSource::try_new(csv.clone())?.with_schema(schema.clone())),
        SourceSpec::Kafka(kafka) => Box::new(KafkaPipelineSource::new(kafka.clone()).with_schema(schema.clone())),
    };

    // Build transforms
    let mut transforms = Vec::new();
    let mut current_schema = schema.clone();
    
    for transform_spec in &spec.transforms {
        let transform = create_transform(transform_spec, current_schema.clone())?;
        current_schema = transform.schema();
        transforms.push(transform);
    }

    // Build sink with the final schema
    let mut sink: Box<dyn rde_core::Sink> = match &spec.sinks[0] {
        rde_core::SinkSpec::Stdout { id } => Box::new(StdoutSink::new(id.clone(), current_schema.clone())),
        rde_core::SinkSpec::ParquetDir { id, path } => Box::new(ParquetDirSink::new(
            id.clone(),
            PathBuf::from(path),
            current_schema.clone(),
        )),
        rde_core::SinkSpec::Iceberg(iceberg) => Box::new(IcebergSink::new(
            iceberg.id.clone(),
            current_schema.clone(),
            iceberg.table_name.clone(),
            iceberg.bucket.clone(),
            iceberg.endpoint.clone(),
            iceberg.access_key.clone(),
            iceberg.secret_key.clone(),
            iceberg.region.clone(),
        )),
    };

    // Spawn tasks
    let mut handles = Vec::new();
    
    // Handle the case where there are no transforms
    if transforms.is_empty() {
        // Simple case: source -> sink
        let (source_tx, sink_rx) = channels.remove(0);
        
        // Source task
        let c1 = cancel.child_token();
        let src_handle = tokio::spawn(async move { 
            source.run(source_tx, c1).await 
        });
        handles.push(src_handle);

        // Sink task
        let c_sink = cancel.child_token();
        let sink_handle = tokio::spawn(async move { 
            sink.run(sink_rx, c_sink).await 
        });
        handles.push(sink_handle);
    } else {
        // Multi-transform case: source -> transform1 -> transform2 -> ... -> sink
        
        // Source task: source -> transform1
        let (source_tx, transform1_rx) = channels.remove(0);
        let c_source = cancel.child_token();
        let src_handle = tokio::spawn(async move { 
            source.run(source_tx, c_source).await 
        });
        handles.push(src_handle);

        // Transform tasks: transform1 -> transform2 -> ... -> transformN
        let mut current_rx = transform1_rx;
        let num_transforms = transforms.len();
        for (i, mut transform) in transforms.into_iter().enumerate() {
            let (transform_tx, next_rx) = if i == num_transforms - 1 {
                // Last transform: transformN -> sink
                channels.remove(0)
            } else {
                // Intermediate transform: transformN -> transformN+1
                channels.remove(0)
            };
            
            let c_transform = cancel.child_token();
            let transform_handle = tokio::spawn(async move { 
                transform.run(current_rx, transform_tx, c_transform).await 
            });
            handles.push(transform_handle);
            
            current_rx = next_rx;
        }

        // Sink task: transformN -> sink
        let c_sink = cancel.child_token();
        let sink_handle = tokio::spawn(async move { 
            sink.run(current_rx, c_sink).await 
        });
        handles.push(sink_handle);
    }

    // Ctrl-C handling
    tokio::select! {
        _ = signal::ctrl_c() => { 
            println!("\nReceived Ctrl-C, shutting down...");
            cancel.cancel(); 
        },
        _ = async {
            // Wait for all tasks to complete
            for handle in handles {
                let _ = handle.await;
            }
        } => {}
    }
    Ok(())
}

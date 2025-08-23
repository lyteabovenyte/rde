use anyhow::Result;
use arrow_schema::SchemaRef;
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


#[derive(Parser, Debug)]
struct Args {
    /// Pipeline YAML
    #[arg(short, long)]
    pipeline: PathBuf,
    /// Bounded channel capacity between operators
    #[arg(long, default_value_t = 8)]
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
            
            let inferred_schema = arrow_csv::reader::infer_schema_from_files(
                &paths,
                b',',
                Some(100),
                csv.has_header,
            )?;
            Arc::new(inferred_schema)
        }
        SourceSpec::Kafka(_) => {
            // For Kafka, use a schema that matches the JSON structure
            Arc::new(arrow_schema::Schema::new(vec![
                // TODO: this is hardcoded schema layout, we should infer schema on the fly
                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
                arrow_schema::Field::new("amount", arrow_schema::DataType::Int64, true),
            ]))
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

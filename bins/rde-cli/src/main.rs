use anyhow::Result;
use arrow_schema::SchemaRef;
use clap::Parser;
use rde_core::PipelineSpec;
use rde_core::SourceSpec;
use rde_core::Transform; // Bring the trait with `run` into scope
use glob;
use rde_io::{sink_parquet::ParquetDirSink, sink_stdout::StdoutSink, sink_iceberg::IcebergSink, source_csv::CsvSource, source_kafka::KafkaPipelineSource};
use rde_tx::Passthrough;
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

    // v0: assume single source -> passthrough(empty schema) -> single sink
    let cancel = CancellationToken::new();
    let (tx1, rx1) = mpsc::channel(args.channel_capacity);
    let (tx2, rx2) = mpsc::channel(args.channel_capacity);

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
    let mut t = Passthrough::new("clean".into(), schema.clone());
    let mut sink: Box<dyn rde_core::Sink> = match &spec.sinks[0] {
        rde_core::SinkSpec::Stdout { id } => Box::new(StdoutSink::new(id.clone(), schema.clone())),
        rde_core::SinkSpec::ParquetDir { id, path } => Box::new(ParquetDirSink::new(
            id.clone(),
            PathBuf::from(path),
            schema.clone(),
        )),
        rde_core::SinkSpec::Iceberg(iceberg) => Box::new(IcebergSink::new(
            iceberg.id.clone(),
            schema.clone(),
            iceberg.table_name.clone(),
            iceberg.bucket.clone(),
            iceberg.endpoint.clone(),
            iceberg.access_key.clone(),
            iceberg.secret_key.clone(),
            iceberg.region.clone(),
        )),
    };
    // Spawn tasks
    let c1 = cancel.child_token();
    let src_handle = tokio::spawn(async move { source.run(tx1, c1).await });
    let c2 = cancel.child_token();
    // run method on t (Passthrough) does not infers the schema -> so parquet file would be empty for now
    let tf_handle = tokio::spawn(async move { t.run(rx1, tx2, c2).await });
    let c3 = cancel.child_token();
    let sink_handle = tokio::spawn(async move { sink.run(rx2, c3).await });
    // Ctrl-C handling
    tokio::select! {
        _ = signal::ctrl_c() => { 
            println!("\nReceived Ctrl-C, shutting down...");
            cancel.cancel(); 
        },
        _ = async {
            // Wait for all tasks to complete
            let _ = src_handle.await;
            let _ = tf_handle.await;
            let _ = sink_handle.await;
        } => {}
    }
    Ok(())
}

use anyhow::Result;
use arrow_schema::SchemaRef;
use clap::Parser;
use rde_core::Operator;
use rde_core::Source; // Bring the trait into scope for CsvSource::schema()
use rde_core::SourceSpec;
use rde_core::Transform; // Bring the trait with `run` into scope
use rde_core::{Message, PipelineSpec};
use rde_io::{sink_parquet::ParquetDirSink, sink_stdout::StdoutSink, source_csv::CsvSource};
use rde_tx::Passthrough;
use std::{path::PathBuf, sync::Arc};
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
    // v0: assume single source -> passthrough -> single sink
    let cancel = CancellationToken::new();
    let (tx1, rx1) = mpsc::channel(args.channel_capacity);
    let (tx2, rx2) = mpsc::channel(args.channel_capacity);
    // Build source from spec
    let mut source = match &spec.sources[0] {
        SourceSpec::Csv(csv) => CsvSource::try_new(csv.clone())?,
    };
    // For now, get schema from source lazily; pass empty schema to operators
    let schema: SchemaRef = source.schema();
    let mut t = Passthrough::new("clean".into(), schema.clone());
    let mut sink: Box<dyn rde_core::Sink> = match &spec.sinks[0] {
        rde_core::SinkSpec::Stdout { id } => Box::new(StdoutSink::new(id.clone(), schema.clone())),
        rde_core::SinkSpec::ParquetDir { id, path } => Box::new(ParquetDirSink::new(
            id.clone(),
            PathBuf::from(path),
            schema.clone(),
        )),
    };
    // Spawn tasks
    let c1 = cancel.child_token();
    let src_handle = tokio::spawn(async move { source.run(tx1, c1).await });
    let c2 = cancel.child_token();
    let tf_handle = tokio::spawn(async move { t.run(rx1, tx2, c2).await });
    let c3 = cancel.child_token();
    let sink_handle = tokio::spawn(async move { sink.run(rx2, c3).await });
    // Ctrl-C handling
    tokio::select! {
    _
    = signal::ctrl_c() => { cancel.cancel(); },
    _
    = &mut tokio::spawn(async {}) => {}
    }
    // Join
    src_handle.await??;
    tf_handle.await??;
    sink_handle.await??;
    Ok(())
}

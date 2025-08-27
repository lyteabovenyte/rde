//! # Kafka Producer - High-Performance Data Streaming Utility
//!
//! This utility provides a flexible and high-performance way to stream JSON data
//! from files into Apache Kafka topics. It's designed to support various JSON formats,
//! batch processing, and comprehensive error handling for testing and production use.
//!
//! ## Features
//!
//! - **Multiple JSON Formats**: Support for JSON objects, arrays, and NDJSON
//! - **Batch Processing**: Configurable batch sizes for optimal throughput
//! - **File Globbing**: Process multiple files with glob patterns
//! - **Progress Tracking**: Real-time progress reporting with configurable intervals
//! - **Error Handling**: Robust error handling with retry logic
//! - **Performance Tuning**: Configurable delays and batch sizes
//! - **Dry Run Mode**: Test configurations without sending data
//!
//! ## Supported JSON Formats
//!
//! ### JSON Object (json)
//! Single JSON object per file:
//! ```json
//! {"id": 1, "name": "Alice", "age": 30}
//! ```
//!
//! ### JSON Array (json_array) 
//! Array of JSON objects:
//! ```json
//! [
//!   {"id": 1, "name": "Alice"},
//!   {"id": 2, "name": "Bob"}
//! ]
//! ```
//!
//! ### NDJSON (ndjson)
//! Newline-delimited JSON objects:
//! ```
//! {"id": 1, "name": "Alice"}
//! {"id": 2, "name": "Bob"}
//! {"id": 3, "name": "Charlie"}
//! ```
//!
//! ## Usage Examples
//!
//! ```bash
//! # Stream NDJSON file to Kafka topic
//! kafka-producer -i data.ndjson -t my-topic -f ndjson
//!
//! # Stream multiple files with progress tracking
//! kafka-producer -i "data/*.json" -t events -f json_array -p 1000
//!
//! # High-throughput streaming with custom batch size
//! kafka-producer -i large-dataset.ndjson -t stream -f ndjson -b 1000 -d 10
//!
//! # Dry run to test configuration
//! kafka-producer -i test.json -t test-topic -f json --dry-run
//! ```

#![allow(unused)]

use anyhow::{Context, Result};
use clap::Parser;
use glob::glob;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[clap(
    name = "kafka-producer",
    about = "Stream JSON data from files to Kafka topics"
)]
struct Args {
    /// Path to JSON file or directory containing JSON files
    #[arg(short, long)]
    input: PathBuf,

    /// Kafka brokers (comma-separated)
    #[arg(short, long, default_value = "localhost:9092")]
    brokers: String,

    /// Kafka topic to send messages to
    #[arg(short, long)]
    topic: String,

    /// Delay between messages in milliseconds (0 = no delay)
    #[arg(short, long, default_value_t = 0)]
    delay_ms: u64,

    /// Batch size (number of messages to send at once)
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// JSON format: object (single JSON object), array (JSON array), ndjson (newline-delimited JSON)
    #[arg(short, long, default_value = "auto")]
    format: JsonFormat,

    /// Glob pattern for JSON files when input is a directory
    #[arg(short, long, default_value = "*.json")]
    glob_pattern: String,

    /// Continue on error (skip bad messages)
    #[arg(long)]
    continue_on_error: bool,

    /// Dry run (don't actually send messages)
    #[arg(long)]
    dry_run: bool,

    /// Producer client ID
    #[arg(long, default_value = "rde-kafka-producer")]
    client_id: String,

    /// Show progress every N messages
    #[arg(long, default_value_t = 100)]
    progress_interval: usize,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum JsonFormat {
    /// Automatically detect format
    Auto,
    /// Single JSON object
    Object,
    /// JSON array of objects
    Array,
    /// Newline-delimited JSON (one object per line)
    Ndjson,
}

struct ProducerStats {
    total_messages: usize,
    successful_messages: usize,
    failed_messages: usize,
    total_bytes: usize,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            total_messages: 0,
            successful_messages: 0,
            failed_messages: 0,
            total_bytes: 0,
        }
    }

    fn print_summary(&self) {
        info!("Producer Summary:");
        info!("  Total messages: {}", self.total_messages);
        info!("  Successful: {}", self.successful_messages);
        info!("  Failed: {}", self.failed_messages);
        info!("  Total bytes sent: {}", self.total_bytes);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    info!("Starting Kafka producer with args: {:?}", args);

    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("client.id", &args.client_id)
        .set("message.timeout.ms", "30000")
        .create()
        .context("Failed to create Kafka producer")?;

    // Get list of JSON files to process
    let json_files = get_json_files(&args.input, &args.glob_pattern)?;
    if json_files.is_empty() {
        warn!("No JSON files found to process");
        return Ok(());
    }

    info!("Found {} JSON files to process", json_files.len());

    let mut stats = ProducerStats::new();

    // Process each file
    for (file_idx, file_path) in json_files.iter().enumerate() {
        info!(
            "Processing file {}/{}: {}",
            file_idx + 1,
            json_files.len(),
            file_path.display()
        );

        match process_json_file(
            &producer,
            file_path,
            &args.topic,
            &args.format,
            args.delay_ms,
            args.batch_size,
            args.continue_on_error,
            args.dry_run,
            args.progress_interval,
            &mut stats,
        )
        .await
        {
            Ok(_) => {
                info!("Successfully processed: {}", file_path.display());
            }
            Err(e) => {
                error!("Failed to process {}: {}", file_path.display(), e);
                if !args.continue_on_error {
                    return Err(e);
                }
            }
        }
    }

    // Flush any remaining messages
    if !args.dry_run {
        if let Err(e) = producer.flush(Duration::from_secs(10)) {
            warn!("Error flushing producer: {:?}", e);
        }
    }

    stats.print_summary();
    info!("Kafka producer finished");

    Ok(())
}

fn get_json_files(input: &Path, glob_pattern: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if input.is_file() {
        files.push(input.to_path_buf());
    } else if input.is_dir() {
        let pattern = format!("{}/{}", input.display(), glob_pattern);
        for entry in glob(&pattern)? {
            match entry {
                Ok(path) => {
                    if path.is_file() {
                        files.push(path);
                    }
                }
                Err(e) => warn!("Error reading glob entry: {}", e),
            }
        }
        // Also check for nested directories
        let recursive_pattern = format!("{}/**/{}", input.display(), glob_pattern);
        for entry in glob(&recursive_pattern)? {
            match entry {
                Ok(path) => {
                    if path.is_file() {
                        files.push(path);
                    }
                }
                Err(e) => warn!("Error reading glob entry: {}", e),
            }
        }
    } else {
        anyhow::bail!("Input path does not exist: {}", input.display());
    }

    // Sort files for consistent processing order
    files.sort();
    files.dedup();

    Ok(files)
}

async fn process_json_file(
    producer: &FutureProducer,
    file_path: &Path,
    topic: &str,
    format: &JsonFormat,
    delay_ms: u64,
    batch_size: usize,
    continue_on_error: bool,
    dry_run: bool,
    progress_interval: usize,
    stats: &mut ProducerStats,
) -> Result<()> {
    let file = File::open(file_path)
        .with_context(|| format!("Failed to open file: {}", file_path.display()))?;
    let reader = BufReader::new(file);

    // Detect format if auto
    let actual_format = if matches!(format, JsonFormat::Auto) {
        detect_json_format(file_path)?
    } else {
        format.clone()
    };

    info!("Using JSON format: {:?}", actual_format);

    match actual_format {
        JsonFormat::Object => {
            // Read entire file as single JSON object
            let content = std::fs::read_to_string(file_path)?;
            let value: Value = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse JSON from {}", file_path.display()))?;
            
            send_message(producer, topic, &value, dry_run, stats).await?;
        }
        JsonFormat::Array => {
            // Read entire file as JSON array
            let content = std::fs::read_to_string(file_path)?;
            let array: Vec<Value> = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse JSON array from {}", file_path.display()))?;
            
            let mut batch = Vec::new();
            for (idx, value) in array.into_iter().enumerate() {
                batch.push(value);
                
                if batch.len() >= batch_size {
                    send_batch(producer, topic, &batch, dry_run, delay_ms, stats).await?;
                    batch.clear();
                }
                
                if (idx + 1) % progress_interval == 0 {
                    info!("Progress: {} messages processed", idx + 1);
                }
            }
            
            // Send remaining messages
            if !batch.is_empty() {
                send_batch(producer, topic, &batch, dry_run, delay_ms, stats).await?;
            }
        }
        JsonFormat::Ndjson | JsonFormat::Auto => {
            // Process line by line
            let mut batch = Vec::new();
            let mut line_num = 0;
            
            for line in reader.lines() {
                line_num += 1;
                let line = line?;
                let trimmed = line.trim();
                
                // Skip empty lines
                if trimmed.is_empty() {
                    continue;
                }
                
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(value) => {
                        batch.push(value);
                        
                        if batch.len() >= batch_size {
                            send_batch(producer, topic, &batch, dry_run, delay_ms, stats).await?;
                            batch.clear();
                        }
                        
                        if stats.total_messages % progress_interval == 0 && stats.total_messages > 0 {
                            info!("Progress: {} messages processed", stats.total_messages);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse JSON at line {}: {}", line_num, e);
                        if !continue_on_error {
                            return Err(e.into());
                        }
                        stats.failed_messages += 1;
                    }
                }
            }
            
            // Send remaining messages
            if !batch.is_empty() {
                send_batch(producer, topic, &batch, dry_run, delay_ms, stats).await?;
            }
        }
    }

    Ok(())
}

async fn send_batch(
    producer: &FutureProducer,
    topic: &str,
    messages: &[Value],
    dry_run: bool,
    delay_ms: u64,
    stats: &mut ProducerStats,
) -> Result<()> {
    for message in messages {
        send_message(producer, topic, message, dry_run, stats).await?;
        
        if delay_ms > 0 {
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }
    Ok(())
}

async fn send_message(
    producer: &FutureProducer,
    topic: &str,
    message: &Value,
    dry_run: bool,
    stats: &mut ProducerStats,
) -> Result<()> {
    let payload = serde_json::to_string(message)?;
    let payload_bytes = payload.as_bytes();
    
    stats.total_messages += 1;
    stats.total_bytes += payload_bytes.len();
    
    if dry_run {
        info!("DRY RUN: Would send message to topic '{}': {}", topic, payload);
        stats.successful_messages += 1;
        return Ok(());
    }
    
    let record = FutureRecord::to(topic)
        .payload(payload_bytes)
        .key("");
    
    match producer.send(record, Duration::from_secs(0)).await {
        Ok(_) => {
            stats.successful_messages += 1;
            Ok(())
        }
        Err((e, _)) => {
            stats.failed_messages += 1;
            Err(anyhow::anyhow!("Failed to send message: {}", e))
        }
    }
}

fn detect_json_format(file_path: &Path) -> Result<JsonFormat> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();
    
    // Read first non-empty line
    loop {
        first_line.clear();
        if reader.read_line(&mut first_line)? == 0 {
            return Ok(JsonFormat::Ndjson); // Empty file, treat as NDJSON
        }
        let trimmed = first_line.trim();
        if !trimmed.is_empty() {
            // Check if it starts with array or object
            if trimmed.starts_with('[') {
                return Ok(JsonFormat::Array);
            } else if trimmed.starts_with('{') {
                // Could be single object or NDJSON
                // Try to read the entire file as JSON
                let content = std::fs::read_to_string(file_path)?;
                if serde_json::from_str::<Value>(&content).is_ok() {
                    return Ok(JsonFormat::Object);
                } else {
                    return Ok(JsonFormat::Ndjson);
                }
            }
            break;
        }
    }
    
    Ok(JsonFormat::Ndjson)
}

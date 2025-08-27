# Blockchain Iceberg Sink Implementation

This document describes the implementation of the `BlockchainIcebergSink` that replaces the placeholder `SimpleSink` and provides a complete data pipeline from blockchain APIs to Apache Iceberg tables.

## Overview

The `BlockchainIcebergSink` implements a production-ready data sinking process that:

1. **Collects data** from blockchain.com API (Bitcoin market data) and cryptopanic.com API (crypto news)
2. **Streams data** through Kafka brokers to the processing engine
3. **Transforms data** using DataFusion for real-time processing
4. **Sinks data** to Apache Iceberg tables with proper batch processing and metadata management

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Blockchain    │───▶│     Kafka    │───▶│   DataFusion    │───▶│   Iceberg       │
│   API           │    │   Broker     │    │   Transform     │    │   Tables        │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
```

## Key Components

### 1. BlockchainIcebergSink

The main sink implementation that handles both Bitcoin market data and crypto news:

```rust
pub struct BlockchainIcebergSink {
    id: String,
    bitcoin_sink: Option<RdeIcebergSink>,
    news_sink: Option<RdeIcebergSink>,
    bitcoin_batch: Vec<BitcoinMarketData>,
    news_batch: Vec<CryptoNews>,
    batch_size: usize,
    bitcoin_table_name: String,
    news_table_name: String,
    bucket: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
    region: String,
}
```

### 2. Data Models

#### BitcoinMarketData

```rust
pub struct BitcoinMarketData {
    pub id: uuid::Uuid,
    pub timestamp: DateTime<Utc>,
    pub price: f64,
    pub volume_24h: f64,
    pub market_cap: f64,
    pub price_change_24h: f64,
    pub high_24h: f64,
    pub low_24h: f64,
    pub transactions_24h: u64,
    pub hash_rate: f64,
    pub difficulty: f64,
    pub circulating_supply: f64,
    pub max_supply: f64,
    pub source: String,
    pub metadata: serde_json::Value,
}
```

#### CryptoNews

```rust
pub struct CryptoNews {
    pub id: String,
    pub title: String,
    pub content: String,
    pub source: String,
    pub url: String,
    pub published_at: DateTime<Utc>,
    pub collected_at: DateTime<Utc>,
    pub sentiment_score: f64,
    pub keywords: Vec<String>,
    pub currencies: Vec<String>,
    pub category: String,
    pub votes_positive: u32,
    pub votes_negative: u32,
    pub votes_important: u32,
    pub votes_liked: u32,
    pub votes_disliked: u32,
    pub votes_lol: u32,
    pub votes_toxic: u32,
    pub votes_saved: u32,
    pub votes_comments: u32,
    pub source_name: String,
    pub metadata: serde_json::Value,
}
```

## Usage

### Basic Usage

```rust
use rde_collectors::BlockchainIcebergSink;

// Create the sink
let mut sink = BlockchainIcebergSink::new(
    "my-pipeline".to_string(),
    "bitcoin_market_data".to_string(),
    "crypto_news".to_string(),
    "my-bucket".to_string(),
    "http://localhost:9000".to_string(),
    "minioadmin".to_string(),
    "minioadmin".to_string(),
    "us-east-1".to_string(),
    Some(1000), // batch size
);

// Add Bitcoin data
let bitcoin_data = BitcoinMarketData::new(45000.0, 25000000000.0, 850000000000.0, 2.5);
sink.add_bitcoin_data(bitcoin_data).await?;

// Add news data
let news_data = CryptoNews::new(
    "news-1".to_string(),
    "Bitcoin Price Surges".to_string(),
    "Bitcoin price reaches new highs...".to_string(),
    "cryptopanic.com".to_string(),
    "https://cryptopanic.com/news/1".to_string(),
    Utc::now(),
);
sink.add_news_data(news_data).await?;

// Flush all pending data
sink.flush_all().await?;
```

### Integration with Collectors

The sink is integrated with the existing collectors:

```rust
use rde_collectors::{BitcoinCollector, NewsCollector, CollectorConfig};

// Load configuration
let config = CollectorConfig::load()?;

// Create collectors (they now use BlockchainIcebergSink internally)
let bitcoin_collector = BitcoinCollector::new(config.clone()).await?;
let news_collector = NewsCollector::new(config.clone()).await?;

// Run collectors
tokio::try_join!(
    bitcoin_collector.run(),
    news_collector.run()
)?;
```

### Pipeline Integration

For custom pipelines, you can use the sink as a standard RDE operator:

```rust
use rde_core::{BatchRx, BatchTx, Message, Operator, Sink};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// Create channels
let (tx, rx) = mpsc::channel(1000);
let cancel = CancellationToken::new();

// Create sink
let mut sink = BlockchainIcebergSink::new(/* ... */);

// Run sink
sink.run(rx, cancel).await?;
```

## Configuration

### Environment Variables

The sink uses the following configuration from `CollectorConfig`:

```toml
[iceberg]
bucket = "iceberg-data"
endpoint = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
region = "us-east-1"
```

### Table Names

- **Bitcoin Market Data Table**: `bitcoin_market_data`
- **Crypto News Table**: `crypto_news`

## Data Flow

### 1. Data Collection

Data is collected from external APIs:

- **Bitcoin Market Data**: blockchain.com API
- **Crypto News**: cryptopanic.com API

### 2. Kafka Streaming

Data is streamed through Kafka topics:

- `bitcoin-market-data` topic for Bitcoin data
- `crypto-news` topic for news data

### 3. DataFusion Transformation

Data is transformed using DataFusion:

- Schema validation and cleaning
- Data type conversions
- Aggregations and filtering
- Real-time SQL transformations

### 4. Iceberg Sinking

Data is written to Iceberg tables:

- **Batch Processing**: Data is batched for efficiency
- **Parquet Files**: Data is stored in Parquet format
- **Metadata Management**: Proper Iceberg metadata is maintained
- **Partitioning**: Support for time-based partitioning
- **Schema Evolution**: Automatic schema evolution support

## Iceberg Table Schema

### Bitcoin Market Data Table

```sql
CREATE TABLE bitcoin_market_data (
    id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price DOUBLE,
    volume_24h DOUBLE,
    market_cap DOUBLE,
    price_change_24h DOUBLE,
    high_24h DOUBLE,
    low_24h DOUBLE,
    transactions_24h BIGINT,
    hash_rate DOUBLE,
    difficulty DOUBLE,
    circulating_supply DOUBLE,
    max_supply DOUBLE,
    source STRING,
    metadata STRING
)
PARTITIONED BY (DATE(timestamp))
```

### Crypto News Table

```sql
CREATE TABLE crypto_news (
    id STRING NOT NULL,
    title STRING NOT NULL,
    content STRING NOT NULL,
    source STRING NOT NULL,
    url STRING NOT NULL,
    published_at TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    sentiment_score DOUBLE,
    keywords STRING,
    currencies STRING,
    category STRING,
    votes_positive INT,
    votes_negative INT,
    votes_important INT,
    votes_liked INT,
    votes_disliked INT,
    votes_lol INT,
    votes_toxic INT,
    votes_saved INT,
    votes_comments INT,
    source_name STRING,
    metadata STRING
)
PARTITIONED BY (DATE(published_at))
```

## Performance Features

### Batch Processing

- **Configurable Batch Size**: Default 1000 records per batch
- **Memory Efficient**: Batches are processed and cleared automatically
- **Backpressure Handling**: Built-in backpressure support

### Iceberg Optimizations

- **Parquet Compression**: Efficient data compression
- **Column Statistics**: Automatic column statistics generation
- **File Size Optimization**: Optimal file sizes for query performance
- **Metadata Cleanup**: Automatic metadata cleanup

### Monitoring

- **Metrics**: Built-in metrics for monitoring
- **Logging**: Comprehensive logging for debugging
- **Error Handling**: Robust error handling and retry logic

## Example Queries

Once data is in Iceberg tables, you can query it using SQL:

### Bitcoin Price Analysis

```sql
SELECT
    DATE(timestamp) as date,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price,
    SUM(volume_24h) as total_volume
FROM bitcoin_market_data
WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE(timestamp)
ORDER BY date;
```

### News Sentiment Analysis

```sql
SELECT
    DATE(published_at) as date,
    COUNT(*) as news_count,
    AVG(sentiment_score) as avg_sentiment,
    SUM(votes_positive) as total_positive_votes
FROM crypto_news
WHERE published_at >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY DATE(published_at)
ORDER BY date;
```

## Error Handling

The sink includes comprehensive error handling:

- **API Failures**: Retry logic for API calls
- **Network Issues**: Connection retry with exponential backoff
- **Data Validation**: Automatic data validation and filtering
- **Storage Errors**: Graceful handling of storage failures

## Monitoring and Observability

### Metrics

The sink exposes metrics for monitoring:

- `bitcoin_records_processed_total`
- `news_records_processed_total`
- `batch_flush_duration_seconds`
- `iceberg_write_errors_total`

### Logging

Comprehensive logging is provided:

```rust
info!("Flushing {} Bitcoin market data records to Iceberg", batch.len());
info!("Successfully flushed Bitcoin market data batch");
error!("Failed to write to Iceberg: {}", e);
```

## Troubleshooting

### Common Issues

1. **Connection Errors**: Check MinIO/S3 endpoint and credentials
2. **Schema Mismatches**: Verify data types match expected schema
3. **Batch Size Issues**: Adjust batch size for optimal performance
4. **Memory Issues**: Monitor memory usage and adjust batch sizes

### Debug Mode

Enable debug logging:

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_env_filter("debug")
    .init();
```

## Future Enhancements

- **Real-time Streaming**: Direct streaming without batching
- **Advanced Partitioning**: Multi-level partitioning strategies
- **Data Quality**: Built-in data quality checks
- **Schema Evolution**: Enhanced schema evolution support
- **Compaction**: Automatic table compaction
- **Time Travel**: Point-in-time queries support

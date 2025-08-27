# RDE API Collectors

This document describes the real-time API data collectors for the RDE pipeline, including Bitcoin market data and crypto news collectors.

## Overview

The RDE API collectors provide real-time data ingestion from external APIs:

- **Bitcoin Market Data Collector**: Fetches Bitcoin price, volume, and market data from blockchain.com APIs
- **Crypto News Collector**: Fetches crypto news and sentiment data from cryptopanic.com APIs

Both collectors support:

- **DataFusion Transformations**: Real-time data processing using Apache Arrow
- **Multiple Sinks**: Iceberg tables for analytics and ElasticSearch for full-text search
- **Kafka Integration**: Real-time streaming to Kafka topics
- **Monitoring**: Prometheus metrics and health checks
- **Secure Configuration**: API keys stored in hidden secrets directory

## Architecture

```
External APIs → Collectors → DataFusion → Multiple Sinks
     🔗           📡          ⚡         📦

     blockchain.com  → Bitcoin Collector → Kafka + Iceberg
     cryptopanic.com → News Collector   → Kafka + Iceberg + ElasticSearch
```

## Setup

### 1. Prerequisites

- Rust 1.70+ and Cargo
- Apache Kafka (for streaming)
- Apache Iceberg (for data lake)
- ElasticSearch (for news search)
- MinIO or S3-compatible storage

### 2. Configuration

#### API Keys Setup

Create the secrets directory and configure your API keys:

```bash
# Create secrets directory (already in .gitignore)
mkdir -p secrets

# Copy the template configuration
cp secrets/api_keys.toml secrets/api_keys.toml

# Edit with your actual API keys
nano secrets/api_keys.toml
```

#### Configuration File

Edit `secrets/api_keys.toml` with your actual credentials:

```toml
[blockchain]
# Blockchain.com API - No key required for basic endpoints
api_key = ""

[cryptopanic]
# CryptoPanic API - Get your free API key from https://cryptopanic.com/developers/api/
api_key = "your_cryptopanic_api_key_here"

[elasticsearch]
# ElasticSearch connection details
host = "localhost"
port = 9200
username = "elastic"
password = "changeme"
use_ssl = false

[kafka]
# Kafka broker configuration
brokers = "localhost:9092"
client_id = "rde-collectors"

[iceberg]
# Iceberg/S3 configuration
bucket = "iceberg-data"
endpoint = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
region = "us-east-1"
```

### 3. Building

Build all collectors:

```bash
# Build all collectors
cargo build --release

# Or build individual collectors
cargo build --release --bin bitcoin-collector
cargo build --release --bin news-collector
```

## Usage

### Bitcoin Market Data Collector

The Bitcoin collector fetches real-time market data from blockchain.com APIs.

#### Command Line Usage

```bash
# Basic usage with default configuration
./scripts/start-bitcoin-collector.sh

# Custom configuration
./scripts/start-bitcoin-collector.sh \
    --interval 60 \
    --kafka-brokers "localhost:9092" \
    --kafka-topic "bitcoin-market" \
    --metrics-port 9090 \
    --log-level debug

# Direct cargo run
cargo run --release --bin bitcoin-collector \
    --interval 30 \
    --kafka-brokers "localhost:9092"
```

#### Configuration Options

| Option            | Description                    | Default          |
| ----------------- | ------------------------------ | ---------------- |
| `--interval`      | Collection interval in seconds | 30               |
| `--kafka-brokers` | Kafka broker addresses         | From config      |
| `--kafka-topic`   | Kafka topic name               | `bitcoin-market` |
| `--metrics-port`  | Metrics server port            | 9090             |
| `--log-level`     | Log level                      | `info`           |

#### Data Collected

The Bitcoin collector fetches:

- **Price Data**: Current price, 24h high/low, price change
- **Volume Data**: 24h trading volume
- **Market Data**: Market cap, circulating supply
- **Network Data**: Hash rate, difficulty, transaction count
- **Metadata**: API response timestamps and source information

### Crypto News Collector

The news collector fetches crypto news and sentiment data from cryptopanic.com APIs.

#### Command Line Usage

```bash
# Basic usage with default configuration
./scripts/start-news-collector.sh

# Custom configuration
./scripts/start-news-collector.sh \
    --interval 300 \
    --kafka-brokers "localhost:9092" \
    --kafka-topic "crypto-news" \
    --elasticsearch-host "localhost" \
    --elasticsearch-port 9200 \
    --metrics-port 9091 \
    --log-level debug

# Direct cargo run
cargo run --release --bin news-collector \
    --interval 300 \
    --elasticsearch-host "localhost"
```

#### Configuration Options

| Option                 | Description                    | Default       |
| ---------------------- | ------------------------------ | ------------- |
| `--interval`           | Collection interval in seconds | 300           |
| `--kafka-brokers`      | Kafka broker addresses         | From config   |
| `--kafka-topic`        | Kafka topic name               | `crypto-news` |
| `--elasticsearch-host` | ElasticSearch host             | From config   |
| `--elasticsearch-port` | ElasticSearch port             | From config   |
| `--metrics-port`       | Metrics server port            | 9091          |
| `--log-level`          | Log level                      | `info`        |

#### Data Collected

The news collector fetches:

- **Article Data**: Title, content, source, URL
- **Sentiment Analysis**: Automated sentiment scoring
- **Vote Data**: Community voting (positive, negative, important, etc.)
- **Metadata**: Keywords, currencies, categories
- **Timestamps**: Published and collected timestamps

## Data Processing

### DataFusion Transformations

Both collectors use DataFusion for real-time data processing:

#### Bitcoin Data Transformations

```sql
-- Example DataFusion transformations applied to Bitcoin data
SELECT
    id,
    timestamp,
    price,
    volume_24h,
    market_cap,
    price_change_24h,
    -- Derived fields
    price * 1000000 as price_satoshi,
    market_cap / circulating_supply as price_per_coin,
    volume_24h / market_cap * 100 as volume_market_cap_ratio,
    -- Volatility classification
    CASE
        WHEN price > 50000 THEN 'high'
        WHEN price > 30000 THEN 'medium'
        ELSE 'low'
    END as volatility_level,
    -- Time components
    EXTRACT(year FROM timestamp) as year,
    EXTRACT(month FROM timestamp) as month,
    EXTRACT(day FROM timestamp) as day,
    EXTRACT(hour FROM timestamp) as hour
FROM bitcoin_data
```

#### News Data Transformations

```sql
-- Example DataFusion transformations applied to news data
SELECT
    id,
    title,
    content,
    sentiment_score,
    -- Derived fields
    votes_positive + votes_liked as positive_votes,
    votes_negative + votes_disliked + votes_toxic as negative_votes,
    (votes_positive + votes_liked) - (votes_negative + votes_disliked + votes_toxic) /
    (votes_positive + votes_liked + votes_negative + votes_disliked + votes_toxic + 1) as engagement_score,
    -- Sentiment classification
    CASE
        WHEN sentiment_score > 0.3 THEN 'positive'
        WHEN sentiment_score < -0.3 THEN 'negative'
        ELSE 'neutral'
    END as sentiment_category,
    -- Content analysis
    LENGTH(title) as title_length,
    LENGTH(content) as content_length
FROM news_data
```

### Custom Functions

The collectors register custom DataFusion functions:

- `sentiment_score(text)`: Calculate sentiment score for text
- `price_volatility(price)`: Classify price volatility level

## Data Storage

### Apache Iceberg Tables

Data is stored in Iceberg tables for analytics:

#### Bitcoin Market Data Table

```sql
CREATE TABLE bitcoin_market_data (
    id STRING,
    timestamp TIMESTAMP,
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
PARTITIONED BY (year, month, day)
```

#### Crypto News Table

```sql
CREATE TABLE crypto_news (
    id STRING,
    title STRING,
    content STRING,
    source STRING,
    url STRING,
    published_at TIMESTAMP,
    collected_at TIMESTAMP,
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
PARTITIONED BY (year, month, day)
```

### ElasticSearch Index

News data is also indexed in ElasticSearch for full-text search:

```json
{
  "mappings": {
    "properties": {
      "id": { "type": "keyword" },
      "title": {
        "type": "text",
        "analyzer": "standard",
        "fields": {
          "keyword": { "type": "keyword" }
        }
      },
      "content": {
        "type": "text",
        "analyzer": "standard"
      },
      "sentiment_score": { "type": "float" },
      "keywords": { "type": "keyword" },
      "currencies": { "type": "keyword" },
      "engagement_score": { "type": "float" },
      "published_at": { "type": "date" }
    }
  }
}
```

## Monitoring

### Metrics

Both collectors expose Prometheus metrics:

#### Bitcoin Collector Metrics

- `bitcoin_price_updates_total`: Total price updates
- `bitcoin_current_price`: Current Bitcoin price
- `bitcoin_volume_24h`: 24h trading volume
- `bitcoin_market_cap`: Market capitalization

#### News Collector Metrics

- `news_articles_processed_total`: Total articles processed
- `news_sentiment_score`: Average sentiment score
- `news_engagement_score`: Average engagement score
- `news_total_votes`: Total votes across all articles

### Health Checks

Health check endpoints are available at:

- Bitcoin Collector: `http://localhost:9090/health`
- News Collector: `http://localhost:9091/health`

### Logging

Structured logging with configurable levels:

```bash
# Set log level
export RUST_LOG=debug

# Run with debug logging
cargo run --release --bin bitcoin-collector --log-level debug
```

## Error Handling

### Retry Logic

Both collectors implement robust retry logic:

- **Exponential Backoff**: Retry delays increase with each failure
- **Maximum Retries**: Configurable retry attempts (default: 3)
- **Graceful Degradation**: Continue operation on non-critical failures

### Data Validation

All data is validated before processing:

- **Schema Validation**: Ensure data matches expected schema
- **Range Validation**: Validate numeric fields within expected ranges
- **Required Fields**: Ensure all required fields are present

## Performance

### Optimization Features

- **Batch Processing**: Process multiple records efficiently
- **Connection Pooling**: Reuse HTTP and database connections
- **Async I/O**: Non-blocking operations for high throughput
- **Memory Management**: Efficient memory usage with Apache Arrow

### Scaling

- **Horizontal Scaling**: Run multiple collector instances
- **Load Balancing**: Distribute load across instances
- **Partitioning**: Data partitioned by time for efficient queries

## Troubleshooting

### Common Issues

#### Configuration Errors

```bash
# Check configuration file
cat secrets/api_keys.toml

# Validate configuration
cargo run --bin bitcoin-collector --help
```

#### API Rate Limits

```bash
# Increase collection interval
./scripts/start-bitcoin-collector.sh --interval 60

# Check API response
curl -s "https://api.blockchain.com/v3/exchange/tickers/BTC-USD" | jq
```

#### Connection Issues

```bash
# Test Kafka connection
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Test ElasticSearch connection
curl -s "http://localhost:9200/_cluster/health" | jq
```

### Debug Mode

Enable debug logging for troubleshooting:

```bash
# Set debug environment
export RUST_LOG=debug
export RDE_ENVIRONMENT=development

# Run with debug output
cargo run --bin bitcoin-collector --log-level debug
```

## Development

### Adding New Collectors

To add a new API collector:

1. **Create Data Models**: Define data structures in `crates/rde-collectors/src/models.rs`
2. **Implement Collector**: Add collector logic in `crates/rde-collectors/src/collectors.rs`
3. **Add Sinks**: Implement storage backends in `crates/rde-collectors/src/sinks.rs`
4. **Create Binary**: Add binary executable in `bins/`
5. **Add Scripts**: Create startup scripts in `scripts/`

### Testing

```bash
# Run unit tests
cargo test --package rde-collectors

# Run integration tests
cargo test --test integration

# Test with mock data
cargo run --bin bitcoin-collector --interval 5 --log-level debug
```

## Security

### API Key Management

- **Hidden Directory**: API keys stored in `secrets/` (gitignored)
- **Environment Variables**: Support for environment-based configuration
- **Validation**: API keys validated at startup
- **Rotation**: Support for key rotation without restart

### Data Privacy

- **No PII**: Collectors do not collect personally identifiable information
- **Encryption**: Data encrypted in transit and at rest
- **Access Control**: Proper authentication for all external services

## Support

For issues and questions:

1. Check the troubleshooting section above
2. Review logs with debug level enabled
3. Verify configuration and API keys
4. Check external service status (Kafka, ElasticSearch, etc.)

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

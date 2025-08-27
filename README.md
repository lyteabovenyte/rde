# RDE - Rust Data Engineering Pipeline

A high-performance, real-time data engineering pipeline built in Rust for streaming blockchain and crypto data to analytics-ready formats.

## 🚀 Quick Start

```bash
# 1. Start the crypto real-time pipeline
./scripts/supreme-pipeline.sh

# 2. Query your crypto data with DuckDB
./scripts/query-data.py

# 3. Interactive SQL mode
./scripts/query-data.py --interactive
```

That's it! The pipeline will set up Kafka topics, start the RDE processing engine, and prepare everything for real-time Bitcoin market data and crypto news analytics.

## 📊 What It Does

The RDE pipeline provides:

1. **🔗 Real-time data ingestion** from blockchain.com and cryptopanic.com APIs
2. **📡 Kafka streaming** for high-throughput message processing
3. **🔄 Data transformation** with DataFusion SQL engine
4. **📦 Iceberg storage** with ACID guarantees and schema evolution
5. **📝 Analytics-ready data** for immediate querying with DuckDB
6. **🎯 Real-time monitoring** of Bitcoin price and crypto news sentiment

## 🏗️ Architecture

```
Blockchain APIs → Kafka Topics → RDE Pipeline → MinIO (S3) → DuckDB Analytics
     🔄              📡              ⚡             📦         🎯
```

**Status**: Infrastructure ready, API integrations pending implementation

### Components

- **Kafka**: Message streaming and buffering
- **MinIO**: S3-compatible object storage
- **RDE**: Rust-based data processing engine
- **DuckDB**: Fast analytical SQL engine with S3 support
- **Docker**: Containerized infrastructure

## 📁 Project Structure

```
rde/
├── scripts/
│   ├── supreme-pipeline.sh     # 🚀 Main pipeline orchestrator
│   └── query-data.py          # 🔍 Crypto data analytics tool
├── crates/                    # 🦀 Rust workspace
│   ├── rde-core/             # Core pipeline logic
│   ├── rde-io/               # Kafka/MinIO connectors
│   └── rde-tx/               # Data transformations
├── examples/                  # 📋 Pipeline configurations
├── data/sources/             # 📂 Data source configurations
│   ├── bitcoin/              # Bitcoin market data configs
│   └── news/                 # Crypto news data configs
└── docker-compose.yml        # 🐳 Infrastructure stack
```

## 🔧 Usage Guide

### 1. Start the Pipeline

```bash
./scripts/supreme-pipeline.sh
```

This will:

- Start Kafka, MinIO, and Zookeeper
- Create topics for Bitcoin market data and crypto news
- Start the RDE processing pipeline
- Set up monitoring and analytics

### 2. Query Your Data

```bash
# Quick crypto data overview
./scripts/query-data.py

# Interactive SQL mode
./scripts/query-data.py --interactive

# Specific query
./scripts/query-data.py --query "SELECT * FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet' LIMIT 5"
```

### 3. Example Analytics Queries

The system provides these analytics automatically:

```sql
-- Latest Bitcoin price and market data
SELECT
    timestamp,
    price,
    volume_24h,
    market_cap,
    price_change_24h,
    market_sentiment
FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet'
ORDER BY timestamp DESC
LIMIT 1;

-- Crypto news sentiment analysis
SELECT
    sentiment_category,
    COUNT(*) as article_count,
    AVG(sentiment_score) as avg_sentiment
FROM 's3://crypto-data/crypto_news_data/**/*.parquet'
WHERE published_at >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY sentiment_category;

-- Price vs News correlation
WITH daily_metrics AS (
    SELECT
        DATE(b.timestamp) as date,
        AVG(b.price) as avg_price,
        COUNT(n.id) as news_count,
        AVG(n.sentiment_score) as avg_sentiment
    FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet' b
    LEFT JOIN 's3://crypto-data/crypto_news_data/**/*.parquet' n
        ON DATE(b.timestamp) = DATE(n.published_at)
    WHERE b.timestamp >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY DATE(b.timestamp)
)
SELECT * FROM daily_metrics WHERE news_count > 0;
```

## 🌐 Service Access

| Service       | URL                   | Credentials             |
| ------------- | --------------------- | ----------------------- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka         | localhost:9092        | N/A                     |

## 🚦 Current Status

### ✅ **Ready Components**

- Infrastructure setup (Kafka, MinIO, Zookeeper)
- RDE processing pipeline
- Iceberg table management
- DuckDB analytics with S3 connectivity
- Interactive SQL queries
- Real-time monitoring

### 🔄 **Pending Implementation**

- **API Integrations**: Bitcoin market data collector (blockchain.com)
- **News Collector**: Crypto news data collector (cryptopanic.com)
- **Data Ingestion**: Real-time streaming from external APIs

### 📋 **Next Steps**

1. **Implement API collectors** for real-time data ingestion
2. **Set up monitoring** for pipeline health and data quality
3. **Add alerting** for price movements and news sentiment
4. **Scale infrastructure** for production workloads

## 🛠️ Development

### Prerequisites

- Docker & Docker Compose
- Rust 1.70+
- Python 3.8+ (for DuckDB queries)

### Build

```bash
cargo build --release --bin rde-cli
```

### API Integration Development

The pipeline is ready for real-time data. To add API integrations:

1. **Bitcoin Market Data**: Implement collector for blockchain.com APIs
2. **Crypto News**: Implement collector for cryptopanic.com APIs
3. **Data Validation**: Add schema validation and data quality checks
4. **Error Handling**: Implement retry logic and dead letter queues

### Custom Pipeline Configuration

Check `examples/` directory for sample YAML configurations.

## 🎯 Design Philosophy

**Real-time. Scalable. Rust-powered.**

- **Zero Latency**: Real-time processing from API to analytics
- **High Throughput**: Rust-based processing with minimal overhead
- **Schema Evolution**: Automatic handling of changing data structures
- **Analytics-Ready**: Direct DuckDB querying without data movement
- **Production-Ready**: ACID guarantees and fault tolerance

## 🆘 Troubleshooting

### Pipeline Won't Start

```bash
# Check Docker services
docker-compose ps

# View logs
docker-compose logs
```

### No Data in Queries

```bash
# Check MinIO buckets
curl http://localhost:9001
# Login: minioadmin/minioadmin

# Check pipeline logs
ls logs/
tail logs/*-pipeline.log
```

### Query Errors

```bash
# Verify Python dependencies
./scripts/query-data.py --help

# Check S3 connectivity
./scripts/query-data.py --interactive
# Run: SELECT 1; -- test basic DuckDB
```

## 📈 Data Schema

### Bitcoin Market Data

```sql
CREATE TABLE bitcoin_market_data (
    timestamp TIMESTAMP,
    price DOUBLE,
    volume_24h DOUBLE,
    market_cap DOUBLE,
    price_change_24h DOUBLE,
    market_sentiment VARCHAR,
    ingestion_time TIMESTAMP,
    partition_date DATE,
    partition_hour INTEGER
);
```

### Crypto News Data

```sql
CREATE TABLE crypto_news_data (
    id VARCHAR,
    title VARCHAR,
    content TEXT,
    source VARCHAR,
    published_at TIMESTAMP,
    sentiment_score DOUBLE,
    keywords ARRAY<VARCHAR>,
    sentiment_category VARCHAR,
    source_category VARCHAR,
    ingestion_time TIMESTAMP,
    partition_date DATE,
    partition_hour INTEGER
);
```

---

**Ready to process real-time crypto data at scale?** 🚀

Start the pipeline with `./scripts/supreme-pipeline.sh` and implement the API integrations for live data!

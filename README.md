# RDE - Rust Data Engineering Pipeline

A simplified, high-performance data engineering pipeline built in Rust for streaming JSON data to analytics-ready formats.

## 🚀 Quick Start

```bash
# 1. Run the complete pipeline (auto-discovers JSON datasets)
./scripts/supreme-pipeline.sh

# 2. Query your data with DuckDB
./scripts/query-data.py

# 3. Interactive SQL mode
./scripts/query-data.py --interactive
```

That's it! The pipeline will auto-discover JSON files, stream them through Kafka, process them into Parquet format, and make them queryable via DuckDB.

## 📊 What It Does

The RDE pipeline automatically:

1. **🔍 Auto-discovers** JSON datasets in `data/json-samples/`
2. **📡 Creates Kafka topics** for each dataset
3. **🔄 Streams data** from JSON → Kafka → RDE → MinIO
4. **📦 Converts to Parquet** format in Iceberg table structure
5. **📝 Generates SQL templates** for immediate analytics
6. **🎯 Enables DuckDB queries** directly on S3/MinIO data

## 🏗️ Architecture

```
JSON Files → Kafka Topics → RDE Pipeline → MinIO (S3) → DuckDB Analytics
    ✅           ✅              ⚠️             ✅         ✅
```

**Status**: End-to-end working system with a known Parquet writing bug in the RDE pipeline (see [RDE_STATUS.md](RDE_STATUS.md))

### Components

- **Kafka**: Message streaming and buffering
- **MinIO**: S3-compatible object storage
- **RDE**: Rust-based data processing engine
- **DuckDB**: Fast analytical SQL engine with S3 support
- **Docker**: Containerized infrastructure

## 📁 Project Structure (Simplified)

```
rde/
├── scripts/
│   ├── supreme-pipeline.sh     # 🚀 Main pipeline orchestrator
│   └── query-data.py          # 🔍 DuckDB analytics tool
├── crates/                    # 🦀 Rust workspace
│   ├── rde-core/             # Core pipeline logic
│   ├── rde-io/               # Kafka/MinIO connectors
│   └── rde-tx/               # Data transformations
├── examples/                  # 📋 Pipeline configurations
├── sql/templates/            # 📝 Auto-generated SQL templates
├── data/json-samples/        # 📂 Your JSON data goes here
└── docker-compose.yml        # 🐳 Infrastructure stack
```

## 🔧 Usage Guide

### 1. Add Your Data

Place JSON files in `data/json-samples/`. Supported formats:

```bash
# JSON Array format
data/json-samples/retail.json     # [{"id": 1}, {"id": 2}]

# NDJSON format
data/json-samples/flights.json    # {"flight": "AA123"}\n{"flight": "BB456"}

# Nested objects
data/json-samples/spotify.json    # {"audio_features": [{"id": 1}, {"id": 2}]}
```

### 2. Run the Pipeline

```bash
./scripts/supreme-pipeline.sh
```

This will:

- Start Kafka, MinIO, and Zookeeper
- Create topics for each JSON file
- Stream data through the pipeline
- Monitor progress and show completion status

### 3. Query Your Data

```bash
# Quick data exploration
./scripts/query-data.py

# Interactive SQL mode
./scripts/query-data.py --interactive
```

### 4. Example Queries

The system generates these automatically, but here are some examples:

```sql
-- Count records in a dataset
SELECT COUNT(*) FROM 's3://retail/**/*.parquet';

-- Sample data preview
SELECT * FROM 's3://flights/**/*.parquet' LIMIT 10;

-- Schema information
DESCRIBE SELECT * FROM 's3://spotify/**/*.parquet';

-- Cross-dataset analysis
SELECT
    'retail' as dataset, COUNT(*) as records
FROM 's3://retail/**/*.parquet'
UNION ALL
SELECT
    'flights' as dataset, COUNT(*) as records
FROM 's3://flights/**/*.parquet';
```

## 🌐 Service Access

| Service       | URL                   | Credentials             |
| ------------- | --------------------- | ----------------------- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka         | localhost:9092        | N/A                     |

## 🚦 Current Status

### ✅ **Working Components**

- Auto dataset discovery and Kafka topic creation
- JSON data streaming and processing
- MinIO storage integration
- DuckDB analytics with S3 connectivity
- Interactive SQL queries
- Auto-generated query templates

### ⚠️ **Known Issues**

- **RDE Pipeline Bug**: Parquet files may be written incorrectly for some datasets (especially `flights`)
- **Schema Evolution**: Dynamic schema handling needs improvement
- See [RDE_STATUS.md](RDE_STATUS.md) for detailed status

### 🔄 **Workaround**

The system works end-to-end. For datasets affected by the RDE bug, the pipeline will still run, and you can inspect the data structure in MinIO.

## 🛠️ Development

### Prerequisites

- Docker & Docker Compose
- Rust 1.70+
- Python 3.8+ (for DuckDB queries)

### Build

```bash
cargo build --release --bin rde-cli --bin kafka-producer
```

### Add New Data Sources

1. Place JSON files in `data/json-samples/`
2. Run `./scripts/supreme-pipeline.sh`
3. Query with `./scripts/query-data.py`

### Custom Pipeline Configuration

Check `examples/` directory for sample YAML configurations.

## 🎯 Design Philosophy

**Simple. Automated. Rust-powered.**

- **Zero Configuration**: Just drop JSON files and run
- **Auto-Discovery**: Pipeline discovers and adapts to your data
- **Performance**: Rust-based processing with minimal overhead
- **Analytics-Ready**: Direct DuckDB querying without data movement

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

---

**Ready to process your JSON data at scale?** 🚀

Drop your files in `data/json-samples/` and run `./scripts/supreme-pipeline.sh`!

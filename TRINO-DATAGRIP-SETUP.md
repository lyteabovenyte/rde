# Trino + DataGrip Setup Guide

Complete guide to query your Iceberg tables using Trino and DataGrip.

## 🏗️ Architecture Overview

```
Your JSON Data → Kafka → RDE Pipeline → Iceberg Tables (MinIO) → Trino → DataGrip
```

## 🚀 Quick Start

### 1. Start Trino Stack
```bash
# Start complete stack (Kafka, MinIO, Trino, Hive Metastore)
./scripts/setup-trino.sh
```

### 2. Create Some Data
```bash
# Run your pipeline to create Iceberg tables
RUST_LOG=info cargo run --bin rde-cli -- -p examples/flights-simple.yml &

# Stream test data
head -100 data/json-samples/flights.json > /tmp/test-flights.json
cargo run --bin kafka-producer -- -i /tmp/test-flights.json -t flights -f ndjson
```

### 3. Register Tables in Trino
```bash
# Register your tables with Trino
./scripts/run-sql.sh sql/setup/register_tables.sql
```

### 4. Query Your Data
```bash
# Run analysis queries
./scripts/run-sql.sh sql/queries/flights_analysis.sql

# Or start interactive session
./scripts/trino-cli.sh
```

## 🔧 DataGrip Connection Setup

### Step 1: Download Trino JDBC Driver
1. Download from: https://repo1.maven.org/maven2/io/trino/trino-jdbc/435/trino-jdbc-435.jar
2. Save to a local directory (e.g., `~/drivers/`)

### Step 2: Configure DataGrip Data Source

1. **Open DataGrip** → File → New → Data Source → More → Trino

2. **Connection Settings**:
   ```
   Host: localhost
   Port: 8080
   Database: iceberg
   Schema: default
   User: admin
   Password: (leave empty)
   ```

3. **Driver Settings**:
   - Click "Download missing driver files" or manually add the JAR
   - Driver class: `io.trino.jdbc.TrinoDriver`
   - Driver URL template: `jdbc:trino://{host}:{port}/{database}/{schema}`

4. **Full JDBC URL**:
   ```
   jdbc:trino://localhost:8080/iceberg/default
   ```

5. **Test Connection** → Should show "Successful"

### Step 3: Explore Your Tables

Once connected, you should see:
```
iceberg
└── default
    ├── flights_data_simple
    ├── retail_products_simple (if created)
    └── spotify_audio_features_simple (if created)
```

## 📊 Sample Queries

### Basic Table Exploration
```sql
-- List all tables
SHOW TABLES FROM iceberg.default;

-- Describe table structure
DESCRIBE iceberg.default.flights_data_simple;

-- Count records
SELECT COUNT(*) FROM iceberg.default.flights_data_simple;
```

### Flight Data Analysis
```sql
-- Daily flight statistics
SELECT fl_date,
       COUNT(*) as flights,
       AVG(dep_delay) as avg_delay,
       COUNT(CASE WHEN dep_delay > 15 THEN 1 END) as delayed_flights
FROM iceberg.default.flights_data_simple
GROUP BY fl_date
ORDER BY fl_date;

-- Delay distribution
SELECT 
    CASE 
        WHEN dep_delay <= 0 THEN 'Early/On Time'
        WHEN dep_delay <= 15 THEN 'Minor Delay'
        WHEN dep_delay <= 60 THEN 'Moderate Delay'
        ELSE 'Major Delay'
    END as delay_category,
    COUNT(*) as flights,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM iceberg.default.flights_data_simple
GROUP BY 1
ORDER BY MIN(dep_delay);
```

### Advanced Analytics
```sql
-- Moving average of delays
SELECT fl_date,
       AVG(dep_delay) as daily_avg_delay,
       AVG(AVG(dep_delay)) OVER (
           ORDER BY fl_date 
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ) as weekly_moving_avg
FROM iceberg.default.flights_data_simple
GROUP BY fl_date
ORDER BY fl_date;

-- Correlation between distance and delays
SELECT 
    ROUND(distance / 500) * 500 as distance_bucket,
    COUNT(*) as flights,
    AVG(dep_delay) as avg_departure_delay,
    AVG(arr_delay) as avg_arrival_delay,
    CORR(distance, dep_delay) as delay_distance_correlation
FROM iceberg.default.flights_data_simple
GROUP BY 1
HAVING COUNT(*) > 10
ORDER BY 1;
```

## 🛠️ Management Commands

### Table Management
```bash
# Check table status
./scripts/run-sql.sh "SHOW TABLES FROM iceberg.default"

# Table statistics
./scripts/run-sql.sh "SHOW STATS FOR iceberg.default.flights_data_simple"

# Table properties
./scripts/run-sql.sh "SHOW CREATE TABLE iceberg.default.flights_data_simple"
```

### Performance Queries
```bash
# Check query performance
./scripts/run-sql.sh "
SELECT 
    query_id,
    state,
    total_cpu_time,
    total_scheduled_time,
    total_blocked_time,
    raw_input_rows,
    raw_input_data_size
FROM system.runtime.queries 
WHERE state = 'FINISHED' 
ORDER BY created DESC 
LIMIT 10"
```

## 🔍 Troubleshooting

### Connection Issues

**Problem**: DataGrip can't connect to Trino
```bash
# Check if Trino is running
curl -s http://localhost:8080/v1/status | jq .

# Check containers
docker ps | grep trino

# View Trino logs
docker logs $(docker ps -q -f name=trino)
```

**Problem**: Tables not visible
```bash
# Re-register tables
./scripts/run-sql.sh sql/setup/register_tables.sql

# Check MinIO for table files
docker exec $(docker ps -q -f name=minio) mc ls local/iceberg-data/ --recursive
```

### Performance Issues

**Problem**: Queries are slow
```sql
-- Check file counts (too many small files can slow queries)
SELECT 
    file_count,
    total_size,
    data_file_count,
    delete_file_count
FROM iceberg.default."flights_data_simple$files";

-- Optimize table (compact small files)
ALTER TABLE iceberg.default.flights_data_simple EXECUTE optimize;
```

### Schema Issues

**Problem**: New columns not appearing
```bash
# Refresh table metadata
./scripts/run-sql.sh "CALL iceberg.system.refresh_materialized_view('iceberg', 'default', 'flights_data_simple')"

# Check Iceberg metadata
./scripts/run-sql.sh "SELECT * FROM iceberg.default.\"flights_data_simple\$history\""
```

## 📚 Useful Resources

- **Trino Documentation**: https://trino.io/docs/current/
- **Iceberg Connector**: https://trino.io/docs/current/connector/iceberg.html
- **DataGrip Trino Plugin**: https://plugins.jetbrains.com/plugin/14145-trino
- **JDBC Driver**: https://repo1.maven.org/maven2/io/trino/trino-jdbc/

## 🎯 Next Steps

1. **Create Dashboards**: Export query results to visualization tools
2. **Scheduled Analytics**: Set up automated reports using the SQL scripts
3. **Data Quality**: Add data validation queries
4. **Performance Optimization**: Monitor and optimize frequently-used queries
5. **Schema Evolution**: Test how schema changes propagate through the stack

Your Iceberg tables are now fully queryable through both command-line and DataGrip! 🎉

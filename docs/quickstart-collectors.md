# Quick Start Guide - RDE API Collectors

This guide will help you get the RDE API collectors up and running in minutes.

## Prerequisites

- Rust 1.70+ installed
- Docker and Docker Compose (for local services)
- A CryptoPanic API key (free at https://cryptopanic.com/developers/api/)

## Step 1: Clone and Setup

```bash
# Clone the repository (if not already done)
git clone <repository-url>
cd rde

# Build the project
cargo build --release
```

## Step 2: Configure API Keys

```bash
# Create secrets directory
mkdir -p secrets

# Copy the template configuration
cp secrets/api_keys.toml secrets/api_keys.toml

# Edit with your API keys
nano secrets/api_keys.toml
```

**Required changes in `secrets/api_keys.toml`:**

- Set your CryptoPanic API key: `api_key = "your_actual_api_key_here"`

## Step 3: Start Local Services (Optional)

If you don't have Kafka, ElasticSearch, or MinIO running, you can start them with Docker:

```bash
# Start local services
docker-compose up -d kafka elasticsearch minio

# Wait for services to be ready
sleep 30
```

## Step 4: Run the Collectors

### Bitcoin Market Data Collector

```bash
# Start Bitcoin collector
./scripts/start-bitcoin-collector.sh

# Or with custom settings
./scripts/start-bitcoin-collector.sh --interval 60 --log-level debug
```

### Crypto News Collector

```bash
# Start news collector (in another terminal)
./scripts/start-news-collector.sh

# Or with custom settings
./scripts/start-news-collector.sh --interval 300 --log-level debug
```

## Step 5: Verify Everything is Working

### Check Metrics

- Bitcoin Collector: http://localhost:9090/metrics
- News Collector: http://localhost:9091/metrics

### Check Health

- Bitcoin Collector: http://localhost:9090/health
- News Collector: http://localhost:9091/health

### Check Logs

```bash
# Look for successful data collection
tail -f logs/bitcoin-collector.log
tail -f logs/news-collector.log
```

## Step 6: View the Data

### Kafka Topics

```bash
# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume Bitcoin data
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bitcoin-market --from-beginning

# Consume news data
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto-news --from-beginning
```

### ElasticSearch (News Data)

```bash
# Check if news index exists
curl -s "http://localhost:9200/_cat/indices" | grep crypto-news

# Search for news
curl -s "http://localhost:9200/crypto-news/_search?q=bitcoin" | jq
```

### MinIO/S3 (Iceberg Tables)

```bash
# Access MinIO console
open http://localhost:9001

# Login with: minioadmin / minioadmin
# Browse the iceberg-data bucket
```

## Troubleshooting

### Common Issues

1. **Configuration not found**

   ```bash
   # Make sure secrets/api_keys.toml exists and has valid API keys
   ls -la secrets/
   cat secrets/api_keys.toml
   ```

2. **Services not accessible**

   ```bash
   # Check if services are running
   docker ps

   # Test connections
   curl -s "http://localhost:9200/_cluster/health" | jq
   curl -s "http://localhost:9000/minio/health/live"
   ```

3. **API rate limits**
   ```bash
   # Increase collection intervals
   ./scripts/start-bitcoin-collector.sh --interval 60
   ./scripts/start-news-collector.sh --interval 600
   ```

### Debug Mode

```bash
# Run with debug logging
export RUST_LOG=debug
./scripts/start-bitcoin-collector.sh --log-level debug
```

## Next Steps

1. **Customize Configuration**: Modify collection intervals, topics, and endpoints
2. **Add Monitoring**: Set up Prometheus and Grafana for metrics visualization
3. **Scale Up**: Run multiple collector instances for high availability
4. **Production Deployment**: Deploy to Kubernetes or cloud platforms

## Support

- Check the full documentation: [docs/collectors.md](collectors.md)
- Review logs for error messages
- Verify API keys and service connectivity
- Check the troubleshooting section above

## Example Output

### Bitcoin Data (JSON)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 43250.5,
  "volume_24h": 28450000000.0,
  "market_cap": 847500000000.0,
  "price_change_24h": 2.5,
  "source": "blockchain.com"
}
```

### News Data (JSON)

```json
{
  "id": "12345",
  "title": "Bitcoin Surges Past $43,000 as Institutional Adoption Grows",
  "content": "Bitcoin has reached new heights as major institutions...",
  "sentiment_score": 0.7,
  "keywords": ["bitcoin", "crypto", "adoption"],
  "currencies": ["BTC"],
  "engagement_score": 0.85,
  "published_at": "2024-01-15T10:00:00Z"
}
```

That's it! Your RDE API collectors are now running and collecting real-time data. 🚀

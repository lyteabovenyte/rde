# Bitcoin Market Data Collector

A real-time Bitcoin market data collector that fetches price data from the blockchain.info API and sends it to Kafka for further processing.

## Features

- **Real-time data collection**: Fetches Bitcoin price data from blockchain.info API
- **Multi-currency support**: Collects data for multiple currencies (USD, EUR, GBP, etc.)
- **Kafka integration**: Sends structured JSON data to Kafka topics
- **Configurable intervals**: Adjustable collection frequency
- **Robust error handling**: Graceful handling of API failures and network issues
- **Structured logging**: Comprehensive logging with tracing

## Data Structure

The collector fetches the following data for each currency:

```json
{
  "currency": "USD",
  "price": 111378.72,
  "buy_price": 111378.72,
  "sell_price": 111378.72,
  "price_15m": 111378.72,
  "timestamp": "2025-08-27T02:40:57.649696Z"
}
```

### Fields

- `currency`: Currency symbol (e.g., "USD", "EUR")
- `price`: Current Bitcoin price in the specified currency
- `buy_price`: Buy price
- `sell_price`: Sell price
- `price_15m`: 15-minute price
- `timestamp`: ISO 8601 timestamp when data was collected

## Usage

### Quick Start

1. **Start the infrastructure** (if not already running):

   ```bash
   ./scripts/setup.sh
   ```

2. **Start the market data collector**:

   ```bash
   ./scripts/start-market-data.sh
   ```

3. **Monitor the data** (in another terminal):
   ```bash
   ./scripts/consume-market-data.sh
   ```

### Command Line Options

```bash
cargo run --bin market-data -- [OPTIONS]

Options:
  --kafka-broker <KAFKA_BROKER>    Kafka broker address [default: localhost:9092]
  --kafka-topic <KAFKA_TOPIC>      Kafka topic for market data [default: bitcoin-market]
  --interval <INTERVAL>            Collection interval in seconds [default: 30]
  --currencies <CURRENCIES>        Target currencies (comma-separated) [default: USD,EUR,GBP,JPY,CNY,CAD,AUD,CHF]
  -h, --help                       Print help
  -V, --version                    Print version
```

### Examples

**Collect data every 10 seconds for USD and EUR only**:

```bash
cargo run --bin market-data -- --interval 10 --currencies "USD,EUR"
```

**Use custom Kafka configuration**:

```bash
cargo run --bin market-data -- \
  --kafka-broker "kafka.example.com:9092" \
  --kafka-topic "crypto-prices" \
  --interval 60 \
  --currencies "USD,EUR,GBP,JPY"
```

**Using environment variables**:

```bash
export KAFKA_BROKER="localhost:9092"
export KAFKA_TOPIC="bitcoin-market"
export INTERVAL=30
export CURRENCIES="USD,EUR,GBP"
./scripts/start-market-data.sh
```

## Configuration

### Environment Variables

- `KAFKA_BROKER`: Kafka broker address (default: localhost:9092)
- `KAFKA_TOPIC`: Kafka topic name (default: bitcoin-market)
- `INTERVAL`: Collection interval in seconds (default: 30)
- `CURRENCIES`: Comma-separated list of currencies (default: USD,EUR,GBP,JPY,CNY,CAD,AUD,CHF)

### Supported Currencies

The following currencies are supported by the blockchain.info API:

- USD (US Dollar)
- EUR (Euro)
- GBP (British Pound)
- JPY (Japanese Yen)
- CNY (Chinese Yuan)
- CAD (Canadian Dollar)
- AUD (Australian Dollar)
- CHF (Swiss Franc)
- And many more...

## Development

### Building

```bash
# Debug build
cargo build --bin market-data

# Release build
cargo build --release --bin market-data
```

### Testing

```bash
# Run tests
cargo test --bin market-data

# Run with specific configuration
cargo run --bin market-data -- --interval 5 --currencies "USD"
```

### Logging

The collector uses structured logging with the `tracing` crate. Log levels can be controlled with the `RUST_LOG` environment variable:

```bash
# Set log level
export RUST_LOG=info
cargo run --bin market-data

# Debug logging
export RUST_LOG=debug
cargo run --bin market-data
```

## Architecture

The collector consists of several components:

1. **HTTP Client**: Fetches data from blockchain.info API
2. **Data Parser**: Parses JSON response into structured data
3. **Kafka Producer**: Sends data to Kafka topics
4. **Scheduler**: Manages collection intervals
5. **Error Handler**: Handles API failures and network issues

## Error Handling

The collector implements robust error handling:

- **API failures**: Logs errors and continues with next collection cycle
- **Network timeouts**: Configurable timeouts for HTTP requests
- **Kafka failures**: Logs delivery failures but continues processing
- **Invalid data**: Skips currencies with missing or invalid data

## Monitoring

### Health Checks

The collector logs its status regularly:

- Collection start/stop events
- Data fetch success/failure
- Kafka message delivery status
- Error conditions

### Metrics

Consider integrating with Prometheus for metrics collection:

- Collection frequency
- API response times
- Kafka delivery success rate
- Error rates by type

## Troubleshooting

### Common Issues

1. **Kafka connection failed**:

   - Ensure Kafka is running: `docker ps | grep kafka`
   - Check broker address: `--kafka-broker localhost:9092`

2. **API request failed**:

   - Check network connectivity
   - Verify API endpoint is accessible
   - Check rate limiting

3. **No data in Kafka**:
   - Verify topic exists: `docker exec rde-kafka-1 kafka-topics --list`
   - Check consumer: `./scripts/consume-market-data.sh`

### Debug Mode

Enable debug logging for troubleshooting:

```bash
export RUST_LOG=debug
cargo run --bin market-data
```

## License

MIT License - see the main project LICENSE file for details.

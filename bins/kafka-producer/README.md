# Kafka Producer

A flexible Kafka producer utility for streaming JSON data from files to Kafka topics. This tool is designed to help test the RDE pipeline with various data formats and schemas.

## Features

- **Multiple JSON formats**: Supports single JSON objects, JSON arrays, and newline-delimited JSON (NDJSON)
- **Automatic format detection**: Automatically detects the JSON format if not specified
- **Batch processing**: Send messages in configurable batches
- **Rate limiting**: Add delays between messages to simulate real-time streaming
- **Error handling**: Continue processing on errors or fail fast
- **Progress tracking**: Shows progress for large files
- **Dry run mode**: Test without actually sending messages

## Usage

```bash
# Build the producer
cargo build --release --bin kafka-producer

# Send a single JSON file to a topic
cargo run --bin kafka-producer -- -i data/json-samples/user-events.json -t user-events

# Send all JSON files from a directory
cargo run --bin kafka-producer -- -i data/json-samples -t test-topic

# Send with a delay between messages (simulate streaming)
cargo run --bin kafka-producer -- -i data/json-samples/product-analytics.ndjson -t products -d 100

# Batch send (send 10 messages at a time)
cargo run --bin kafka-producer -- -i data/json-samples/schema-evolution-demo.ndjson -t evolution-test --batch-size 10

# Dry run to see what would be sent
cargo run --bin kafka-producer -- -i data/json-samples/transactions.json -t transactions --dry-run
```

## Command Line Options

- `-i, --input <PATH>`: Path to JSON file or directory containing JSON files
- `-t, --topic <TOPIC>`: Kafka topic to send messages to
- `-b, --brokers <BROKERS>`: Kafka brokers (default: localhost:9092)
- `-d, --delay-ms <MS>`: Delay between messages in milliseconds (default: 0)
- `-f, --format <FORMAT>`: JSON format: auto, object, array, ndjson (default: auto)
- `--batch-size <SIZE>`: Number of messages to send at once (default: 1)
- `--glob-pattern <PATTERN>`: Glob pattern for JSON files (default: *.json)
- `--continue-on-error`: Continue processing even if some messages fail
- `--dry-run`: Show what would be sent without actually sending
- `--progress-interval <N>`: Show progress every N messages (default: 100)

## JSON Format Examples

### Single Object (object format)
```json
{
  "id": 1,
  "name": "Single message",
  "timestamp": "2024-01-15T10:00:00Z"
}
```

### Array of Objects (array format)
```json
[
  {"id": 1, "name": "Message 1"},
  {"id": 2, "name": "Message 2"},
  {"id": 3, "name": "Message 3"}
]
```

### Newline-Delimited JSON (ndjson format)
```
{"id": 1, "name": "Message 1"}
{"id": 2, "name": "Message 2"}
{"id": 3, "name": "Message 3"}
```

## Testing Schema Evolution

The `schema-evolution-demo.ndjson` file demonstrates how fields are gradually added to messages, which helps test the RDE pipeline's schema evolution capabilities:

1. Start with basic fields (id, name, type, value)
2. Add timestamp field
3. Add nested metadata object
4. Add array field (tags)
5. Add boolean field (is_processed)
6. Add float field (score)
7. Combine all fields with a new field

## Integration with RDE Pipeline

To test the full pipeline:

1. Start the infrastructure:
   ```bash
   docker-compose up -d
   scripts/start-minio.sh
   ```

2. Run the RDE pipeline:
   ```bash
   cargo run --bin rde-cli -- -p examples/kafka-iceberg-topic-mapping.yml
   ```

3. Stream data using the producer:
   ```bash
   cargo run --bin kafka-producer -- -i data/json-samples/user-events.json -t user-events -d 1000
   ```

The pipeline will consume messages, infer/evolve the schema, apply transformations, and write to Iceberg tables.

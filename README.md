### Usage:
1. #### Read ***CSV*** files and sink into ***parquet***:
```bash
RUST_LOG=info cargo run -p rde-cli -- --pipeline examples/mini.yml
```
---
2. #### Stream JSON messages from ***kafka*** broker by subscribing to specific topics on specific brokers:
    - first build kafka docker image:
    ```bash
    docker compose up -d
    ```

    - then create the desired topic:
    ```bash
    docker exec -it <kafka_container_id> kafka-topics --create --topic sales --bootstrap-server localhost:9092
    ```

    - now we have both the broker and the topic, run the project to start subscribing to broker on the desired topic:
    ```bash
    cargo run -p rde-cli -- --pipeline examples/kafka.yml
    ```

    - start producing messages to see how it works:
    ```bash
    docker exec -it <kafka_container_id> kafka-console-producer --broker-list localhost:9092 --topic sales
    ```
<p align="center">NOTE: for now the engine only accepts json messages through kafka, so start producing json messages such as `{"id": 1, "amount": 300}`</p>

----
3. Sink received json messages to MinIO (S3 like data storage) and iceberg:

Iceberg tables can live in an “S3” bucket provided by MinIO:
```bash
# MinIO container
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

The Iceberg sink will:
- Read JSON messages from Kafka
- Convert them to Arrow RecordBatches
- Write them as Parquet files to MinIO
- Store files in the format: `s3://iceberg-bucket/sales_table/data/data-{timestamp}-{uuid}.parquet`

Note: This is a simplified implementation that writes Parquet files to MinIO. Full Iceberg metadata management (manifests, table metadata) is planned for future versions.


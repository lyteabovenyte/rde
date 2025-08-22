### Usage:
1. Read CSV files and sink into `out.parquet`:
`RUST_LOG=info cargo run -p rde-cli -- --pipeline examples/mini.yml`

2. Stream JSON messages from ***kafka*** broker by subscribing to specific topics on specific brokers:
    - first build kafka docker image:
    ```bash
    docker compose up -d
    ```

    - then create the desired topic:
    `docker exec -it <kafka_container_id> kafka-topics --create --topic sales --bootstrap-server localhost:9092`

    - now we have both the broker and the topic, run the project to start subscribing to broker on the desired topic:
    `cargo run -p rde-cli -- --pipeline examples/kafka.yml`

    - start producing messages to see how it works:
    `docker exec -it <kafka_container_id> kafka-console-producer --broker-list localhost:9092 --topic sales`
        - NOTE: for now the engine only accepts json messages through kafka, so start producing json messages such as `{"id": 1, "amount": 300}`
    
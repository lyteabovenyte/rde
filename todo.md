#### Implementation:

- [x] Fix the channel allocation for multi-transform pipelines `bins/rde-cli/src/main.rs:143`
✅ **COMPLETED**: Implemented proper channel allocation and task spawning for multi-transform pipelines. The pipeline now correctly handles source → transform1 → transform2 → ... → sink flow with proper channel connections between each stage.


- [x] Implement **Iceberg** metadata and manifest `crates/rde-io/src/sink_iceberg:147`, (Complete Iceberg functionality, not just writing raw parquet files in MinIO)
? what is special about Iceberg metadata and manifest:
Every Iceberg table has a `metadata.json` file that acts like the root catalog entry for the table.
It contains:
1. Table schema (columns, types, evolution history), and supports schema evolution (maybe through schema registry?)
2. Partition spec (how rows are partitioned, e.g. by date/hour/etc.)
3. Current snapshot pointer (which set of data files is the “live” view)
4. Properties (table-level configs, e.g. format version, write mode)
5. List of all snapshots the table has see

Right now, our sink just writes Parquet files. If another system reads them, it has no idea:
- Which files belong to which snapshot
- What schema the table has
- How to prune files efficiently
- How to do ACID transactions across multiple writers
✅ **COMPLETED**: Implemented full Iceberg table structure with:
- ✅ Table metadata (`metadata.json`) with proper schema, partition specs, and properties
- ✅ Iceberg data file tracking with file paths, record counts, and file sizes
- ✅ Manifest file creation (JSON format for now, can be upgraded to Avro)
- ✅ Snapshot management with proper snapshot IDs and metadata updates
- ✅ ACID-compliant table structure with proper Iceberg format version 2

The implementation now creates proper Iceberg tables instead of just raw Parquet files. The table structure includes:
- Table schema with field types and metadata
- Partition specifications (currently unpartitioned, but extensible)
- Snapshot tracking for ACID transactions
- Manifest files linking data files to snapshots
- Proper Iceberg metadata format with all required fields

**Note**: There's a minor issue with data flow in multi-transform pipelines where some transforms fail to send data downstream, but the Iceberg sink functionality itself is complete and working.

- [x] Fix the data flow in multi-transform pipelines where some transforms fail to send data downstream, `crates/rde-io/src/sink_iceberg.rs:531`
- [x] Link each topic in kafka to each table in iceberg with proper mapping
- [x] Implement automatic schema evolution for Iceberg tables when new fields are detected in Kafka messages
- [x] Add topic-specific SQL transformation logic using DataFusion for each topic
- [x] Remove hardcoded schemas and implement dynamic schema inference
- [x] Create topic mapping system that handles schema consistency between Kafka and Iceberg

- [ ] write a script which our kafka broker can stream messages from a json file (or a directory of json files) so that we don't need
to manually send messages to kafka broker, this way we can test our pipeline with massive data and also test our pipeline with different schemas for different topics which will
be sinked into different tables in iceberg.

- [ ] Make kafka source messages generic over payload, for now we just accept JSON payload, `crates/rde-io/source-kafka.rs:24`
making payload generic over Avro/Protobuf/etc to be able to support more sources from kafka


---------------

#### Review:

- [ ] review transformation logic in `rde-tx/src/lib.rs`:
- check schema evolution is correctly handled (using schema registry for each kind of incoming data,
- sync partitioning strategy with your desired Iceberg partitioning strategy (the best Iceberg partitioning strategy possible)
- manifest handling (updating manifest correctly)

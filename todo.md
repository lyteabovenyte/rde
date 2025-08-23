#### Implementation:

- [ ] Fix the channel allocation for multi-transform pipelines `bins/rde-cli/src/main.rs:143`
for now we are just skipping the transformation layers and connecting source -> sink


- [ ] Implement **Iceberg** metadata and manifest `crates/rde-io/src/sink_iceberg:147`, (Complete Iceberg functioanlity, not just writing raw parquet files in MinIO)
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
When we implement it, we'll need to:
- Write manifest files for the new Parquet files you just created.
- Write or update a manifest list pointing to those manifests.
- Update the table metadata JSON with a new snapshot that points to that manifest list.
- Atomically commit this new metadata file (Iceberg uses atomic rename in object stores or a catalog like Hive/Glue/Nessie).

- [ ] Make kafka source messages generic over payload, for now we just accept JSON payload, `crates/rde-io/source-kafka.rs:24`
making payload generic over Avro/Protobuf/etc to be able to support more sources from kafka


---------------

#### Review:

- [ ] review transformation logic in `rde-tx/src/lib.rs`:
- check schema evolution is correctly handled (using schema registry for each kind of incoming data,
- sync partitioning strategy with your desired Iceberg partitioning strategy (the best Iceberg partitioning strategy possible)
- manifest handling (updating manifest correctly)

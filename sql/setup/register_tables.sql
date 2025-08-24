-- Register Iceberg tables with Trino
-- Run this after your pipeline has created tables in MinIO

-- Register flights table
CALL iceberg.system.register_table(
  schema_name => 'default',
  table_name => 'flights_data_simple',
  table_location => 's3://iceberg-data/flights_data_simple'
);

-- Register retail table (if exists)
CALL iceberg.system.register_table(
  schema_name => 'default', 
  table_name => 'retail_products_simple',
  table_location => 's3://iceberg-data/retail_products_simple'
);

-- Register spotify table (if exists)
CALL iceberg.system.register_table(
  schema_name => 'default',
  table_name => 'spotify_audio_features_simple', 
  table_location => 's3://iceberg-data/spotify_audio_features_simple'
);

-- Show registered tables
SHOW TABLES FROM iceberg.default;

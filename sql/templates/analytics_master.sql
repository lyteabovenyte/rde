-- Master Analytics Queries
-- Cross-dataset analysis and reporting

-- Available datasets: flights retail spotify

-- 1. Dataset summary
SELECT 'flights' as dataset, COUNT(*) as record_count FROM 's3://flights/**/*.parquet'
UNION ALL
SELECT 'retail' as dataset, COUNT(*) as record_count FROM 's3://retail/**/*.parquet'
UNION ALL
SELECT 'spotify' as dataset, COUNT(*) as record_count FROM 's3://spotify/**/*.parquet'
UNION ALL;

-- 2. Sample data from each dataset
-- Sample from flights:
SELECT 'flights' as source, * FROM 's3://flights/**/*.parquet' LIMIT 3;

-- Sample from retail:
SELECT 'retail' as source, * FROM 's3://retail/**/*.parquet' LIMIT 3;

-- Sample from spotify:
SELECT 'spotify' as source, * FROM 's3://spotify/**/*.parquet' LIMIT 3;

-- TODO: Add cross-dataset join queries and advanced analytics


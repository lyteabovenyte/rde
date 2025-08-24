-- Uflights Dataset Analysis Queries
-- Auto-generated SQL templates for flights data

-- 1. Basic data overview
SELECT COUNT(*) as total_records
FROM 's3://flights/**/*.parquet';

-- 2. Sample data preview  
SELECT *
FROM 's3://flights/**/*.parquet'
LIMIT 10;

-- 3. Schema information
DESCRIBE SELECT * FROM 's3://flights/**/*.parquet';

-- 4. Column analysis
SELECT * FROM (
    DESCRIBE SELECT * FROM 's3://flights/**/*.parquet'
) LIMIT 20;

-- TODO: Add your custom flights analysis queries below
-- Examples:
-- - Time-based analysis
-- - Aggregations and grouping
-- - Business metrics calculations
-- - Data trends and patterns


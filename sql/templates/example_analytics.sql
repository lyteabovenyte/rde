-- Example Advanced Analytics Queries
-- These examples show powerful patterns you can use with your data

-- 1. FLIGHTS ANALYTICS
-- Flight performance analysis
SELECT 
    FL_DATE,
    AVG(DEP_DELAY) as avg_departure_delay,
    AVG(ARR_DELAY) as avg_arrival_delay,
    COUNT(*) as total_flights,
    SUM(CASE WHEN DEP_DELAY > 15 THEN 1 ELSE 0 END) as delayed_departures,
    SUM(CASE WHEN ARR_DELAY > 15 THEN 1 ELSE 0 END) as delayed_arrivals
FROM iceberg.default.flights
GROUP BY FL_DATE
ORDER BY FL_DATE;

-- Delay correlation analysis
SELECT 
    CASE 
        WHEN DEP_DELAY <= 0 THEN 'On Time'
        WHEN DEP_DELAY <= 15 THEN 'Minor Delay'
        WHEN DEP_DELAY <= 60 THEN 'Moderate Delay'
        ELSE 'Major Delay'
    END as departure_category,
    AVG(ARR_DELAY) as avg_arrival_delay,
    COUNT(*) as flight_count
FROM iceberg.default.flights
GROUP BY 1
ORDER BY avg_arrival_delay;

-- 2. RETAIL ANALYTICS  
-- Product performance analysis
SELECT 
    brand,
    COUNT(*) as product_count,
    AVG(CAST(regexp_replace(actual_price, '[,$]', '') AS DOUBLE)) as avg_price,
    AVG(CAST(average_rating AS DOUBLE)) as avg_rating,
    AVG(CAST(no_of_ratings AS INTEGER)) as avg_num_ratings
FROM iceberg.default.retail
WHERE actual_price IS NOT NULL 
  AND average_rating IS NOT NULL
GROUP BY brand
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC;

-- Price vs Rating correlation
SELECT 
    CASE 
        WHEN CAST(regexp_replace(actual_price, '[,$]', '') AS DOUBLE) < 500 THEN 'Budget'
        WHEN CAST(regexp_replace(actual_price, '[,$]', '') AS DOUBLE) < 2000 THEN 'Mid-Range'
        ELSE 'Premium'
    END as price_category,
    AVG(CAST(average_rating AS DOUBLE)) as avg_rating,
    COUNT(*) as product_count
FROM iceberg.default.retail
WHERE actual_price IS NOT NULL 
  AND average_rating IS NOT NULL
GROUP BY 1;

-- 3. SPOTIFY ANALYTICS
-- Music characteristics analysis
SELECT 
    danceability_category,
    energy_category,
    mood_category,
    COUNT(*) as track_count,
    AVG(tempo) as avg_tempo,
    AVG(duration_seconds) as avg_duration_seconds
FROM iceberg.default.spotify_audio_features
GROUP BY danceability_category, energy_category, mood_category
ORDER BY track_count DESC;

-- Tempo vs Energy correlation
SELECT 
    tempo_category,
    AVG(energy) as avg_energy,
    AVG(danceability) as avg_danceability,
    AVG(valence) as avg_positivity,
    COUNT(*) as track_count
FROM iceberg.default.spotify_audio_features
GROUP BY tempo_category
ORDER BY avg_energy DESC;

-- 4. CROSS-DATASET ANALYSIS
-- Compare dataset sizes and characteristics
SELECT 
    'flights' as dataset,
    COUNT(*) as record_count,
    'Flight operations data' as description
FROM iceberg.default.flights

UNION ALL

SELECT 
    'retail' as dataset,
    COUNT(*) as record_count,
    'E-commerce product data' as description
FROM iceberg.default.retail

UNION ALL

SELECT 
    'spotify' as dataset,
    COUNT(*) as record_count,
    'Music audio features data' as description
FROM iceberg.default.spotify_audio_features

ORDER BY record_count DESC;

-- 5. TIME-BASED ANALYSIS PATTERNS
-- Example: Daily trends (adjust based on your timestamp columns)
-- Note: Add actual timestamp columns from your data

-- 6. BUSINESS METRICS EXAMPLES
-- Key Performance Indicators template
SELECT 
    'Flight On-Time Performance' as metric,
    CAST(
        SUM(CASE WHEN DEP_DELAY <= 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        AS DECIMAL(5,2)
    ) as value_pct
FROM iceberg.default.flights

UNION ALL

SELECT 
    'Average Product Rating' as metric,
    CAST(
        AVG(CAST(average_rating AS DOUBLE))
        AS DECIMAL(5,2)
    ) as value_pct
FROM iceberg.default.retail
WHERE average_rating IS NOT NULL

UNION ALL

SELECT 
    'High Energy Music Percentage' as metric,
    CAST(
        SUM(CASE WHEN energy_category = 'high_energy' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        AS DECIMAL(5,2)
    ) as value_pct
FROM iceberg.default.spotify_audio_features;

-- 7. DATA QUALITY DASHBOARD
-- Monitor data completeness and quality
SELECT 
    'flights' as dataset,
    'dep_delay' as column_name,
    COUNT(*) as total_records,
    COUNT(dep_delay) as non_null_records,
    CAST(COUNT(dep_delay) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
FROM iceberg.default.flights

UNION ALL

SELECT 
    'retail' as dataset,
    'actual_price' as column_name,
    COUNT(*) as total_records,
    COUNT(actual_price) as non_null_records,
    CAST(COUNT(actual_price) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completeness_pct
FROM iceberg.default.retail

ORDER BY dataset, column_name;

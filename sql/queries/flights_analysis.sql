-- Flight Data Analysis Queries

-- 1. Basic table overview
SELECT COUNT(*) as total_flights, 
       MIN(fl_date) as earliest_date,
       MAX(fl_date) as latest_date
FROM iceberg.default.flights_data_simple;

-- 2. Delay statistics by date
SELECT fl_date,
       COUNT(*) as flights_count,
       AVG(dep_delay) as avg_departure_delay,
       AVG(arr_delay) as avg_arrival_delay,
       COUNT(CASE WHEN dep_delay > 15 THEN 1 END) as delayed_departures,
       COUNT(CASE WHEN arr_delay > 15 THEN 1 END) as delayed_arrivals
FROM iceberg.default.flights_data_simple
GROUP BY fl_date
ORDER BY fl_date;

-- 3. Flight performance distribution
SELECT 
    CASE 
        WHEN dep_delay <= 0 AND arr_delay <= 0 THEN 'Early'
        WHEN dep_delay BETWEEN 0 AND 15 AND arr_delay BETWEEN 0 AND 15 THEN 'On Time'
        WHEN dep_delay > 15 OR arr_delay > 15 THEN 'Delayed'
        ELSE 'Unknown'
    END as performance_category,
    COUNT(*) as flight_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM iceberg.default.flights_data_simple
GROUP BY 1
ORDER BY flight_count DESC;

-- 4. Distance vs delay correlation
SELECT 
    CASE 
        WHEN distance < 500 THEN 'Short (<500mi)'
        WHEN distance < 1000 THEN 'Medium (500-1000mi)'
        WHEN distance < 2000 THEN 'Long (1000-2000mi)'
        ELSE 'Very Long (>2000mi)'
    END as distance_category,
    COUNT(*) as flights,
    AVG(dep_delay) as avg_dep_delay,
    AVG(arr_delay) as avg_arr_delay,
    AVG(air_time) as avg_air_time
FROM iceberg.default.flights_data_simple
GROUP BY 1
ORDER BY 1;

-- 5. Worst delay days
SELECT fl_date,
       AVG(dep_delay + arr_delay) as total_avg_delay,
       MAX(dep_delay) as max_departure_delay,
       MAX(arr_delay) as max_arrival_delay,
       COUNT(*) as flights_count
FROM iceberg.default.flights_data_simple
WHERE dep_delay > 0 OR arr_delay > 0
GROUP BY fl_date
ORDER BY total_avg_delay DESC
LIMIT 10;

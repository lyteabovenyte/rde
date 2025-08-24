-- Retail Product Analysis Queries

-- 1. Table overview
SELECT COUNT(*) as total_products,
       COUNT(DISTINCT brand) as unique_brands,
       COUNT(DISTINCT category) as unique_categories,
       MIN(actual_price) as min_price,
       MAX(actual_price) as max_price,
       AVG(actual_price) as avg_price
FROM iceberg.default.retail_products_simple;

-- 2. Top brands by product count
SELECT brand,
       COUNT(*) as product_count,
       AVG(actual_price) as avg_price,
       AVG(average_rating) as avg_rating
FROM iceberg.default.retail_products_simple
WHERE brand IS NOT NULL
GROUP BY brand
ORDER BY product_count DESC
LIMIT 20;

-- 3. Category analysis
SELECT category,
       COUNT(*) as products,
       AVG(actual_price) as avg_price,
       MIN(actual_price) as min_price,
       MAX(actual_price) as max_price,
       AVG(average_rating) as avg_rating,
       COUNT(CASE WHEN out_of_stock = true THEN 1 END) as out_of_stock_count
FROM iceberg.default.retail_products_simple
GROUP BY category
ORDER BY products DESC;

-- 4. Price distribution
SELECT 
    CASE 
        WHEN actual_price < 100 THEN 'Under $100'
        WHEN actual_price < 500 THEN '$100-500'
        WHEN actual_price < 1000 THEN '$500-1000'
        WHEN actual_price < 5000 THEN '$1000-5000'
        ELSE 'Over $5000'
    END as price_range,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM iceberg.default.retail_products_simple
GROUP BY 1
ORDER BY MIN(actual_price);

-- 5. Rating analysis
SELECT 
    CASE 
        WHEN average_rating >= 4.5 THEN 'Excellent (4.5+)'
        WHEN average_rating >= 4.0 THEN 'Very Good (4.0-4.5)'
        WHEN average_rating >= 3.0 THEN 'Good (3.0-4.0)'
        WHEN average_rating >= 2.0 THEN 'Fair (2.0-3.0)'
        ELSE 'Poor (<2.0)'
    END as rating_category,
    COUNT(*) as products,
    AVG(actual_price) as avg_price
FROM iceberg.default.retail_products_simple
WHERE average_rating IS NOT NULL
GROUP BY 1
ORDER BY MIN(average_rating) DESC;

-- 6. Inventory status
SELECT 
    out_of_stock,
    COUNT(*) as products,
    AVG(actual_price) as avg_price,
    AVG(average_rating) as avg_rating
FROM iceberg.default.retail_products_simple
GROUP BY out_of_stock;

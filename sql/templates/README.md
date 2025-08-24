# SQL Query Templates

This directory contains ready-to-use SQL query templates for analyzing your data in Trino.

## Auto-Generated Templates

When you run `./scripts/supreme-pipeline.sh`, it will automatically generate SQL templates in `sql/auto-generated/` for each dataset discovered in `data/json-samples/`.

## Usage

1. **Run the supreme pipeline** to generate dataset-specific templates:
   ```bash
   ./scripts/supreme-pipeline.sh
   ```

2. **Execute queries** using the run-sql script:
   ```bash
   ./scripts/run-sql.sh sql/auto-generated/flights_queries.sql
   ./scripts/run-sql.sh sql/auto-generated/analytics_master.sql
   ```

3. **Connect with DataGrip**:
   - URL: `jdbc:trino://localhost:8080/iceberg/default`
   - User: any username (no password required)
   - Driver: Trino JDBC Driver

## Template Structure

Each dataset gets its own SQL file with:
- Basic data overview queries
- Schema inspection
- Data quality checks  
- Placeholder sections for custom analytics

## Available Datasets

The supreme pipeline automatically detects and processes:
- **flights**: Flight delay and performance data
- **retail**: E-commerce product and sales data  
- **spotify**: Audio features and music analytics data

## Custom Analytics

Add your domain-specific queries to the generated templates or create new files in this directory.

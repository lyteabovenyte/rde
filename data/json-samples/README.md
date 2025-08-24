# JSON Sample Data

This directory contains sample JSON datasets for testing the RDE pipeline. Due to GitHub's file size limits, the large datasets are not included in the repository.

## Available Datasets

### Small Dataset (Included)
- **spotify.json** (60KB) - Spotify audio features data with nested JSON structure
  - Format: Nested JSON with `audio_features` array
  - Records: ~2,000 audio features
  - Use case: Testing nested JSON processing and music analytics

### Large Datasets (Not Included - Too Big for GitHub)
The following files need to be added manually:

- **flights.json** (117MB) - Flight delay and performance data
  - Format: NDJSON (newline-delimited JSON)
  - Records: ~1,000,000 flight records
  - Fields: FL_DATE, DEP_DELAY, ARR_DELAY, AIR_TIME, DISTANCE, etc.
  - Use case: Large-scale time series analytics, delay prediction

- **retail.json** (79MB) - E-commerce product catalog data
  - Format: JSON array
  - Records: ~2,000,000 product listings
  - Fields: product_id, price, rating, category, brand, etc.
  - Use case: Product analytics, pricing strategies, inventory management

## How to Add Large Datasets

You can source these datasets from:

1. **Public datasets** (recommended):
   - Flight data: [US DOT Bureau of Transportation Statistics](https://www.transtats.bts.gov/)
   - Retail data: [Kaggle E-commerce datasets](https://www.kaggle.com/datasets?search=ecommerce)

2. **Generate test data**:
   ```bash
   # Generate sample data using the included script
   ./scripts/generate-test-data.py -t flights -n 1000000 -f ndjson -o data/json-samples
   ./scripts/generate-test-data.py -t retail -n 2000000 -f array -o data/json-samples
   ```

3. **Download from your data sources** and place them in this directory with the expected names.

## File Format Requirements

### flights.json
```json
{"FL_DATE":"2006-01-01","DEP_DELAY":5,"ARR_DELAY":19,"AIR_TIME":350,"DISTANCE":2475,"DEP_TIME":9.083333,"ARR_TIME":12.483334}
{"FL_DATE":"2006-01-02","DEP_DELAY":167,"ARR_DELAY":216,"AIR_TIME":343,"DISTANCE":2475,"DEP_TIME":11.783334,"ARR_TIME":15.766666}
```

### retail.json
```json
[
  {
    "_id": "product-id-1",
    "actual_price": "2,999",
    "average_rating": "3.9",
    "brand": "Brand Name",
    "category": "Electronics",
    "description": "Product description...",
    "out_of_stock": false
  }
]
```

## Testing Without Large Files

If you don't have the large datasets, you can still test the pipeline:

1. **Use the data generator**:
   ```bash
   ./scripts/generate-test-data.py -t all -n 1000
   ```

2. **Use small sample files**:
   ```bash
   # Create small test files
   head -1000 your-flights-data.json > data/json-samples/flights.json
   jq '.[0:1000]' your-retail-data.json > data/json-samples/retail.json
   ```

3. **Test with spotify.json only**:
   ```bash
   ./quick-start.sh  # Choose spotify pipeline only
   ```

## Pipeline Compatibility

These datasets are designed to work with:
- `examples/flights-pipeline.yml` or `examples/flights-simple.yml`
- `examples/retail-pipeline.yml` or `examples/retail-simple.yml`  
- `examples/spotify-pipeline.yml` or `examples/spotify-simple.yml`

The pipeline will automatically:
- ✅ Infer schema from JSON structure
- ✅ Handle schema evolution as new fields are added
- ✅ Apply appropriate data transformations
- ✅ Write to partitioned Iceberg tables

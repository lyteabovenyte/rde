#!/usr/bin/env python3
"""
Simple script to query Iceberg tables in MinIO using DuckDB
"""

import duckdb
import pandas as pd
import os
import sys
import subprocess
import socket
from pathlib import Path

def setup_duckdb():
    """Setup DuckDB with S3 and Iceberg extensions"""
    conn = duckdb.connect()
    
    # Install required extensions
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Configure S3/MinIO connection
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    
    # Set S3 configuration for DuckDB
    conn.execute(f"SET s3_endpoint='{minio_endpoint}';")
    conn.execute(f"SET s3_access_key_id='{access_key}';")
    conn.execute(f"SET s3_secret_access_key='{secret_key}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    return conn

def test_connection(conn):
    """Test S3/MinIO connection"""
    try:
        # Try a simple query to test connection
        conn.execute("SELECT 1 as test;").fetchall()
        print("✅ DuckDB S3 connection configured")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def debug_bucket_contents(conn, bucket_name):
    """Debug function to see what's actually in a bucket"""
    try:
        print(f"🔍 Exploring bucket: {bucket_name}")
        
        # Try to list top-level contents
        try:
            result = conn.execute(f"SELECT * FROM glob('s3://{bucket_name}/*') LIMIT 10;").fetchall()
            if result:
                print(f"   Top-level contents:")
                for item in result:
                    print(f"     - {item[0]}")
        except Exception as e:
            print(f"   Error listing top-level: {e}")
            
        # Try to find any parquet files
        try:
            result = conn.execute(f"SELECT * FROM glob('s3://{bucket_name}/**/*.parquet') LIMIT 5;").fetchall()
            if result:
                print(f"   Found parquet files:")
                for item in result:
                    print(f"     - {item[0]}")
            else:
                print(f"   No parquet files found")
        except Exception as e:
            print(f"   Error finding parquet files: {e}")
            
    except Exception as e:
        print(f"   Error exploring bucket {bucket_name}: {e}")

def query_dataset(conn, dataset_name):
    """Query a dataset directly by name"""
    try:
        # Try different common patterns for where data might be stored
        patterns = [
            f"s3://{dataset_name}/{dataset_name}/data/*.parquet",    # Simple structure
            f"s3://{dataset_name}/{dataset_name}/data/**/*.parquet",  # Iceberg structure  
            f"s3://{dataset_name}/**/*.parquet",
            f"s3://data/{dataset_name}/**/*.parquet", 
            f"s3://iceberg/{dataset_name}/**/*.parquet",
            f"s3://iceberg-data/{dataset_name}/**/*.parquet"
        ]
        
        for pattern in patterns:
            try:
                query = f"SELECT COUNT(*) as record_count FROM '{pattern}';"
                result = conn.execute(query).fetchone()
                
                if result and result[0] > 0:
                    print(f"✅ Found {result[0]} records in {dataset_name}")
                    print(f"   Pattern: {pattern}")
                    
                    # Show sample data
                    sample_query = f"SELECT * FROM '{pattern}' LIMIT 5;"
                    sample_result = conn.execute(sample_query).fetchdf()
                    print(f"   Sample data:")
                    print(sample_result)
                    print()
                    return pattern
                    
            except Exception:
                continue
                
        print(f"❌ No data found for {dataset_name}")
        return None
        
    except Exception as e:
        print(f"Error querying {dataset_name}: {e}")
        return None

def interactive_query(conn):
    """Interactive SQL query mode"""
    print("\n=== Interactive SQL Mode ===")
    print("Enter SQL queries (type 'exit' to quit):")
    print("Examples:")
    print("  SELECT * FROM 's3://flights/**/*.parquet' LIMIT 5;")
    print("  SELECT COUNT(*) FROM 's3://retail/**/*.parquet';")
    print("  DESCRIBE SELECT * FROM 's3://spotify/**/*.parquet';")
    print()
    
    while True:
        try:
            query = input("SQL> ").strip()
            if query.lower() in ['exit', 'quit']:
                break
                
            if not query:
                continue
                
            result = conn.execute(query).fetchdf()
            print(result)
            print()
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")

def check_dependencies():
    """Check and install dependencies"""
    print("🦆 DuckDB Iceberg Query Tool")
    print("=" * 40)
    
    # Check if MinIO is running
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 9000))
        sock.close()
        if result != 0:
            print("❌ MinIO is not running on localhost:9000")
            print("💡 Start MinIO with: docker-compose up -d minio")
            sys.exit(1)
        print("✅ MinIO is available")
    except Exception:
        print("❌ Cannot check MinIO connection")
        sys.exit(1)
    
    # Check and install Python dependencies
    missing_deps = []
    try:
        import duckdb
    except ImportError:
        missing_deps.append('duckdb')
    
    try:
        import pandas
    except ImportError:
        missing_deps.append('pandas')
        
    try:
        import boto3
    except ImportError:
        missing_deps.append('boto3')
    
    if missing_deps:
        print(f"📦 Installing missing dependencies: {', '.join(missing_deps)}")
        subprocess.run([sys.executable, '-m', 'pip', 'install'] + missing_deps, check=True)
        print("✅ Dependencies installed")
    else:
        print("✅ Dependencies available")

def main():
    """Main function"""
    check_dependencies()
    
    # Setup DuckDB
    conn = setup_duckdb()
    
    # Test connection
    if not test_connection(conn):
        print("Failed to setup DuckDB connection")
        return
    
    # Optional: Debug bucket contents (uncomment if needed)
    # print("🔍 Debugging bucket contents...")
    # buckets = ['flights', 'retail', 'spotify', 'iceberg-data']
    # for bucket in buckets:
    #     debug_bucket_contents(conn, bucket)
    #     print()
    
    # Try to query common dataset names
    datasets = ['flights', 'retail', 'spotify']
    found_datasets = []
    
    print("🔍 Searching for datasets...")
    for dataset in datasets:
        pattern = query_dataset(conn, dataset)
        if pattern:
            found_datasets.append((dataset, pattern))
    
    if not found_datasets:
        print("\n❌ No datasets found!")
        print("Make sure you've run the pipeline first: ./scripts/supreme-pipeline.sh")
        print("Data should be in MinIO at localhost:9000")
        return
    
    print(f"\n🎉 Found {len(found_datasets)} datasets!")
    
    # Interactive mode
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        print("\nAvailable datasets:")
        for dataset, pattern in found_datasets:
            print(f"  - {dataset}: {pattern}")
        interactive_query(conn)
    
    print("🎉 Query execution completed!")

if __name__ == "__main__":
    main()

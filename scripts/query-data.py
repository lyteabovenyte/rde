#!/usr/bin/env python3
"""
Crypto Data Analytics Tool

Query and analyze real-time Bitcoin market data and crypto news data
stored in Iceberg tables via DuckDB.

Usage:
    ./scripts/query-crypto-data.py                    # Quick overview
    ./scripts/query-crypto-data.py --interactive      # Interactive mode
    ./scripts/query-crypto-data.py --query "SELECT * FROM bitcoin_market_data LIMIT 5"
"""

import argparse
import sys
import duckdb
import os
from pathlib import Path
from typing import List, Optional
import json

# Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_REGION = "us-east-1"

# Crypto data tables
CRYPTO_TABLES = {
    "bitcoin_market_data": "s3://crypto-data/bitcoin_market_data/**/*.parquet",
    "crypto_news_data": "s3://crypto-data/crypto_news_data/**/*.parquet"
}

def setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Set up DuckDB connection with S3 support."""
    conn = duckdb.connect(':memory:')
    
    # Install and load S3 extension
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    
    # Configure S3 credentials
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}'")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}'")
    conn.execute(f"SET s3_region='{MINIO_REGION}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    return conn

def get_table_info(conn: duckdb.DuckDBPyConnection, table_name: str) -> dict:
    """Get information about a table."""
    try:
        # Check if table has data
        count_result = conn.execute(f"SELECT COUNT(*) FROM '{CRYPTO_TABLES[table_name]}'").fetchone()
        count = count_result[0] if count_result else 0
        
        if count == 0:
            return {"name": table_name, "count": 0, "status": "empty"}
        
        # Get sample data
        sample = conn.execute(f"SELECT * FROM '{CRYPTO_TABLES[table_name]}' LIMIT 3").fetchdf()
        
        # Get schema
        schema_result = conn.execute(f"DESCRIBE SELECT * FROM '{CRYPTO_TABLES[table_name]}'").fetchdf()
        
        return {
            "name": table_name,
            "count": count,
            "status": "active",
            "sample": sample,
            "schema": schema_result
        }
    except Exception as e:
        return {"name": table_name, "count": 0, "status": "error", "error": str(e)}

def print_table_overview(conn: duckdb.DuckDBPyConnection):
    """Print overview of all crypto data tables."""
    print("🔍 Crypto Data Overview")
    print("=" * 50)
    
    for table_name in CRYPTO_TABLES:
        info = get_table_info(conn, table_name)
        
        if info["status"] == "active":
            print(f"✅ {info['name']}: {info['count']:,} records")
            if not info['sample'].empty:
                print(f"   📊 Sample columns: {', '.join(info['sample'].columns[:5])}")
        elif info["status"] == "empty":
            print(f"⚠️  {info['name']}: No data yet")
        else:
            print(f"❌ {info['name']}: Error - {info.get('error', 'Unknown error')}")
    
    print()

def run_bitcoin_analytics(conn: duckdb.DuckDBPyConnection):
    """Run Bitcoin market data analytics."""
    print("📈 Bitcoin Market Analytics")
    print("=" * 40)
    
    try:
        # Latest price
        latest = conn.execute("""
            SELECT 
                timestamp,
                price,
                volume_24h,
                market_cap,
                price_change_24h,
                market_sentiment
            FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet'
            ORDER BY timestamp DESC
            LIMIT 1
        """).fetchdf()
        
        if not latest.empty:
            row = latest.iloc[0]
            print(f"💰 Latest Price: ${row['price']:,.2f}")
            print(f"📊 24h Volume: ${row['volume_24h']:,.0f}")
            print(f"🏦 Market Cap: ${row['market_cap']:,.0f}")
            print(f"📈 24h Change: {row['price_change_24h']:+.2f}%")
            print(f"🎯 Sentiment: {row['market_sentiment']}")
        
        # Price trends
        trends = conn.execute("""
            SELECT 
                DATE(timestamp) as date,
                AVG(price) as avg_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                COUNT(*) as data_points
            FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet'
            WHERE timestamp >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
        """).fetchdf()
        
        if not trends.empty:
            print(f"\n📅 Last 7 Days Price Trends:")
            for _, row in trends.iterrows():
                print(f"   {row['date']}: ${row['avg_price']:,.2f} (${row['min_price']:,.2f} - ${row['max_price']:,.2f})")
        
    except Exception as e:
        print(f"❌ Error analyzing Bitcoin data: {e}")
    
    print()

def run_news_analytics(conn: duckdb.DuckDBPyConnection):
    """Run crypto news analytics."""
    print("📰 Crypto News Analytics")
    print("=" * 35)
    
    try:
        # Recent news count
        recent_news = conn.execute("""
            SELECT COUNT(*) as count
            FROM 's3://crypto-data/crypto_news_data/**/*.parquet'
            WHERE published_at >= CURRENT_TIMESTAMP - INTERVAL 24 HOUR
        """).fetchone()
        
        if recent_news:
            print(f"📰 News in last 24h: {recent_news[0]:,}")
        
        # Sentiment analysis
        sentiment = conn.execute("""
            SELECT 
                sentiment_category,
                COUNT(*) as count,
                AVG(sentiment_score) as avg_sentiment
            FROM 's3://crypto-data/crypto_news_data/**/*.parquet'
            WHERE published_at >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY sentiment_category
            ORDER BY count DESC
        """).fetchdf()
        
        if not sentiment.empty:
            print(f"\n😊 Sentiment Analysis (Last 7 Days):")
            for _, row in sentiment.iterrows():
                print(f"   {row['sentiment_category']}: {row['count']} articles (avg: {row['avg_sentiment']:.2f})")
        
        # Top sources
        sources = conn.execute("""
            SELECT 
                source,
                COUNT(*) as article_count
            FROM 's3://crypto-data/crypto_news_data/**/*.parquet'
            WHERE published_at >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY source
            ORDER BY article_count DESC
            LIMIT 5
        """).fetchdf()
        
        if not sources.empty:
            print(f"\n📚 Top News Sources (Last 7 Days):")
            for _, row in sources.iterrows():
                print(f"   {row['source']}: {row['article_count']} articles")
        
    except Exception as e:
        print(f"❌ Error analyzing news data: {e}")
    
    print()

def run_cross_analysis(conn: duckdb.DuckDBPyConnection):
    """Run cross-analysis between Bitcoin price and news sentiment."""
    print("🔗 Bitcoin Price vs News Sentiment")
    print("=" * 40)
    
    try:
        # Correlation analysis
        correlation = conn.execute("""
            WITH daily_metrics AS (
                SELECT 
                    DATE(b.timestamp) as date,
                    AVG(b.price) as avg_price,
                    AVG(b.price_change_24h) as avg_price_change,
                    COUNT(n.id) as news_count,
                    AVG(n.sentiment_score) as avg_sentiment
                FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet' b
                LEFT JOIN 's3://crypto-data/crypto_news_data/**/*.parquet' n
                    ON DATE(b.timestamp) = DATE(n.published_at)
                WHERE b.timestamp >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY DATE(b.timestamp)
            )
            SELECT 
                COUNT(*) as days_with_data,
                AVG(avg_price) as avg_daily_price,
                AVG(news_count) as avg_daily_news,
                AVG(avg_sentiment) as avg_daily_sentiment
            FROM daily_metrics
            WHERE news_count > 0
        """).fetchone()
        
        if correlation:
            print(f"📊 Analysis Period: {correlation[0]} days")
            print(f"💰 Average Daily Price: ${correlation[1]:,.2f}")
            print(f"📰 Average Daily News: {correlation[2]:.1f} articles")
            print(f"😊 Average Daily Sentiment: {correlation[3]:.2f}")
        
    except Exception as e:
        print(f"❌ Error in cross-analysis: {e}")
    
    print()

def interactive_mode(conn: duckdb.DuckDBPyConnection):
    """Run interactive SQL mode."""
    print("🔍 Interactive SQL Mode")
    print("=" * 25)
    print("Available tables:")
    for table_name, path in CRYPTO_TABLES.items():
        print(f"  - {table_name} (path: {path})")
    print("\nType 'exit' to quit, 'help' for examples")
    print("-" * 50)
    
    while True:
        try:
            query = input("SQL> ").strip()
            
            if query.lower() in ['exit', 'quit']:
                break
            elif query.lower() == 'help':
                print_help_examples()
                continue
            elif not query:
                continue
            
            # Execute query
            result = conn.execute(query).fetchdf()
            
            if not result.empty:
                print(result.to_string(index=False))
            else:
                print("Query executed successfully (no results)")
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

def print_help_examples():
    """Print helpful SQL examples."""
    print("\n📝 SQL Examples:")
    print("-" * 20)
    print("-- Latest Bitcoin price")
    print("SELECT * FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet' ORDER BY timestamp DESC LIMIT 1;")
    print()
    print("-- Recent news sentiment")
    print("SELECT sentiment_category, COUNT(*) FROM 's3://crypto-data/crypto_news_data/**/*.parquet' GROUP BY sentiment_category;")
    print()
    print("-- Price trends by hour")
    print("SELECT EXTRACT(HOUR FROM timestamp) as hour, AVG(price) FROM 's3://crypto-data/bitcoin_market_data/**/*.parquet' GROUP BY hour ORDER BY hour;")
    print()

def main():
    parser = argparse.ArgumentParser(description="Crypto Data Analytics Tool")
    parser.add_argument("--interactive", "-i", action="store_true", help="Start interactive SQL mode")
    parser.add_argument("--query", "-q", type=str, help="Execute a specific SQL query")
    parser.add_argument("--overview", "-o", action="store_true", help="Show data overview only")
    
    args = parser.parse_args()
    
    try:
        # Set up connection
        conn = setup_duckdb_connection()
        
        if args.query:
            # Execute specific query
            result = conn.execute(args.query).fetchdf()
            if not result.empty:
                print(result.to_string(index=False))
            else:
                print("Query executed successfully (no results)")
        
        elif args.interactive:
            # Interactive mode
            print_table_overview(conn)
            interactive_mode(conn)
        
        elif args.overview:
            # Overview only
            print_table_overview(conn)
        
        else:
            # Default: show all analytics
            print_table_overview(conn)
            run_bitcoin_analytics(conn)
            run_news_analytics(conn)
            run_cross_analysis(conn)
            
            print("💡 Tip: Use --interactive for custom queries or --help for more options")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# API Integration Guide

This guide explains how to implement real-time data collectors for Bitcoin market data and crypto news to feed into the RDE pipeline.

## Overview

The RDE pipeline is designed to process real-time data from external APIs. Currently, we need to implement collectors for:

1. **Bitcoin Market Data** - From blockchain.com APIs
2. **Crypto News Data** - From cryptopanic.com APIs

## Architecture

```
External APIs → Data Collectors → Kafka Topics → RDE Pipeline → Iceberg Tables
     🔗              📡              📊              ⚡              📦
```

## 1. Bitcoin Market Data Collector

### API Source: blockchain.com

Blockchain.com provides free APIs for Bitcoin market data:

- **Ticker API**: `https://api.blockchain.com/v3/exchange/tickers/BTC-USD`
- **Price API**: `https://api.blockchain.com/v3/exchange/rates`
- **Stats API**: `https://api.blockchain.com/v3/stats`

### Implementation Strategy

Create a Rust service that:

1. **Polls APIs** at regular intervals (e.g., every 30 seconds)
2. **Transforms data** to match our schema
3. **Sends to Kafka** topic `bitcoin-market`

### Example Implementation

```rust
// crates/rde-collectors/src/bitcoin_collector.rs
use reqwest::Client;
use serde_json::Value;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::{interval, Duration};

pub struct BitcoinCollector {
    producer: FutureProducer,
    client: Client,
    interval_secs: u64,
}

impl BitcoinCollector {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("client.id", "bitcoin-collector")
            .create()?;

        Ok(Self {
            producer,
            client: Client::new(),
            interval_secs: 30,
        })
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(self.interval_secs));

        loop {
            interval.tick().await;

            // Fetch market data
            let market_data = self.fetch_market_data().await?;

            // Send to Kafka
            self.send_to_kafka(&market_data).await?;

            tracing::info!("Sent Bitcoin market data: ${}", market_data.price);
        }
    }

    async fn fetch_market_data(&self) -> Result<BitcoinMarketData, Box<dyn std::error::Error>> {
        let response = self.client
            .get("https://api.blockchain.com/v3/exchange/tickers/BTC-USD")
            .send()
            .await?;

        let data: Value = response.json().await?;

        Ok(BitcoinMarketData {
            timestamp: chrono::Utc::now(),
            price: data["last_trade_price"].as_f64().unwrap_or(0.0),
            volume_24h: data["volume_24h"].as_f64().unwrap_or(0.0),
            market_cap: data["market_cap"].as_f64().unwrap_or(0.0),
            price_change_24h: data["price_24h_change"].as_f64().unwrap_or(0.0),
            // ... other fields
        })
    }

    async fn send_to_kafka(&self, data: &BitcoinMarketData) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string(data)?;

        self.producer
            .send(
                FutureRecord::to("bitcoin-market")
                    .payload(json.as_bytes())
                    .key("bitcoin"),
                Duration::from_secs(5),
            )
            .await?;

        Ok(())
    }
}

#[derive(serde::Serialize)]
struct BitcoinMarketData {
    timestamp: chrono::DateTime<chrono::Utc>,
    price: f64,
    volume_24h: f64,
    market_cap: f64,
    price_change_24h: f64,
    // Add more fields as needed
}
```

## 2. Crypto News Collector

### API Source: cryptopanic.com

CryptoPanic provides free APIs for crypto news:

- **News API**: `https://cryptopanic.com/api/v1/posts/`
- **Public API Key**: Free tier available
- **Rate Limits**: 30 requests per minute

### Implementation Strategy

Create a Rust service that:

1. **Polls news API** every few minutes
2. **Processes sentiment** using basic text analysis
3. **Sends to Kafka** topic `crypto-news`

### Example Implementation

```rust
// crates/rde-collectors/src/news_collector.rs
use reqwest::Client;
use serde_json::Value;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::{interval, Duration};

pub struct NewsCollector {
    producer: FutureProducer,
    client: Client,
    api_key: String,
    interval_secs: u64,
}

impl NewsCollector {
    pub async fn new(api_key: String) -> Result<Self, Box<dyn std::error::Error>> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("client.id", "news-collector")
            .create()?;

        Ok(Self {
            producer,
            client: Client::new(),
            api_key,
            interval_secs: 300, // 5 minutes
        })
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut interval = interval(Duration::from_secs(self.interval_secs));

        loop {
            interval.tick().await;

            // Fetch news
            let news_items = self.fetch_news().await?;

            // Process and send each news item
            for news in news_items {
                self.send_to_kafka(&news).await?;
            }

            tracing::info!("Processed {} news items", news_items.len());
        }
    }

    async fn fetch_news(&self) -> Result<Vec<CryptoNews>, Box<dyn std::error::Error>> {
        let response = self.client
            .get("https://cryptopanic.com/api/v1/posts/")
            .query(&[("auth_token", &self.api_key), ("currencies", &"BTC".to_string())])
            .send()
            .await?;

        let data: Value = response.json().await?;

        let mut news_items = Vec::new();

        if let Some(results) = data["results"].as_array() {
            for result in results {
                let news = CryptoNews {
                    id: result["id"].as_str().unwrap_or("").to_string(),
                    title: result["title"].as_str().unwrap_or("").to_string(),
                    content: result["text"].as_str().unwrap_or("").to_string(),
                    source: result["source"]["title"].as_str().unwrap_or("").to_string(),
                    published_at: chrono::Utc::now(), // Parse from API response
                    sentiment_score: self.calculate_sentiment(result["title"].as_str().unwrap_or("")),
                    keywords: self.extract_keywords(result["title"].as_str().unwrap_or("")),
                    // ... other fields
                };
                news_items.push(news);
            }
        }

        Ok(news_items)
    }

    fn calculate_sentiment(&self, text: &str) -> f64 {
        // Simple sentiment analysis
        let positive_words = ["bull", "bullish", "surge", "rally", "gain", "up", "positive"];
        let negative_words = ["bear", "bearish", "crash", "drop", "fall", "down", "negative"];

        let text_lower = text.to_lowercase();
        let positive_count = positive_words.iter().filter(|word| text_lower.contains(word)).count();
        let negative_count = negative_words.iter().filter(|word| text_lower.contains(word)).count();

        (positive_count as f64 - negative_count as f64) / (positive_count + negative_count + 1) as f64
    }

    fn extract_keywords(&self, text: &str) -> Vec<String> {
        // Simple keyword extraction
        let keywords = ["bitcoin", "btc", "crypto", "blockchain", "defi", "nft"];
        keywords
            .iter()
            .filter(|keyword| text.to_lowercase().contains(keyword))
            .map(|s| s.to_string())
            .collect()
    }

    async fn send_to_kafka(&self, news: &CryptoNews) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string(news)?;

        self.producer
            .send(
                FutureRecord::to("crypto-news")
                    .payload(json.as_bytes())
                    .key(&news.id),
                Duration::from_secs(5),
            )
            .await?;

        Ok(())
    }
}

#[derive(serde::Serialize)]
struct CryptoNews {
    id: String,
    title: String,
    content: String,
    source: String,
    published_at: chrono::DateTime<chrono::Utc>,
    sentiment_score: f64,
    keywords: Vec<String>,
    // Add more fields as needed
}
```

## 3. Integration with RDE Pipeline

### Configuration

Add collector configurations to your pipeline:

```yaml
# examples/crypto-pipeline.yml
collectors:
  bitcoin:
    enabled: true
    interval_secs: 30
    api_endpoint: "https://api.blockchain.com/v3/exchange/tickers/BTC-USD"

  news:
    enabled: true
    interval_secs: 300
    api_endpoint: "https://cryptopanic.com/api/v1/posts/"
    api_key: "${CRYPTOPANIC_API_KEY}"
```

### Running Collectors

Create startup scripts:

```bash
# scripts/start-bitcoin-collector.sh
#!/bin/bash
cargo run --bin bitcoin-collector -- \
    --interval 30 \
    --kafka-brokers localhost:9092 \
    --topic bitcoin-market
```

```bash
# scripts/start-news-collector.sh
#!/bin/bash
export CRYPTOPANIC_API_KEY="your-api-key-here"

cargo run --bin news-collector -- \
    --interval 300 \
    --kafka-brokers localhost:9092 \
    --topic crypto-news \
    --api-key "$CRYPTOPANIC_API_KEY"
```

## 4. Data Quality & Monitoring

### Schema Validation

Ensure data quality by validating incoming data:

```rust
use serde_json::Value;
use validator::{Validate, ValidationError};

#[derive(Validate)]
struct BitcoinMarketData {
    #[validate(range(min = 0.0))]
    price: f64,
    #[validate(range(min = 0.0))]
    volume_24h: f64,
    #[validate(range(min = 0.0))]
    market_cap: f64,
    // ... other validations
}

impl BitcoinCollector {
    async fn validate_and_send(&self, data: &BitcoinMarketData) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(errors) = data.validate() {
            tracing::warn!("Invalid data: {:?}", errors);
            return Ok(()); // Skip invalid data
        }

        self.send_to_kafka(data).await
    }
}
```

### Error Handling

Implement robust error handling:

```rust
use tokio::time::{sleep, Duration};

impl BitcoinCollector {
    async fn run_with_retry(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            match self.run().await {
                Ok(_) => break,
                Err(e) => {
                    tracing::error!("Collector error: {}", e);
                    tracing::info!("Retrying in 60 seconds...");
                    sleep(Duration::from_secs(60)).await;
                }
            }
        }
        Ok(())
    }
}
```

### Monitoring

Add metrics and monitoring:

```rust
use metrics::{counter, gauge};

impl BitcoinCollector {
    async fn record_metrics(&self, data: &BitcoinMarketData) {
        counter!("bitcoin.price_updates", 1);
        gauge!("bitcoin.current_price", data.price);
        gauge!("bitcoin.volume_24h", data.volume_24h);
    }
}
```

## 5. Production Considerations

### Rate Limiting

Respect API rate limits:

```rust
use tokio::time::sleep;

impl BitcoinCollector {
    async fn fetch_with_rate_limit(&self) -> Result<BitcoinMarketData, Box<dyn std::error::Error>> {
        // Add delay to respect rate limits
        sleep(Duration::from_millis(100)).await;

        self.fetch_market_data().await
    }
}
```

### Configuration Management

Use environment variables for configuration:

```rust
use std::env;

impl BitcoinCollector {
    pub async fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let interval_secs = env::var("BITCOIN_COLLECTOR_INTERVAL")
            .unwrap_or_else(|_| "30".to_string())
            .parse()?;

        let api_endpoint = env::var("BITCOIN_API_ENDPOINT")
            .unwrap_or_else(|_| "https://api.blockchain.com/v3/exchange/tickers/BTC-USD".to_string());

        // ... rest of implementation
    }
}
```

### Health Checks

Add health check endpoints:

```rust
use axum::{Router, routing::get};

async fn health_check() -> &'static str {
    "OK"
}

pub async fn start_health_server() {
    let app = Router::new().route("/health", get(health_check));

    axum::Server::bind(&"0.0.0.0:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
```

## Next Steps

1. **Implement the collectors** using the examples above
2. **Add proper error handling** and retry logic
3. **Set up monitoring** and alerting
4. **Test with real APIs** and validate data quality
5. **Deploy to production** with proper configuration

The RDE pipeline is ready to process the data once the collectors are implemented!

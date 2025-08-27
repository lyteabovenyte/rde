#![allow(unused)]

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn, error};

/// Bitcoin market data collector
/// 
/// Fetches real-time Bitcoin price data from blockchain.info API
/// and sends it to Kafka for further processing.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka broker address
    #[arg(long, default_value = "localhost:9092")]
    kafka_broker: String,
    
    /// Kafka topic for market data
    #[arg(long, default_value = "bitcoin-market")]
    kafka_topic: String,
    
    /// Collection interval in seconds
    #[arg(long, default_value = "30")]
    interval: u64,
    
    /// Target currencies to collect (comma-separated)
    #[arg(long, default_value = "USD,EUR,GBP,JPY,CNY,CAD,AUD,CHF")]
    currencies: String,
}

/// Bitcoin price data for a specific currency
#[derive(Debug, Serialize, Deserialize)]
struct BitcoinPriceData {
    /// Currency symbol (e.g., "USD", "EUR")
    currency: String,
    /// Current price
    price: f64,
    /// Buy price
    buy_price: f64,
    /// Sell price
    sell_price: f64,
    /// 15-minute price
    price_15m: f64,
    /// Timestamp when data was collected
    timestamp: DateTime<Utc>,
}

/// Raw response from blockchain.info API
#[derive(Debug, Deserialize)]
struct BlockchainTickerResponse {
    #[serde(flatten)]
    currencies: std::collections::HashMap<String, CurrencyData>,
}

/// Currency data from blockchain.info API
#[derive(Debug, Deserialize)]
struct CurrencyData {
    #[serde(rename = "15m")]
    price_15m: f64,
    buy: f64,
    last: f64,
    sell: f64,
    symbol: String,
}

/// Market data collector
struct MarketDataCollector {
    http_client: Client,
    kafka_producer: FutureProducer,
    kafka_topic: String,
    target_currencies: Vec<String>,
}

impl MarketDataCollector {
    /// Create a new market data collector
    fn new(
        kafka_broker: String,
        kafka_topic: String,
        target_currencies: Vec<String>,
    ) -> Result<Self> {
        // Create HTTP client
        let http_client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        // Create Kafka producer
        let kafka_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_broker)
            .set("message.timeout.ms", "5000")
            .set("delivery.timeout.ms", "10000")
            .set("request.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            http_client,
            kafka_producer,
            kafka_topic,
            target_currencies,
        })
    }

    /// Fetch Bitcoin market data from blockchain.info API
    async fn fetch_market_data(&self) -> Result<Vec<BitcoinPriceData>> {
        let ticker_url = "https://api.blockchain.info/ticker";
        
        info!("Fetching Bitcoin market data from {}", ticker_url);
        
        let response = self.http_client.get(ticker_url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "API request failed with status: {}",
                response.status()
            ));
        }

        let ticker_data: BlockchainTickerResponse = response.json().await?;
        let timestamp = Utc::now();
        
        let mut market_data = Vec::new();

        for currency in &self.target_currencies {
            if let Some(currency_data) = ticker_data.currencies.get(currency) {
                let price_data = BitcoinPriceData {
                    currency: currency.clone(),
                    price: currency_data.last,
                    buy_price: currency_data.buy,
                    sell_price: currency_data.sell,
                    price_15m: currency_data.price_15m,
                    timestamp,
                };
                
                market_data.push(price_data);
                info!("Collected data for {}: ${:.2}", currency, currency_data.last);
            } else {
                warn!("Currency {} not found in API response", currency);
            }
        }

        Ok(market_data)
    }

    /// Send market data to Kafka
    async fn send_to_kafka(&self, market_data: &[BitcoinPriceData]) -> Result<()> {
        for data in market_data {
            let json_data = serde_json::to_string(data)?;
            
            let record = FutureRecord::to(&self.kafka_topic)
                .key(&data.currency)
                .payload(json_data.as_bytes());

            match self.kafka_producer.send(record, Duration::from_secs(5)).await {
                Ok((partition, offset)) => {
                    info!(
                        "Sent {} data to Kafka: partition={}, offset={}",
                        data.currency, partition, offset
                    );
                }
                Err((e, _)) => {
                    error!("Failed to send {} data to Kafka: {}", data.currency, e);
                }
            }
        }

        Ok(())
    }

    /// Run the collector in a loop
    async fn run(&self, interval: u64) -> Result<()> {
        info!("Starting market data collector with {}s interval", interval);
        
        loop {
            match self.fetch_market_data().await {
                Ok(market_data) => {
                    if let Err(e) = self.send_to_kafka(&market_data).await {
                        error!("Failed to send data to Kafka: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to fetch market data: {}", e);
                }
            }

            tokio::time::sleep(Duration::from_secs(interval)).await;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    
    // Parse target currencies
    let target_currencies: Vec<String> = args
        .currencies
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .collect();

    info!("Starting Bitcoin market data collector");
    info!("Target currencies: {:?}", target_currencies);
    info!("Kafka broker: {}", args.kafka_broker);
    info!("Kafka topic: {}", args.kafka_topic);
    info!("Collection interval: {}s", args.interval);

    // Create and run the collector
    let collector = MarketDataCollector::new(
        args.kafka_broker,
        args.kafka_topic,
        target_currencies,
    )?;

    collector.run(args.interval).await?;

    Ok(())
}

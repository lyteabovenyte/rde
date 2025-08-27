#![allow(unused)]

use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Testing Bitcoin API call...");
    
    // Create HTTP client
    let http_client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // Fetch ticker data from blockchain.info API
    let ticker_url = "https://api.blockchain.info/ticker";
    let ticker_response = http_client.get(ticker_url).send().await?;
    
    if !ticker_response.status().is_success() {
        return Err(anyhow::anyhow!("Ticker API returned status: {}", ticker_response.status()));
    }
    
    let ticker_data: Value = ticker_response.json().await?;
    println!("Successfully fetched ticker data!");
    
    // Print the full response structure for analysis
    println!("Full response structure:");
    println!("{}", serde_json::to_string_pretty(&ticker_data).unwrap());

    // Define the currencies we want to collect (major ones)
    let target_currencies = ["USD", "EUR", "GBP", "JPY", "CNY", "CAD", "AUD", "CHF"];
    
    // Find the best available currency (prioritize USD, then EUR, etc.)
    let mut selected_currency = None;
    let mut selected_data = None;
    
    for currency in &target_currencies {
        if let Some(currency_data) = ticker_data.get(currency) {
            if let Some(data_obj) = currency_data.as_object() {
                selected_currency = Some(currency.to_string());
                selected_data = Some(data_obj);
                break;
            }
        }
    }
    
    let (currency, btc_data) = match (selected_currency, selected_data) {
        (Some(c), Some(d)) => (c, d),
        _ => return Err(anyhow::anyhow!("No valid currency data found in ticker response"))
    };

    // Extract price data
    let price = btc_data["last"].as_f64().unwrap_or(0.0);
    let buy_price = btc_data["buy"].as_f64().unwrap_or(0.0);
    let sell_price = btc_data["sell"].as_f64().unwrap_or(0.0);
    let price_change_15m = btc_data["15m"].as_f64().unwrap_or(0.0);
    
    // Calculate price change percentage
    let price_change_24h = if price > 0.0 && price_change_15m > 0.0 {
        ((price_change_15m - price) / price) * 100.0
    } else {
        0.0
    };

    println!("=== Bitcoin Market Data ===");
    println!("Currency: {}", currency);
    println!("Price: ${:.2}", price);
    println!("Buy Price: ${:.2}", buy_price);
    println!("Sell Price: ${:.2}", sell_price);
    println!("15m Change: ${:.2}", price_change_15m);
    println!("24h Change: {:.2}%", price_change_24h);
    println!("==========================");

    // Show available currencies
    println!("\nAvailable currencies in response:");
    if let Some(obj) = ticker_data.as_object() {
        for (key, _) in obj.iter().take(10) {
            println!("  - {}", key);
        }
        if obj.len() > 10 {
            println!("  ... and {} more", obj.len() - 10);
        }
    }

    Ok(())
}

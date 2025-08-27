use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use std::fs;
use toml;

#[tokio::main]
async fn main() -> Result<()> {
    // Load API key from secrets file
    let secrets_content = fs::read_to_string("../secrets/api_keys.toml")?;
    let secrets: toml::Value = toml::from_str(&secrets_content)?;
    
    let api_key = secrets["cryptopanic"]["api_key"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("CryptoPanic API key not found in secrets file"))?;
    
    // Create HTTP client
    let http_client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    // Fetch posts with metadata
    let posts_url = format!("https://cryptopanic.com/api/v1/posts/?auth_token={}&metadata=true&filter=hot", api_key);
    let posts_response = http_client.get(&posts_url).send().await?;
    
    if !posts_response.status().is_success() {
        let status = posts_response.status();
        let error_text = posts_response.text().await?;
        println!("Error: {} - {}", status, error_text);
        return Err(anyhow::anyhow!("Posts API request failed"));
    }
    
    let posts_data: Value = posts_response.json().await?;

    // Display posts
    if let Some(posts) = posts_data["results"].as_array() {
        println!("📰 CryptoPanic News Feed ({} articles):", posts.len());
        println!("{}", "=".repeat(60));
        

        
        for (i, post) in posts.iter().take(5).enumerate() {
            println!("\n{}. {}", i + 1, post["title"].as_str().unwrap_or("No title"));
            
            if let Some(published_at) = post["published_at"].as_str() {
                println!("   📅 {}", published_at);
            }
            
            if let Some(description) = post["description"].as_str() {
                if !description.is_empty() {
                    println!("   📝 {}", description);
                }
            }
            
            if let Some(domain) = post["domain"].as_str() {
                println!("   🔗 Source: {}", domain);
            }
        }
    }

    Ok(())
}

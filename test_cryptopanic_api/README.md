# CryptoPanic API Test

This binary tests the CryptoPanic API integration for the RDE (Rust Data Engineering) project.

## Purpose

The `test_cryptopanic_api` binary validates that:

- The CryptoPanic API key is properly configured
- The API endpoints are accessible and returning data
- The response format matches expected structure
- Real-time cryptocurrency news data can be fetched

## Features

- **Posts Endpoint**: Tests the main posts endpoint to fetch cryptocurrency news
- **Currencies Endpoint**: Tests the currencies endpoint (may require premium tier)
- **Tags Endpoint**: Tests the tags endpoint (may require premium tier)
- **API Key Management**: Automatically loads API key from `../secrets/api_keys.toml`

## Usage

```bash
cd test_cryptopanic_api
cargo run
```

## Configuration

The test automatically reads the CryptoPanic API key from `../secrets/api_keys.toml`:

```toml
[cryptopanic]
api_key = "your_api_key_here"
```

## API Key Setup

1. Visit [CryptoPanic Developers](https://cryptopanic.com/developers/api/)
2. Register for a free account
3. Generate an API key
4. Add the key to `secrets/api_keys.toml`

## Output

The test displays:

- API connection status
- Sample news posts with titles, descriptions, and publication dates
- Article summaries/excerpts (with `metadata=true` parameter)
- Source domain and publication metadata
- Currency information (if available)
- Tag information (if available)
- Error messages for any failed endpoints

## Content Access

With the current API key and `metadata=true` parameter, you can access:

- ✅ **Titles**: Full article headlines
- ✅ **Descriptions**: Article summaries and excerpts
- ✅ **Publication Dates**: When articles were published
- ✅ **Source Domains**: News source information
- ✅ **URLs**: Direct links to articles
- ✅ **Currencies**: Related cryptocurrency tags
- ✅ **Votes**: Community voting data

**Note**: For full article content, a PRO subscription would be required.

## Dependencies

- `anyhow`: Error handling
- `reqwest`: HTTP client
- `serde_json`: JSON parsing
- `tokio`: Async runtime
- `toml`: Configuration file parsing

## Notes

- The free tier of CryptoPanic API may have limited access to certain endpoints
- Some endpoints (currencies, tags) may return 404 errors with the free tier
- The posts endpoint is the primary endpoint and should work with any valid API key

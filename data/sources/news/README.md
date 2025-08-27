# Crypto News Data Source

This directory contains configuration and documentation for crypto news data collection.

## Overview

Crypto news data is collected from cryptopanic.com APIs and includes:

- News articles and headlines
- Source information
- Sentiment analysis
- Keyword extraction
- Publication timestamps

## Data Schema

```json
{
  "id": "12345",
  "title": "Bitcoin Surges Past $45,000 as Institutional Adoption Grows",
  "content": "Bitcoin has reached new heights as major institutions...",
  "source": "CoinDesk",
  "published_at": "2024-01-15T10:30:00Z",
  "sentiment_score": 0.75,
  "keywords": ["bitcoin", "btc", "crypto"],
  "sentiment_category": "positive",
  "source_category": "major",
  "ingestion_time": "2024-01-15T10:30:05Z",
  "partition_date": "2024-01-15",
  "partition_hour": 10
}
```

## API Sources

### cryptopanic.com APIs

- **News API**: `https://cryptopanic.com/api/v1/posts/`
- **Public API Key**: Free tier available
- **Rate Limits**: 30 requests per minute

### API Parameters

- `auth_token`: Your API key
- `currencies`: Filter by currency (e.g., "BTC")
- `filter`: Filter by type (hot, rising, important)
- `public`: Include public posts

## Sentiment Analysis

The news collector performs basic sentiment analysis using:

- **Positive keywords**: bull, bullish, surge, rally, gain, up, positive
- **Negative keywords**: bear, bearish, crash, drop, fall, down, negative
- **Scoring**: Range from -1.0 (very negative) to +1.0 (very positive)

## Configuration

Create configuration files in this directory for:

- API endpoints and authentication
- Collection intervals
- Sentiment analysis rules
- Keyword extraction patterns
- Quality thresholds

## Implementation Status

- [ ] API collector implementation
- [ ] Sentiment analysis
- [ ] Keyword extraction
- [ ] Data validation
- [ ] Error handling
- [ ] Monitoring and alerting
- [ ] Production deployment

## Next Steps

1. Implement the news collector service
2. Add sentiment analysis and keyword extraction
3. Set up data quality validation
4. Configure monitoring and alerting
5. Deploy to production environment

See `docs/api-integration.md` for detailed implementation guidance.

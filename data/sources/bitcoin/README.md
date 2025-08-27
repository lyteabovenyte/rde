# Bitcoin Market Data Source

This directory contains configuration and documentation for Bitcoin market data collection.

## Overview

Bitcoin market data is collected from blockchain.com APIs and includes:

- Real-time price data
- 24-hour volume and market cap
- Price change percentages
- Market sentiment indicators

## Data Schema

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "price": 43250.5,
  "volume_24h": 28475000000,
  "market_cap": 847500000000,
  "price_change_24h": 2.5,
  "market_sentiment": "bullish",
  "ingestion_time": "2024-01-15T10:30:05Z",
  "partition_date": "2024-01-15",
  "partition_hour": 10
}
```

## API Sources

### blockchain.com APIs

- **Ticker API**: `https://api.blockchain.com/v3/exchange/tickers/BTC-USD`
- **Price API**: `https://api.blockchain.com/v3/exchange/rates`
- **Stats API**: `https://api.blockchain.com/v3/stats`

### Rate Limits

- Free tier: 30 requests per minute
- No authentication required for basic endpoints

## Configuration

Create configuration files in this directory for:

- API endpoints and parameters
- Collection intervals
- Data transformation rules
- Quality thresholds

## Implementation Status

- [ ] API collector implementation
- [ ] Data validation
- [ ] Error handling
- [ ] Monitoring and alerting
- [ ] Production deployment

## Next Steps

1. Implement the Bitcoin collector service
2. Add data quality validation
3. Set up monitoring and alerting
4. Deploy to production environment

See `docs/api-integration.md` for detailed implementation guidance.

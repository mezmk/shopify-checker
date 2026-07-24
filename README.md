# Shopify Checker Server

FastAPI server for Shopify card checking with load balancing.

## Deploy on Railway

1. Create new Railway project
2. Import this repo
3. Deploy

## API Endpoints

### POST /check
Check cards with single API.
```json
{
    "cards": ["card1|mm|yyyy|cvv"],
    "site": "https://example.myshopify.com",
    "proxy": "ip:port:user:pass"
}
```

### POST /check_batch
Check cards in parallel batches.
```json
{
    "cards": ["card1|mm|yyyy|cvv"],
    "site": "https://example.myshopify.com",
    "proxy": "ip:port:user:pass",
    "batch_size": 50
}
```

### GET /health
Health check.

## Environment Variables

- `PORT` - Server port (default: 8000)

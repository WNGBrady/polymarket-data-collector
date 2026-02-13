# Polymarket Call of Duty Data Collector

A Python program to collect and store data from Polymarket's API for Call of Duty prediction markets, including timestamped price data for correlation with match events.

## Features

- **Market Discovery**: Automatically finds COD-related markets on Polymarket
- **Historical Data Collection**: Backfills price history and trade data
- **Real-time Streaming**: WebSocket-based live price updates
- **SQLite Storage**: Persistent local database for all collected data

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Discover Markets
Find and store Call of Duty markets:
```bash
python main.py --discover
```

### Collect Historical Data
Backfill price and trade history for stored markets:
```bash
python main.py --historical
```

### Real-time Collection
Start live price streaming via WebSocket:
```bash
python main.py --realtime
```
Press `Ctrl+C` to stop.

### View Statistics
Show database statistics:
```bash
python main.py --stats
```

### List Markets
Show all stored markets:
```bash
python main.py --list
```

### Run All
Discover markets and collect historical data:
```bash
python main.py --all
```

## Database Schema

The SQLite database (`data/polymarket_cod.db`) contains:

- **markets**: Market metadata (ID, question, outcomes, token IDs)
- **price_history**: Historical prices with Unix timestamps
- **trades**: Individual trade records with timestamps
- **realtime_prices**: Live price updates with millisecond timestamps

## API Endpoints Used

| API | Base URL | Purpose |
|-----|----------|---------|
| Gamma API | `https://gamma-api.polymarket.com` | Market discovery, metadata |
| CLOB API | `https://clob.polymarket.com` | Price history |
| Data API | `https://data-api.polymarket.com` | Trade history |
| WebSocket | `wss://ws-subscriptions-clob.polymarket.com` | Real-time prices |

## Rate Limits

The collector respects Polymarket's rate limits:
- Gamma API (markets): 300 req/10s
- Gamma API (events): 500 req/10s
- CLOB API (prices): 1500 req/10s
- Data API (trades): 200 req/10s

Exponential backoff is used when rate limited.

## Project Structure

```
PolyMarket_COD/
├── src/
│   ├── __init__.py
│   ├── config.py              # API URLs, rate limits
│   ├── database.py            # SQLite setup and queries
│   ├── market_discovery.py    # Market search and storage
│   ├── historical_collector.py # Price/trade backfill
│   ├── realtime_collector.py  # WebSocket streaming
│   └── utils.py               # Rate limiting, error handling
├── data/
│   └── polymarket_cod.db      # SQLite database
├── main.py                    # CLI entry point
├── requirements.txt
└── README.md
```

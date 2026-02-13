# Polymarket COD Data Collector - Usage Guide

A tool for collecting Call of Duty League (CDL) prediction market data from Polymarket, including historical prices, trades, real-time streaming, and orderbook snapshots.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Commands](#cli-commands)
- [Data Collection Workflows](#data-collection-workflows)
- [Reading the Data](#reading-the-data)
- [Database Schema](#database-schema)
- [Example Analysis Scripts](#example-analysis-scripts)

---

## Installation

### Requirements

- Python 3.8+
- Required packages:

```bash
pip install requests websockets
```

### Setup

```bash
# Clone or download the project
cd PolyMarket_COD

# Install dependencies
pip install -r requirements.txt
```

---

## Quick Start

```bash
# 1. Discover COD markets
python main.py --discover

# 2. Collect historical data (prices + trades)
python main.py --historical

# 3. View what you collected
python main.py --stats
```

---

## CLI Commands

### Discovery Commands

#### `--discover`
Find and store Call of Duty markets from Polymarket.

```bash
python main.py --discover
python main.py --discover --include-closed  # Include resolved markets
```

This searches using:
- General terms: "call of duty", "cdl", etc.
- All 12 CDL team names
- Event types: "cdl major", "cdl stage", "cdl qualifier"
- Tag-based filtering for esports/gaming categories

#### `--discover-tags`
Show available tags from the Polymarket API (useful for debugging).

```bash
python main.py --discover-tags
```

### Data Collection Commands

#### `--historical`
Collect historical price and trade data for all discovered markets.

```bash
python main.py --historical
```

This fetches:
- Price history (hourly intervals, last 30 days)
- Trade history (up to 10,000 trades per request)

#### `--orderbook`
Collect current orderbook snapshots for all markets.

```bash
python main.py --orderbook
```

Captures:
- Best bid/ask prices and sizes
- Bid-ask spread and mid price
- Top 5 price levels on each side

#### `--realtime`
Stream live price updates via WebSocket.

```bash
python main.py --realtime
python main.py --realtime --with-orderbook  # Also poll orderbooks every 60s
```

Press `Ctrl+C` to stop.

#### `--continuous`
Tournament mode - runs continuously with all features enabled.

```bash
python main.py --continuous
```

This mode:
- Runs market discovery every 30 minutes
- Streams real-time prices via WebSocket
- Polls orderbooks every 60 seconds
- Automatically backfills historical data for new markets
- Graceful shutdown with `Ctrl+C`

**Recommended for CDL tournament weekends (Thursday-Sunday).**

### Utility Commands

#### `--all`
Run discovery + historical collection in sequence.

```bash
python main.py --all
```

#### `--stats`
Show database statistics.

```bash
python main.py --stats
```

Output:
```
Database Statistics:
  - Markets: 33
  - Price history records: 1250
  - Trade records: 8543
  - Real-time price records: 0
  - Orderbook snapshots: 165
```

#### `--list`
List all stored markets with their IDs.

```bash
python main.py --list
```

---

## Data Collection Workflows

### One-Time Historical Collection

```bash
# Full historical backfill
python main.py --discover
python main.py --historical
python main.py --orderbook
python main.py --stats
```

### Live Tournament Monitoring

```bash
# Option 1: Continuous mode (recommended)
python main.py --continuous

# Option 2: Manual realtime with orderbooks
python main.py --discover
python main.py --realtime --with-orderbook
```

### Daily Update Routine

```bash
# Run daily to catch new markets and update trades
python main.py --all
python main.py --orderbook
```

---

## Reading the Data

The data is stored in a SQLite database at `data/polymarket_cod.db`.

### Using Python + Pandas

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('data/polymarket_cod.db')

# Load markets
markets = pd.read_sql_query("SELECT * FROM markets", conn)
print(f"Found {len(markets)} markets")
print(markets[['market_id', 'question']].head())

# Load price history
prices = pd.read_sql_query("""
    SELECT p.*, m.question
    FROM price_history p
    JOIN markets m ON p.market_id = m.market_id
    ORDER BY p.timestamp
""", conn)
print(f"Found {len(prices)} price records")

# Load trades
trades = pd.read_sql_query("""
    SELECT t.*, m.question
    FROM trades t
    JOIN markets m ON t.market_id = m.market_id
    ORDER BY t.timestamp DESC
""", conn)
print(f"Found {len(trades)} trades")

# Load orderbook snapshots
orderbooks = pd.read_sql_query("""
    SELECT * FROM orderbook_snapshots
    ORDER BY timestamp DESC
""", conn)
print(f"Found {len(orderbooks)} orderbook snapshots")

# Load realtime prices
realtime = pd.read_sql_query("""
    SELECT * FROM realtime_prices
    ORDER BY timestamp DESC
""", conn)
print(f"Found {len(realtime)} realtime price records")

conn.close()
```

### Using SQLite CLI

```bash
# Open database
sqlite3 data/polymarket_cod.db

# Show tables
.tables

# Show table schema
.schema markets
.schema trades

# Count records
SELECT COUNT(*) FROM markets;
SELECT COUNT(*) FROM trades;
SELECT COUNT(*) FROM price_history;
SELECT COUNT(*) FROM orderbook_snapshots;

# List markets
SELECT market_id, question FROM markets;

# Recent trades
SELECT * FROM trades ORDER BY timestamp DESC LIMIT 10;

# Exit
.quit
```

### Using DB Browser for SQLite

1. Download [DB Browser for SQLite](https://sqlitebrowser.org/)
2. Open `data/polymarket_cod.db`
3. Browse tables visually

---

## Database Schema

### `markets`
Discovered prediction markets.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| market_id | TEXT | Unique Polymarket market ID |
| condition_id | TEXT | Condition ID for trade queries |
| clob_token_id_yes | TEXT | CLOB token ID for YES outcome |
| clob_token_id_no | TEXT | CLOB token ID for NO outcome |
| question | TEXT | Market question (e.g., "Will OpTic Texas win?") |
| outcomes | TEXT | JSON array of outcome names |
| start_date | TEXT | Market start date |
| end_date | TEXT | Market end date |
| created_at | TIMESTAMP | When record was created |

### `price_history`
Historical price data (hourly intervals).

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| market_id | TEXT | Foreign key to markets |
| timestamp | INTEGER | Unix timestamp (seconds) |
| price | REAL | Price (0.0 - 1.0, represents probability) |

### `trades`
Individual trade records.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| market_id | TEXT | Foreign key to markets |
| trade_id | TEXT | Unique trade ID |
| timestamp | INTEGER | Unix timestamp |
| price | REAL | Trade price (0.0 - 1.0) |
| size | REAL | Trade size (USDC amount) |
| side | TEXT | BUY or SELL |
| outcome | TEXT | Which outcome was traded |

### `realtime_prices`
WebSocket streaming price updates.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| market_id | TEXT | Token ID (from WebSocket) |
| timestamp | INTEGER | Unix timestamp (milliseconds) |
| bid | REAL | Best bid price |
| ask | REAL | Best ask price |
| last_price | REAL | Last trade price |

### `orderbook_snapshots`
Point-in-time orderbook data.

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Auto-increment primary key |
| market_id | TEXT | Foreign key to markets |
| token_id | TEXT | CLOB token ID |
| timestamp | INTEGER | Unix timestamp (milliseconds) |
| best_bid_price | REAL | Highest bid price |
| best_bid_size | REAL | Size at best bid |
| best_ask_price | REAL | Lowest ask price |
| best_ask_size | REAL | Size at best ask |
| spread | REAL | Bid-ask spread |
| mid_price | REAL | (bid + ask) / 2 |
| bid_depth | TEXT | JSON array of top 5 bid levels |
| ask_depth | TEXT | JSON array of top 5 ask levels |

---

## Example Analysis Scripts

### Plot Price History for a Market

```python
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect('data/polymarket_cod.db')

# Get a specific market's prices
market_id = "YOUR_MARKET_ID"  # Replace with actual ID
query = """
    SELECT timestamp, price
    FROM price_history
    WHERE market_id = ?
    ORDER BY timestamp
"""
df = pd.read_sql_query(query, conn, params=[market_id])
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

plt.figure(figsize=(12, 6))
plt.plot(df['datetime'], df['price'])
plt.title('Price History')
plt.xlabel('Time')
plt.ylabel('Price (Probability)')
plt.ylim(0, 1)
plt.grid(True)
plt.show()

conn.close()
```

### Analyze Trade Volume

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/polymarket_cod.db')

# Trade volume by market
query = """
    SELECT
        m.question,
        COUNT(t.id) as trade_count,
        SUM(t.size) as total_volume,
        AVG(t.price) as avg_price
    FROM trades t
    JOIN markets m ON t.market_id = m.market_id
    GROUP BY t.market_id
    ORDER BY total_volume DESC
"""
df = pd.read_sql_query(query, conn)
print(df)

conn.close()
```

### Calculate Bid-Ask Spread Over Time

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/polymarket_cod.db')

query = """
    SELECT
        timestamp,
        market_id,
        spread,
        mid_price
    FROM orderbook_snapshots
    ORDER BY timestamp
"""
df = pd.read_sql_query(query, conn)
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

# Average spread by market
avg_spread = df.groupby('market_id')['spread'].mean()
print("Average spread by market:")
print(avg_spread)

conn.close()
```

### Export to CSV

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/polymarket_cod.db')

# Export all tables
for table in ['markets', 'price_history', 'trades', 'realtime_prices', 'orderbook_snapshots']:
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(f'data/{table}.csv', index=False)
    print(f"Exported {table}: {len(df)} rows")

conn.close()
```

---

## Troubleshooting

### No markets found
```bash
# Check if discovery found any markets
python main.py --discover
python main.py --list
```

If no markets appear, there may not be active COD markets on Polymarket.

### Price history empty
The price history endpoint may return 400 errors for some markets. The collector uses the `interval` parameter and time bounds to maximize compatibility.

### WebSocket disconnects
The realtime collector automatically reconnects with exponential backoff. This is normal behavior.

### Database locked errors
Only run one instance of the collector at a time. Close any other connections to the database.

---

## Configuration

Edit `src/config.py` to customize:

```python
# Polling intervals
ORDERBOOK_POLL_INTERVAL = 60  # seconds
DISCOVERY_INTERVAL = 1800     # 30 minutes

# Orderbook depth
ORDERBOOK_DEPTH = 5  # price levels to store

# Add custom search terms
COD_SEARCH_TERMS = [...]
COD_VALIDATION_TERMS = [...]
```

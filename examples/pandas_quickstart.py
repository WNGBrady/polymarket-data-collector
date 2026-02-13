#!/usr/bin/env python3
"""
Quick start guide for analyzing Polymarket COD data with pandas.

This script demonstrates common data analysis patterns.
Copy and paste these snippets into your own scripts or Jupyter notebooks.

Usage:
    python examples/pandas_quickstart.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "polymarket_cod.db"


def load_all_data():
    """Load all tables into pandas DataFrames."""
    conn = sqlite3.connect(DB_PATH)

    data = {
        'markets': pd.read_sql_query("SELECT * FROM markets", conn),
        'price_history': pd.read_sql_query("SELECT * FROM price_history", conn),
        'trades': pd.read_sql_query("SELECT * FROM trades", conn),
        'realtime_prices': pd.read_sql_query("SELECT * FROM realtime_prices", conn),
        'orderbook_snapshots': pd.read_sql_query("SELECT * FROM orderbook_snapshots", conn),
    }

    conn.close()
    return data


def example_price_analysis():
    """Example: Analyze price history."""
    print("\n=== PRICE HISTORY ANALYSIS ===\n")

    conn = sqlite3.connect(DB_PATH)

    # Load price history with market info
    df = pd.read_sql_query("""
        SELECT
            p.market_id,
            p.timestamp,
            p.price,
            m.question
        FROM price_history p
        JOIN markets m ON p.market_id = m.market_id
    """, conn)

    conn.close()

    if df.empty:
        print("No price history data found.")
        return

    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

    # Summary statistics by market
    summary = df.groupby('market_id').agg({
        'price': ['count', 'mean', 'std', 'min', 'max'],
        'datetime': ['min', 'max']
    }).round(4)

    print("Price History Summary by Market:")
    print(summary.head(10))

    # Price volatility
    volatility = df.groupby('market_id')['price'].std().sort_values(ascending=False)
    print("\nMost Volatile Markets (by price std dev):")
    print(volatility.head(5))


def example_trade_analysis():
    """Example: Analyze trade data."""
    print("\n=== TRADE ANALYSIS ===\n")

    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query("""
        SELECT
            t.*,
            m.question
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
    """, conn)

    conn.close()

    if df.empty:
        print("No trade data found.")
        return

    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df['hour'] = df['datetime'].dt.hour
    df['day_of_week'] = df['datetime'].dt.day_name()

    # Basic stats
    print(f"Total trades: {len(df):,}")
    print(f"Total volume: ${df['size'].sum():,.2f}")
    print(f"Average trade size: ${df['size'].mean():,.2f}")

    # Volume by hour
    hourly_volume = df.groupby('hour')['size'].sum()
    print("\nVolume by Hour of Day:")
    print(hourly_volume)

    # Buy vs Sell
    side_volume = df.groupby('side')['size'].sum()
    print("\nVolume by Side:")
    print(side_volume)

    # Most active markets
    market_volume = df.groupby('question')['size'].sum().sort_values(ascending=False)
    print("\nTop 5 Markets by Volume:")
    print(market_volume.head(5))


def example_orderbook_analysis():
    """Example: Analyze orderbook snapshots."""
    print("\n=== ORDERBOOK ANALYSIS ===\n")

    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query("""
        SELECT
            o.*,
            m.question
        FROM orderbook_snapshots o
        JOIN markets m ON o.market_id = m.market_id
    """, conn)

    conn.close()

    if df.empty:
        print("No orderbook data found.")
        return

    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

    # Summary stats
    print(f"Total snapshots: {len(df):,}")
    print(f"Average spread: {df['spread'].mean():.4f}")
    print(f"Average mid price: {df['mid_price'].mean():.4f}")

    # Spread statistics by market
    spread_stats = df.groupby('market_id').agg({
        'spread': ['mean', 'std', 'min', 'max'],
        'mid_price': ['mean', 'std']
    }).round(4)

    print("\nSpread Statistics by Market:")
    print(spread_stats.head(10))

    # Markets with tightest spreads (most liquid)
    avg_spread = df.groupby('market_id')['spread'].mean().sort_values()
    print("\nMarkets with Tightest Average Spreads:")
    print(avg_spread.head(5))


def example_time_series():
    """Example: Time series analysis."""
    print("\n=== TIME SERIES ANALYSIS ===\n")

    conn = sqlite3.connect(DB_PATH)

    # Get price history for plotting
    df = pd.read_sql_query("""
        SELECT
            market_id,
            timestamp,
            price
        FROM price_history
        ORDER BY timestamp
    """, conn)

    conn.close()

    if df.empty:
        print("No price history data found.")
        return

    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

    # Resample to hourly
    for market_id in df['market_id'].unique()[:3]:  # First 3 markets
        market_df = df[df['market_id'] == market_id].set_index('datetime')
        hourly = market_df['price'].resample('1H').mean()

        print(f"\nMarket: {market_id[:40]}...")
        print(f"  Data points: {len(hourly)}")
        print(f"  Price range: {hourly.min():.3f} - {hourly.max():.3f}")
        print(f"  Price change: {hourly.iloc[-1] - hourly.iloc[0]:.3f}")


def example_join_analysis():
    """Example: Join multiple tables for analysis."""
    print("\n=== COMBINED ANALYSIS ===\n")

    conn = sqlite3.connect(DB_PATH)

    # Combine trades with orderbook data
    query = """
        SELECT
            t.market_id,
            m.question,
            COUNT(DISTINCT t.id) as trade_count,
            SUM(t.size) as total_volume,
            AVG(t.price) as avg_trade_price,
            AVG(o.spread) as avg_spread,
            AVG(o.mid_price) as avg_mid_price
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        LEFT JOIN orderbook_snapshots o ON t.market_id = o.market_id
        GROUP BY t.market_id
        ORDER BY total_volume DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("No data found.")
        return

    print("Market Activity Summary:")
    print(df[['question', 'trade_count', 'total_volume', 'avg_trade_price', 'avg_spread']].head(10).to_string())


def main():
    """Run all examples."""
    print("=" * 60)
    print("  PANDAS QUICKSTART GUIDE")
    print("=" * 60)

    # Check if database exists
    if not DB_PATH.exists():
        print(f"\nDatabase not found at: {DB_PATH}")
        print("Run 'python main.py --discover' first to collect data.")
        return

    # Load and display all data
    print("\nLoading data...")
    data = load_all_data()

    for name, df in data.items():
        print(f"  {name}: {len(df):,} rows")

    # Run examples
    example_price_analysis()
    example_trade_analysis()
    example_orderbook_analysis()
    example_time_series()
    example_join_analysis()

    print("\n" + "=" * 60)
    print("  COPY THESE SNIPPETS FOR YOUR OWN ANALYSIS")
    print("=" * 60)

    print("""
# Basic setup
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/polymarket_cod.db')

# Load a table
df = pd.read_sql_query("SELECT * FROM trades", conn)

# Join tables
df = pd.read_sql_query('''
    SELECT t.*, m.question
    FROM trades t
    JOIN markets m ON t.market_id = m.market_id
''', conn)

# Convert timestamps
df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

# Group and aggregate
summary = df.groupby('market_id').agg({
    'price': ['mean', 'std'],
    'size': 'sum'
})

# Time series resampling
df.set_index('datetime')['price'].resample('1H').mean()

# Export to CSV
df.to_csv('output.csv', index=False)

conn.close()
""")


if __name__ == "__main__":
    main()

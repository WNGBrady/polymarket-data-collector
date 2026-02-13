#!/usr/bin/env python3
"""
Example script for analyzing collected Polymarket COD data.

Usage:
    python examples/analyze_data.py
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "polymarket_cod.db"


def get_connection():
    """Get database connection."""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        print("Run 'python main.py --discover' first to collect data.")
        return None
    return sqlite3.connect(DB_PATH)


def print_section(title):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def show_summary():
    """Show database summary statistics."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("DATABASE SUMMARY")

    # Count records in each table
    tables = ['markets', 'price_history', 'trades', 'realtime_prices', 'orderbook_snapshots']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count:,} records")

    conn.close()


def list_markets():
    """List all discovered markets."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("DISCOVERED MARKETS")

    cursor.execute("""
        SELECT market_id, question, outcomes, start_date, end_date
        FROM markets
        ORDER BY created_at DESC
    """)

    rows = cursor.fetchall()
    for i, (market_id, question, outcomes, start_date, end_date) in enumerate(rows, 1):
        outcomes_list = json.loads(outcomes) if outcomes else []
        outcomes_str = " vs ".join(outcomes_list[:2]) if outcomes_list else "N/A"
        print(f"{i}. {question[:70]}...")
        print(f"   ID: {market_id[:40]}...")
        print(f"   Outcomes: {outcomes_str}")
        print(f"   Period: {start_date or 'N/A'} to {end_date or 'N/A'}")
        print()

    conn.close()


def show_trade_activity():
    """Show trade activity by market."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("TRADE ACTIVITY BY MARKET")

    cursor.execute("""
        SELECT
            m.question,
            COUNT(t.id) as trade_count,
            COALESCE(SUM(t.size), 0) as total_volume,
            COALESCE(AVG(t.price), 0) as avg_price,
            MIN(t.timestamp) as first_trade,
            MAX(t.timestamp) as last_trade
        FROM markets m
        LEFT JOIN trades t ON m.market_id = t.market_id
        GROUP BY m.market_id
        ORDER BY total_volume DESC
    """)

    rows = cursor.fetchall()
    for question, trade_count, volume, avg_price, first_ts, last_ts in rows:
        print(f"Market: {question[:60]}...")
        print(f"  Trades: {trade_count:,}")
        print(f"  Volume: ${volume:,.2f}")
        print(f"  Avg Price: {avg_price:.3f}")
        if first_ts and last_ts:
            first_dt = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d %H:%M")
            last_dt = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d %H:%M")
            print(f"  Time Range: {first_dt} to {last_dt}")
        print()

    conn.close()


def show_recent_trades(limit=20):
    """Show most recent trades."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section(f"RECENT TRADES (last {limit})")

    cursor.execute("""
        SELECT
            t.timestamp,
            t.price,
            t.size,
            t.side,
            t.outcome,
            m.question
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        ORDER BY t.timestamp DESC
        LIMIT ?
    """, (limit,))

    rows = cursor.fetchall()
    for ts, price, size, side, outcome, question in rows:
        dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{dt}] {side:4} {outcome or 'N/A':10} @ {price:.3f} (${size:.2f})")
        print(f"    {question[:50]}...")
        print()

    conn.close()


def show_price_history_sample():
    """Show sample price history data."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("PRICE HISTORY SAMPLE")

    # Get markets with price history
    cursor.execute("""
        SELECT DISTINCT m.market_id, m.question
        FROM price_history p
        JOIN markets m ON p.market_id = m.market_id
        LIMIT 5
    """)

    markets = cursor.fetchall()

    for market_id, question in markets:
        print(f"Market: {question[:60]}...")

        cursor.execute("""
            SELECT timestamp, price
            FROM price_history
            WHERE market_id = ?
            ORDER BY timestamp DESC
            LIMIT 5
        """, (market_id,))

        prices = cursor.fetchall()
        for ts, price in prices:
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
            print(f"  {dt}: {price:.3f}")
        print()

    conn.close()


def show_orderbook_snapshots():
    """Show recent orderbook snapshots."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("RECENT ORDERBOOK SNAPSHOTS")

    cursor.execute("""
        SELECT
            o.timestamp,
            o.best_bid_price,
            o.best_ask_price,
            o.spread,
            o.mid_price,
            m.question
        FROM orderbook_snapshots o
        JOIN markets m ON o.market_id = m.market_id
        ORDER BY o.timestamp DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    for ts, bid, ask, spread, mid, question in rows:
        dt = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{dt}] {question[:40]}...")
        print(f"  Bid: {bid:.4f}  Ask: {ask:.4f}  Spread: {spread:.4f}  Mid: {mid:.4f}")
        print()

    conn.close()


def show_realtime_activity():
    """Show realtime price activity summary."""
    conn = get_connection()
    if not conn:
        return

    cursor = conn.cursor()

    print_section("REALTIME PRICE ACTIVITY")

    cursor.execute("SELECT COUNT(*) FROM realtime_prices")
    count = cursor.fetchone()[0]

    if count == 0:
        print("No realtime price data collected yet.")
        print("Run 'python main.py --realtime' to stream live prices.")
        conn.close()
        return

    cursor.execute("""
        SELECT
            market_id,
            COUNT(*) as updates,
            MIN(timestamp) as first_update,
            MAX(timestamp) as last_update,
            AVG(last_price) as avg_price
        FROM realtime_prices
        GROUP BY market_id
        ORDER BY updates DESC
        LIMIT 10
    """)

    rows = cursor.fetchall()
    for market_id, updates, first_ts, last_ts, avg_price in rows:
        first_dt = datetime.fromtimestamp(first_ts / 1000).strftime("%Y-%m-%d %H:%M")
        last_dt = datetime.fromtimestamp(last_ts / 1000).strftime("%Y-%m-%d %H:%M")
        print(f"Token: {market_id[:40]}...")
        print(f"  Updates: {updates:,}")
        print(f"  Avg Price: {avg_price:.4f}")
        print(f"  Time Range: {first_dt} to {last_dt}")
        print()

    conn.close()


def export_to_csv():
    """Export all data to CSV files."""
    conn = get_connection()
    if not conn:
        return

    print_section("EXPORTING TO CSV")

    output_dir = Path(__file__).parent.parent / "data"

    tables = ['markets', 'price_history', 'trades', 'realtime_prices', 'orderbook_snapshots']

    for table in tables:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]

        csv_path = output_dir / f"{table}.csv"
        with open(csv_path, 'w') as f:
            f.write(','.join(columns) + '\n')
            for row in rows:
                values = [str(v) if v is not None else '' for v in row]
                # Escape commas and quotes
                values = [f'"{v}"' if ',' in v or '"' in v else v for v in values]
                f.write(','.join(values) + '\n')

        print(f"  Exported {table}: {len(rows):,} rows -> {csv_path.name}")

    conn.close()
    print(f"\nCSV files saved to: {output_dir}")


def main():
    """Run all analysis functions."""
    print("\n" + "="*60)
    print("  POLYMARKET COD DATA ANALYSIS")
    print("="*60)

    show_summary()
    list_markets()
    show_trade_activity()
    show_recent_trades(10)
    show_price_history_sample()
    show_orderbook_snapshots()
    show_realtime_activity()

    print_section("EXPORT OPTIONS")
    print("To export data to CSV files, run:")
    print("  python examples/analyze_data.py --export")
    print()
    print("To use with pandas:")
    print("  import pandas as pd")
    print("  import sqlite3")
    print("  conn = sqlite3.connect('data/polymarket_cod.db')")
    print("  df = pd.read_sql_query('SELECT * FROM trades', conn)")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--export":
        export_to_csv()
    else:
        main()

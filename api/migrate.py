"""Database migrations for the API.

Run standalone: python -m api.migrate
Adds the closing_lines table and game_start_time column,
then backfills game_start_time from the Gamma API and
computes closing lines from existing trade data.
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import requests

# Resolve DB path
_SERVER_DB = "/opt/polymarket-collector/data/polymarket_esports.db"
_LOCAL_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "polymarket_esports.db")
DB_PATH = _SERVER_DB if os.path.exists(_SERVER_DB) else _LOCAL_DB

GAMMA_API_URL = "https://gamma-api.polymarket.com"


def _backfill_game_start_times(conn: sqlite3.Connection) -> int:
    """Backfill game_start_time on markets rows where it is NULL, using the Gamma API."""
    cursor = conn.cursor()
    cursor.execute("SELECT market_id FROM markets WHERE game_start_time IS NULL")
    rows = cursor.fetchall()

    if not rows:
        print("  All markets already have game_start_time, nothing to backfill.")
        return 0

    total = len(rows)
    print(f"  Backfilling game_start_time for {total} markets from Gamma API...")
    updated = 0

    for i, row in enumerate(rows):
        mid = row["market_id"]
        try:
            resp = requests.get(f"{GAMMA_API_URL}/markets", params={"id": mid}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data and len(data) > 0:
                gst = data[0].get("gameStartTime")
                if gst:
                    cursor.execute(
                        "UPDATE markets SET game_start_time = ? WHERE market_id = ?",
                        (gst, mid),
                    )
                    updated += 1
        except Exception as e:
            print(f"    Warning: failed to fetch start time for {mid}: {e}")

        if (i + 1) % 20 == 0 or (i + 1) == total:
            print(f"    Fetched {i + 1}/{total} market start times...")
        time.sleep(0.1)  # rate limit

    conn.commit()
    print(f"  Updated game_start_time on {updated}/{total} markets.")
    return updated


def _backfill_closing_lines(conn: sqlite3.Connection) -> int:
    """Compute closing lines from trade data for matches that have final_prices but no closing_lines."""
    cursor = conn.cursor()

    # Find game_ids in final_prices that don't yet have closing_lines entries
    cursor.execute("""
        SELECT DISTINCT fp.game_id, fp.market_id, fp.home_team, fp.away_team,
               fp.final_score, fp.last_trade_price
        FROM final_prices fp
        LEFT JOIN closing_lines cl ON fp.game_id = cl.game_id AND fp.market_id = cl.market_id
        WHERE cl.id IS NULL AND fp.game_id IS NOT NULL
    """)
    rows = cursor.fetchall()

    if not rows:
        print("  No final_prices rows without closing lines, nothing to backfill.")
        return 0

    print(f"  Computing closing lines for {len(rows)} final_prices entries...")
    inserted = 0

    for row in rows:
        game_id = row["game_id"]
        market_id = row["market_id"]
        home_team = row["home_team"]
        away_team = row["away_team"]
        final_score = row["final_score"]
        last_trade_price = row["last_trade_price"]

        # Get game_start_time and question from markets table
        cursor.execute(
            "SELECT game_start_time, question FROM markets WHERE market_id = ?",
            (market_id,),
        )
        mkt = cursor.fetchone()
        if not mkt or not mkt["game_start_time"]:
            continue

        gst_str = mkt["game_start_time"]
        question = mkt["question"]

        # Parse game_start_time to epoch
        try:
            gst_clean = gst_str.replace("+00", "+00:00") if gst_str.endswith("+00") else gst_str
            gst_dt = datetime.fromisoformat(gst_clean)
            if gst_dt.tzinfo is None:
                gst_dt = gst_dt.replace(tzinfo=timezone.utc)
            gst_ts = gst_dt.timestamp()
        except Exception:
            continue

        # Get pre-match trades
        cursor.execute("""
            SELECT outcome, price, timestamp FROM trades
            WHERE market_id = ? AND timestamp < ?
            ORDER BY timestamp ASC
        """, (market_id, gst_ts))
        trades = cursor.fetchall()

        if not trades:
            continue

        # Determine winner
        home_won = None
        if last_trade_price is not None:
            home_won = last_trade_price > 0.5

        # Group by outcome
        outcome_trades = {}
        for t in trades:
            outcome = t["outcome"]
            if outcome not in outcome_trades:
                outcome_trades[outcome] = []
            outcome_trades[outcome].append({"price": t["price"], "timestamp": t["timestamp"]})

        for team_name in [home_team, away_team]:
            if not team_name:
                continue
            is_home = (team_name == home_team)

            if team_name in outcome_trades and outcome_trades[team_name]:
                team_trades = outcome_trades[team_name]
                last_t = max(team_trades, key=lambda t: t["timestamp"])
                closing_price = last_t["price"]
                min_price = min(t["price"] for t in team_trades)
                max_price = max(t["price"] for t in team_trades)
                n_trades = len(team_trades)
            else:
                other_team = away_team if is_home else home_team
                if other_team not in outcome_trades or not outcome_trades[other_team]:
                    continue
                other_trades = outcome_trades[other_team]
                last_other = max(other_trades, key=lambda t: t["timestamp"])
                closing_price = 1 - last_other["price"]
                min_price = 1 - max(t["price"] for t in other_trades)
                max_price = 1 - min(t["price"] for t in other_trades)
                n_trades = 0

            if home_won is not None:
                team_won = 1 if ((is_home and home_won) or (not is_home and not home_won)) else 0
            else:
                team_won = None

            cursor.execute("""
                INSERT INTO closing_lines
                (game_id, market_id, home_team, away_team, team,
                 is_home, question, game_start_time,
                 closing_price, min_price, max_price,
                 final_score, team_won, n_trades)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_id, market_id, home_team, away_team, team_name,
                1 if is_home else 0, question, gst_str,
                closing_price, min_price, max_price,
                final_score, team_won, n_trades,
            ))
            inserted += 1

    conn.commit()
    print(f"  Inserted {inserted} closing line rows.")
    return inserted


def migrate(db_path: str | None = None):
    """Run all migrations."""
    path = db_path or DB_PATH
    if not os.path.exists(path):
        print(f"Database not found at {path}")
        sys.exit(1)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Add game_start_time column to markets table if missing
    cursor.execute("PRAGMA table_info(markets)")
    existing_cols = {row["name"] for row in cursor.fetchall()}

    if "game_start_time" not in existing_cols:
        print("Adding game_start_time column to markets table...")
        cursor.execute("ALTER TABLE markets ADD COLUMN game_start_time TEXT")
        conn.commit()
        print("  Done.")
    else:
        print("game_start_time column already exists on markets.")

    # 2. Create closing_lines table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS closing_lines (
            id INTEGER PRIMARY KEY,
            game_id TEXT,
            market_id TEXT,
            home_team TEXT,
            away_team TEXT,
            team TEXT,
            is_home INTEGER,
            question TEXT,
            game_start_time TEXT,
            closing_price REAL,
            min_price REAL,
            max_price REAL,
            final_score TEXT,
            team_won INTEGER,
            n_trades INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closing_lines_game_id ON closing_lines(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closing_lines_gst ON closing_lines(game_start_time)")
    conn.commit()
    print("closing_lines table ready.")

    # 3. Add wallet identity columns to trades table for bot tracking
    cursor.execute("PRAGMA table_info(trades)")
    trade_cols = {row["name"] for row in cursor.fetchall()}
    trade_migrations = [
        ("proxy_wallet", "TEXT"),
        ("name", "TEXT"),
        ("pseudonym", "TEXT"),
        ("transaction_hash", "TEXT"),
        ("outcome_index", "INTEGER"),
        ("asset", "TEXT"),
    ]
    added = 0
    for col_name, col_type in trade_migrations:
        if col_name not in trade_cols:
            cursor.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
            added += 1
    if added:
        print(f"Added {added} wallet identity column(s) to trades.")
    else:
        print("trades table already has wallet identity columns.")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet ON trades(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_proxy_wallet_ts ON trades(proxy_wallet, timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_market_proxy_wallet ON trades(market_id, proxy_wallet)")
    conn.commit()

    # 4. Wallet aggregation table (Phase 2 - bot tracking)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            proxy_wallet         TEXT PRIMARY KEY,
            pseudonym            TEXT,
            name                 TEXT,
            first_seen_ts        INTEGER,
            last_seen_ts         INTEGER,
            total_trades         INTEGER,
            total_volume_usd     REAL,
            distinct_markets     INTEGER,
            distinct_games       INTEGER,
            games_json           TEXT,
            buy_count            INTEGER,
            sell_count           INTEGER,
            median_trade_size    REAL,
            trade_size_cv        REAL,
            median_inter_trade_s REAL,
            inter_trade_cv       REAL,
            active_hours         INTEGER,
            active_days_per_week REAL,
            round_size_share     REAL,
            night_share          REAL,
            cross_market_burst   INTEGER,
            markets_per_day      REAL,
            two_sided_ratio      REAL,
            bot_score            REAL,
            bot_label            TEXT,
            last_recomputed_ts   INTEGER
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_bot_label ON wallets(bot_label)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_volume ON wallets(total_volume_usd DESC)")

    # 5. CS2 wallet signal correlation table (Phase 4)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cs2_wallet_signals (
            proxy_wallet     TEXT,
            signal_name      TEXT,
            signal_value     REAL,
            n_observations   INTEGER,
            computed_at_ts   INTEGER,
            PRIMARY KEY (proxy_wallet, signal_name)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cs2_signals_name ON cs2_wallet_signals(signal_name)")
    conn.commit()
    print("wallets + cs2_wallet_signals tables ready.")

    # 6. Backfill game_start_time from Gamma API for markets missing it
    print("Backfilling game_start_time...")
    _backfill_game_start_times(conn)

    # 7. Backfill closing_lines from existing trade + final_prices data
    print("Backfilling closing lines from trade data...")
    _backfill_closing_lines(conn)

    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    db_arg = sys.argv[1] if len(sys.argv) > 1 else None
    migrate(db_arg)

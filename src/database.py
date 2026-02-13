"""SQLite database setup and query helpers for Polymarket esports data."""

import sqlite3
import json
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

from .config import DATABASE_PATH

# ---------------------------------------------------------------------------
# Persistent connection singleton (WAL mode, reused across the process)
# ---------------------------------------------------------------------------

_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def get_connection() -> sqlite3.Connection:
    """Return the shared database connection, creating it on first call.

    The connection uses WAL journal mode and NORMAL synchronous for performance.
    A threading lock serialises write access.
    """
    global _conn
    if _conn is not None:
        return _conn

    with _lock:
        # Double-check after acquiring lock
        if _conn is not None:
            return _conn

        db_path = Path(DATABASE_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        _conn = conn

    return _conn


def close_connection() -> None:
    """Close the shared database connection (for clean shutdown)."""
    global _conn
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None


# ---------------------------------------------------------------------------
# Realtime price write buffer
# ---------------------------------------------------------------------------

_rt_buffer: List[tuple] = []
_rt_buffer_lock = threading.Lock()
_rt_last_flush: float = time.monotonic()

_RT_BUFFER_SIZE = 100
_RT_FLUSH_INTERVAL = 1.0  # seconds


def buffer_realtime_price(market_id: str, timestamp: int, bid: float, ask: float, last_price: float) -> None:
    """Accumulate a realtime price row; auto-flushes every 100 rows or 1 second."""
    with _rt_buffer_lock:
        _rt_buffer.append((market_id, timestamp, bid, ask, last_price))
        if len(_rt_buffer) >= _RT_BUFFER_SIZE or (time.monotonic() - _rt_last_flush) >= _RT_FLUSH_INTERVAL:
            _flush_rt_buffer()


def _flush_rt_buffer() -> None:
    """Flush the realtime price buffer to the database (caller holds _rt_buffer_lock)."""
    global _rt_last_flush
    if not _rt_buffer:
        return

    conn = get_connection()
    with _lock:
        conn.executemany(
            "INSERT INTO realtime_prices (market_id, timestamp, bid, ask, last_price) VALUES (?, ?, ?, ?, ?)",
            _rt_buffer,
        )
        conn.commit()

    _rt_buffer.clear()
    _rt_last_flush = time.monotonic()


def flush_all_buffers() -> None:
    """Flush every pending write buffer. Call before shutdown."""
    with _rt_buffer_lock:
        _flush_rt_buffer()


# ---------------------------------------------------------------------------
# Database initialisation & migration
# ---------------------------------------------------------------------------

def maybe_rename_legacy_db() -> None:
    """Rename the legacy polymarket_cod.db to polymarket_esports.db if it exists."""
    legacy_path = Path("data/polymarket_cod.db")
    new_path = Path(DATABASE_PATH)

    if legacy_path.exists() and not new_path.exists():
        new_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(legacy_path), str(new_path))


def init_database() -> None:
    """Initialize the database with required tables."""
    conn = get_connection()
    cursor = conn.cursor()

    # Markets table (with new columns for multi-game support)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY,
            market_id TEXT UNIQUE,
            condition_id TEXT,
            clob_token_id_yes TEXT,
            clob_token_id_no TEXT,
            question TEXT,
            outcomes TEXT,
            start_date TEXT,
            end_date TEXT,
            game TEXT DEFAULT 'cod',
            event_id TEXT,
            game_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Price history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            timestamp INTEGER,
            price REAL,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Trades table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            trade_id TEXT UNIQUE,
            timestamp INTEGER,
            price REAL,
            size REAL,
            side TEXT,
            outcome TEXT,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Real-time prices table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS realtime_prices (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            timestamp INTEGER,
            bid REAL,
            ask REAL,
            last_price REAL,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Orderbook snapshots table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            token_id TEXT,
            timestamp INTEGER,
            best_bid_price REAL,
            best_bid_size REAL,
            best_ask_price REAL,
            best_ask_size REAL,
            spread REAL,
            mid_price REAL,
            bid_depth TEXT,
            ask_depth TEXT,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Final prices table (snapshots at match end)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS final_prices (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            game TEXT,
            game_id TEXT,
            match_ended_at TEXT,
            snapshot_taken_at TEXT,
            last_trade_price REAL,
            best_bid REAL,
            best_ask REAL,
            mid_price REAL,
            spread REAL,
            home_team TEXT,
            away_team TEXT,
            final_score TEXT,
            match_period TEXT,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Open interest table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS open_interest (
            id INTEGER PRIMARY KEY,
            market_id TEXT,
            condition_id TEXT,
            timestamp INTEGER,
            open_interest REAL,
            FOREIGN KEY (market_id) REFERENCES markets(market_id)
        )
    """)

    # Game ID map table (maps sports WS gameId to Polymarket market_id)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS game_id_map (
            id INTEGER PRIMARY KEY,
            game_id TEXT,
            market_id TEXT,
            event_id TEXT,
            game TEXT,
            UNIQUE(game_id, market_id)
        )
    """)

    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_market ON price_history(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_timestamp ON price_history(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_realtime_market ON realtime_prices(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_realtime_timestamp ON realtime_prices(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_market ON orderbook_snapshots(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON orderbook_snapshots(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_token ON orderbook_snapshots(token_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_game ON markets(game)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_game_id ON markets(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_final_prices_market ON final_prices(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_final_prices_game_id ON final_prices(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_open_interest_market ON open_interest(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_open_interest_timestamp ON open_interest(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_id_map_game_id ON game_id_map(game_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_game_id_map_market_id ON game_id_map(market_id)")

    conn.commit()


def migrate_database() -> None:
    """Migrate existing database to add new columns if they don't exist.

    Should be called BEFORE init_database() so that indexes on new columns
    can be created successfully.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Check if markets table exists at all
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='markets'")
    if not cursor.fetchone():
        return  # Fresh DB, init_database() will create everything

    # Check existing columns on markets table
    cursor.execute("PRAGMA table_info(markets)")
    existing_cols = {row["name"] for row in cursor.fetchall()}

    migrations = [
        ("game", "TEXT DEFAULT 'cod'"),
        ("event_id", "TEXT"),
        ("game_id", "TEXT"),
    ]

    for col_name, col_type in migrations:
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE markets ADD COLUMN {col_name} {col_type}")

    conn.commit()


# ---------------------------------------------------------------------------
# Data insertion helpers
# ---------------------------------------------------------------------------

def upsert_market(market_data: Dict[str, Any]) -> None:
    """Insert or update a market record."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO markets
        (market_id, condition_id, clob_token_id_yes, clob_token_id_no, question, outcomes,
         start_date, end_date, game, event_id, game_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        market_data.get("market_id"),
        market_data.get("condition_id"),
        market_data.get("clob_token_id_yes"),
        market_data.get("clob_token_id_no"),
        market_data.get("question"),
        json.dumps(market_data.get("outcomes", [])),
        market_data.get("start_date"),
        market_data.get("end_date"),
        market_data.get("game", "cod"),
        market_data.get("event_id"),
        market_data.get("game_id"),
    ))

    conn.commit()


def insert_price_history(market_id: str, prices: List[Dict[str, Any]]) -> int:
    """Insert price history records. Returns count of new records inserted."""
    if not prices:
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for price in prices:
        try:
            cursor.execute("""
                INSERT INTO price_history (market_id, timestamp, price)
                VALUES (?, ?, ?)
            """, (market_id, price.get("timestamp"), price.get("price")))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    return inserted


def insert_trades(market_id: str, trades: List[Dict[str, Any]]) -> int:
    """Insert trade records. Returns count of new records inserted."""
    if not trades:
        return 0

    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for trade in trades:
        try:
            cursor.execute("""
                INSERT INTO trades (market_id, trade_id, timestamp, price, size, side, outcome)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                market_id,
                trade.get("trade_id"),
                trade.get("timestamp"),
                trade.get("price"),
                trade.get("size"),
                trade.get("side"),
                trade.get("outcome"),
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    return inserted


def insert_realtime_price(market_id: str, timestamp: int, bid: float, ask: float, last_price: float) -> None:
    """Insert a real-time price record."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO realtime_prices (market_id, timestamp, bid, ask, last_price)
        VALUES (?, ?, ?, ?, ?)
    """, (market_id, timestamp, bid, ask, last_price))

    conn.commit()


def insert_orderbook_snapshot(
    market_id: str,
    token_id: str,
    timestamp: int,
    best_bid_price: Optional[float],
    best_bid_size: Optional[float],
    best_ask_price: Optional[float],
    best_ask_size: Optional[float],
    spread: Optional[float],
    mid_price: Optional[float],
    bid_depth: List[Dict[str, Any]],
    ask_depth: List[Dict[str, Any]],
) -> None:
    """Insert an orderbook snapshot record."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO orderbook_snapshots
        (market_id, token_id, timestamp, best_bid_price, best_bid_size,
         best_ask_price, best_ask_size, spread, mid_price, bid_depth, ask_depth)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        market_id,
        token_id,
        timestamp,
        best_bid_price,
        best_bid_size,
        best_ask_price,
        best_ask_size,
        spread,
        mid_price,
        json.dumps(bid_depth),
        json.dumps(ask_depth),
    ))

    conn.commit()


def insert_final_price(
    market_id: str,
    game: str,
    game_id: str,
    match_ended_at: str,
    snapshot_taken_at: str,
    last_trade_price: Optional[float],
    best_bid: Optional[float],
    best_ask: Optional[float],
    mid_price: Optional[float],
    spread: Optional[float],
    home_team: Optional[str],
    away_team: Optional[str],
    final_score: Optional[str],
    match_period: Optional[str],
) -> None:
    """Insert a final price snapshot taken at match end."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO final_prices
        (market_id, game, game_id, match_ended_at, snapshot_taken_at,
         last_trade_price, best_bid, best_ask, mid_price, spread,
         home_team, away_team, final_score, match_period)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        market_id, game, game_id, match_ended_at, snapshot_taken_at,
        last_trade_price, best_bid, best_ask, mid_price, spread,
        home_team, away_team, final_score, match_period,
    ))

    conn.commit()


def insert_open_interest(
    market_id: str,
    condition_id: str,
    timestamp: int,
    open_interest: float,
) -> None:
    """Insert an open interest record."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO open_interest (market_id, condition_id, timestamp, open_interest)
        VALUES (?, ?, ?, ?)
    """, (market_id, condition_id, timestamp, open_interest))

    conn.commit()


def upsert_game_id_mapping(game_id: str, market_id: str, event_id: Optional[str], game: str) -> None:
    """Insert or update a game_id -> market_id mapping."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO game_id_map (game_id, market_id, event_id, game)
        VALUES (?, ?, ?, ?)
    """, (game_id, market_id, event_id, game))

    conn.commit()


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_markets_by_game_id(game_id: str) -> List[Dict[str, Any]]:
    """Get all markets associated with a sports WS game_id."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.* FROM markets m
        INNER JOIN game_id_map gm ON m.market_id = gm.market_id
        WHERE gm.game_id = ?
    """, (game_id,))
    rows = cursor.fetchall()

    markets = []
    for row in rows:
        market = dict(row)
        market["outcomes"] = json.loads(market["outcomes"]) if market["outcomes"] else []
        markets.append(market)

    return markets


def get_all_markets(game: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all stored markets, optionally filtered by game."""
    conn = get_connection()
    cursor = conn.cursor()

    if game:
        cursor.execute("SELECT * FROM markets WHERE game = ?", (game,))
    else:
        cursor.execute("SELECT * FROM markets")
    rows = cursor.fetchall()

    markets = []
    for row in rows:
        market = dict(row)
        market["outcomes"] = json.loads(market["outcomes"]) if market["outcomes"] else []
        markets.append(market)

    return markets


def get_market_by_id(market_id: str) -> Optional[Dict[str, Any]]:
    """Get a market by its ID."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM markets WHERE market_id = ?", (market_id,))
    row = cursor.fetchone()

    if row:
        market = dict(row)
        market["outcomes"] = json.loads(market["outcomes"]) if market["outcomes"] else []
        return market
    return None


def get_latest_price_timestamp(market_id: str) -> Optional[int]:
    """Get the latest price history timestamp for a market."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT MAX(timestamp) as max_ts FROM price_history WHERE market_id = ?",
        (market_id,)
    )
    row = cursor.fetchone()

    return row["max_ts"] if row and row["max_ts"] else None


def get_latest_trade_timestamp(market_id: str) -> Optional[int]:
    """Get the latest trade timestamp for a market."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT MAX(timestamp) as max_ts FROM trades WHERE market_id = ?",
        (market_id,)
    )
    row = cursor.fetchone()

    return row["max_ts"] if row and row["max_ts"] else None


def get_stats() -> Dict[str, Any]:
    """Get database statistics including per-game breakdown."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    cursor.execute("SELECT COUNT(*) FROM markets")
    stats["markets"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM price_history")
    stats["price_history_records"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM trades")
    stats["trade_records"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM realtime_prices")
    stats["realtime_price_records"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM orderbook_snapshots")
    stats["orderbook_snapshots"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM final_prices")
    stats["final_price_snapshots"] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM open_interest")
    stats["open_interest_records"] = cursor.fetchone()[0]

    # Per-game market counts
    cursor.execute("SELECT game, COUNT(*) as cnt FROM markets GROUP BY game")
    stats["markets_by_game"] = {row["game"]: row["cnt"] for row in cursor.fetchall()}

    return stats

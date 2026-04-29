"""Read-only SQLite database helpers for the API.

Uses URI mode with ?mode=ro for read-only access.
WAL mode (set by the collector) allows unlimited concurrent readers.
"""

import sqlite3
import threading
from typing import Any

from .config import DB_PATH

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Return a thread-local read-only connection, creating on first call."""
    conn: sqlite3.Connection | None = getattr(_local, "conn", None)
    if conn is not None:
        return conn

    uri = f"file:{DB_PATH}?mode=ro"
    # Thread-local (not shared) so concurrent FastAPI requests don't serialize
    # on a single connection's GIL-protected execute.
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    # Small private cache: this multiplies by the threadpool size and the
    # systemd unit caps the API at MemoryMax=100M. The real perf win is mmap
    # below, which is shared via the OS page cache.
    conn.execute("PRAGMA cache_size = -4000")
    conn.execute("PRAGMA mmap_size = 268435456")

    _local.conn = conn
    return conn


def query_all(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    """Execute a query and return all rows as dicts."""
    conn = _get_conn()
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def query_one(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    """Execute a query and return the first row as a dict, or None."""
    conn = _get_conn()
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    return dict(row) if row else None


def db_path() -> str:
    """Return the database file path."""
    return DB_PATH

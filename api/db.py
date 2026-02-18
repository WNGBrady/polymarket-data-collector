"""Read-only SQLite database helpers for the API.

Uses URI mode with ?mode=ro for read-only access.
WAL mode (set by the collector) allows unlimited concurrent readers.
"""

import sqlite3
from typing import Any

from .config import DB_PATH

_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    """Return a shared read-only connection, creating on first call."""
    global _conn
    if _conn is not None:
        return _conn

    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    _conn = conn
    return _conn


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

"""Simple in-memory TTL cache. No external dependencies."""

import time
from typing import Any

_store: dict[str, tuple[float, Any]] = {}


def get(key: str) -> Any | None:
    """Return cached value if it exists and hasn't expired, else None."""
    entry = _store.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if time.monotonic() > expires_at:
        del _store[key]
        return None
    return value


def put(key: str, value: Any, ttl: float) -> None:
    """Store a value with a TTL in seconds."""
    _store[key] = (time.monotonic() + ttl, value)


def make_key(endpoint: str, game: str, date_start: str, date_end: str) -> str:
    """Build a cache key from common filter params."""
    return f"{endpoint}:{game}:{date_start}:{date_end}"

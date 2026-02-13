"""Utility functions for rate limiting and error handling."""

import time
import logging
from functools import wraps
from typing import Callable, Any, Dict
from collections import defaultdict

from .config import RATE_LIMITS, RATE_LIMIT_WINDOW, MAX_RETRIES, INITIAL_BACKOFF, MAX_BACKOFF

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("polymarket_esports")


class RateLimiter:
    """Rate limiter that tracks requests per endpoint."""

    def __init__(self):
        self.request_times: Dict[str, list] = defaultdict(list)

    def wait_if_needed(self, endpoint_key: str) -> None:
        """Wait if we're approaching the rate limit for this endpoint."""
        if endpoint_key not in RATE_LIMITS:
            return

        limit = RATE_LIMITS[endpoint_key]
        now = time.time()
        window_start = now - RATE_LIMIT_WINDOW

        # Clean old requests
        self.request_times[endpoint_key] = [
            t for t in self.request_times[endpoint_key] if t > window_start
        ]

        # Check if we need to wait
        if len(self.request_times[endpoint_key]) >= limit:
            oldest = min(self.request_times[endpoint_key])
            wait_time = oldest + RATE_LIMIT_WINDOW - now + 0.1
            if wait_time > 0:
                logger.info(f"Rate limit approaching for {endpoint_key}, waiting {wait_time:.2f}s")
                time.sleep(wait_time)

        # Record this request
        self.request_times[endpoint_key].append(time.time())

    def reset(self, endpoint_key: str = None) -> None:
        """Reset rate limit tracking for an endpoint or all endpoints."""
        if endpoint_key:
            self.request_times[endpoint_key] = []
        else:
            self.request_times.clear()


# Global rate limiter instance
rate_limiter = RateLimiter()


def with_retry(func: Callable) -> Callable:
    """Decorator to add exponential backoff retry logic."""
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        backoff = INITIAL_BACKOFF
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                # Check if it's a rate limit error (429)
                if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                    if e.response.status_code == 429:
                        logger.warning(f"Rate limited on attempt {attempt + 1}, backing off {backoff}s")
                    elif e.response.status_code >= 500:
                        logger.warning(f"Server error on attempt {attempt + 1}, backing off {backoff}s")
                    else:
                        raise
                else:
                    logger.warning(f"Error on attempt {attempt + 1}: {e}, backing off {backoff}s")

                if attempt < MAX_RETRIES - 1:
                    time.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)

        raise last_exception

    return wrapper


async def async_with_retry(coro_func: Callable) -> Callable:
    """Async version of retry decorator."""
    import asyncio

    @wraps(coro_func)
    async def wrapper(*args, **kwargs) -> Any:
        backoff = INITIAL_BACKOFF
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                return await coro_func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                logger.warning(f"Async error on attempt {attempt + 1}: {e}, backing off {backoff}s")

                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)

        raise last_exception

    return wrapper


def parse_timestamp(timestamp_value: Any) -> int:
    """Parse various timestamp formats to Unix timestamp (seconds or milliseconds)."""
    if timestamp_value is None:
        return None

    if isinstance(timestamp_value, (int, float)):
        return int(timestamp_value)

    if isinstance(timestamp_value, str):
        # Try parsing as ISO format
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except ValueError:
            pass

        # Try parsing as numeric string
        try:
            return int(float(timestamp_value))
        except ValueError:
            pass

    return None


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

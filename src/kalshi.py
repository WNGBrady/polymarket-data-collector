"""Thin HTTP client for the Kalshi public API.

Kalshi exposes per-market orderbook + trade history under
https://api.elections.kalshi.com/trade-api/v2 with no auth. We hit it
synchronously with `requests` and let the realtime collector run calls in
threads via `asyncio.to_thread`, mirroring how `historical_collector` is
consumed elsewhere.

Orderbook shape (as of 2026-05): /markets/{ticker}/orderbook returns
    { "orderbook_fp": { "yes_dollars": [["0.19","73247.31"], ...],
                        "no_dollars":  [["0.20","54944.00"], ...] } }
The legacy `yes`/`no` keys used by probe_kalshi.py are gone.

Trades shape: /markets/trades?ticker=X returns
    { "cursor": "...", "trades": [{"trade_id","created_time","taker_side",
                                   "yes_price_dollars","no_price_dollars",
                                   "count_fp"}, ...] }
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from .config import KALSHI_API_BASE, KALSHI_HTTP_TIMEOUT
from .utils import logger


def _parse_iso8601_ms(s: str) -> int:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_levels(raw: Optional[List[List[Any]]]) -> List[Dict[str, float]]:
    """Kalshi returns levels as [["0.19","73247.31"], ...] sorted ascending by price.
    Convert to [{price, size}] with floats; drop malformed rows."""
    out: List[Dict[str, float]] = []
    if not raw:
        return out
    for level in raw:
        if not level or len(level) < 2:
            continue
        p = _safe_float(level[0])
        sz = _safe_float(level[1])
        if p is None or sz is None:
            continue
        out.append({"price": p, "size": sz})
    return out


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = f"{KALSHI_API_BASE}{path}"
    try:
        r = requests.get(url, params=params, timeout=KALSHI_HTTP_TIMEOUT)
    except requests.RequestException as e:
        logger.debug(f"kalshi: request failed {url}: {e}")
        return None
    if r.status_code >= 500:
        # one cheap retry on server-side blips
        try:
            r = requests.get(url, params=params, timeout=KALSHI_HTTP_TIMEOUT)
        except requests.RequestException as e:
            logger.debug(f"kalshi: retry failed {url}: {e}")
            return None
    if r.status_code != 200:
        logger.debug(f"kalshi: HTTP {r.status_code} {url}: {r.text[:200]}")
        return None
    try:
        return r.json()
    except ValueError:
        return None


def fetch_orderbook(ticker: str) -> Optional[Dict[str, Any]]:
    """Return parsed orderbook for `ticker`.

    Output:
        {
          "yes_bids": [{price, size}, ...],   # ordered ascending by price
          "yes_asks": [...],                   # YES side has no asks key in the API;
                                                # we synthesize asks from no_dollars
                                                # because YES_ask_price = 1 - NO_bid_price.
          "no_bids":  [...],
          "no_asks":  [...],
          "yes_best_bid", "yes_best_ask",
          "no_best_bid",  "no_best_ask",
          "yes_mid", "yes_spread"
        }

    Kalshi only ships resting orders for one side per ticker (yes_dollars + no_dollars
    on the same ticker are the YES_bid and NO_bid ladders). We derive the YES_ask
    ladder from the NO_bid ladder via the binary-contract identity: YES_ask = 1 - NO_bid.
    """
    data = _get(f"/markets/{ticker}/orderbook")
    if not data:
        return None
    book = data.get("orderbook_fp") or data.get("orderbook") or {}

    yes_bids = _parse_levels(book.get("yes_dollars") or book.get("yes"))
    no_bids = _parse_levels(book.get("no_dollars") or book.get("no"))

    # Best YES bid = highest price on yes_bids; ladder is ascending so it's the last entry.
    yes_best_bid = yes_bids[-1]["price"] if yes_bids else None
    no_best_bid = no_bids[-1]["price"] if no_bids else None

    # YES_ask = 1 - NO_bid (using the highest NO bid).
    yes_best_ask = (1.0 - no_best_bid) if no_best_bid is not None else None
    no_best_ask = (1.0 - yes_best_bid) if yes_best_bid is not None else None

    yes_mid = None
    yes_spread = None
    if yes_best_bid is not None and yes_best_ask is not None:
        yes_mid = (yes_best_bid + yes_best_ask) / 2.0
        yes_spread = yes_best_ask - yes_best_bid

    # YES_asks ladder synthesized from no_bids (mirror prices).
    yes_asks = [{"price": 1.0 - lvl["price"], "size": lvl["size"]} for lvl in reversed(no_bids)]
    no_asks = [{"price": 1.0 - lvl["price"], "size": lvl["size"]} for lvl in reversed(yes_bids)]

    return {
        "yes_bids": yes_bids,
        "yes_asks": yes_asks,
        "no_bids": no_bids,
        "no_asks": no_asks,
        "yes_best_bid": yes_best_bid,
        "yes_best_ask": yes_best_ask,
        "no_best_bid": no_best_bid,
        "no_best_ask": no_best_ask,
        "yes_mid": yes_mid,
        "yes_spread": yes_spread,
    }


def fetch_trades(ticker: str, since_ms: Optional[int] = None, max_pages: int = 5) -> List[Dict[str, Any]]:
    """Fetch trades for `ticker`, newest first. Stops at the first page whose
    oldest trade is older than `since_ms`, or after `max_pages` cursor follows.
    Returns dicts with parsed timestamps in epoch-ms.

    Kalshi's /markets/trades is cursor-paginated and ordered newest-first, so a
    typical 60s poll hits 1 page. The cap is a guardrail against runaway
    pagination if the cursor doesn't terminate.
    """
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    while pages < max_pages:
        params: Dict[str, Any] = {"ticker": ticker, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data = _get("/markets/trades", params=params)
        if not data:
            break
        trades = data.get("trades") or []
        if not trades:
            break

        stop = False
        for t in trades:
            ct = t.get("created_time")
            if not ct:
                continue
            try:
                ts_ms = _parse_iso8601_ms(ct)
            except Exception:
                continue
            if since_ms is not None and ts_ms <= since_ms:
                stop = True
                break
            out.append({
                "ticker": ticker,
                "trade_id": t.get("trade_id"),
                "created_time_ms": ts_ms,
                "taker_side": t.get("taker_side"),
                "yes_price": _safe_float(t.get("yes_price_dollars")),
                "no_price": _safe_float(t.get("no_price_dollars")),
                "count_fp": _safe_float(t.get("count_fp")),
            })
        if stop:
            break
        cursor = data.get("cursor")
        if not cursor:
            break
        pages += 1

    return out


def fetch_market(ticker: str) -> Optional[Dict[str, Any]]:
    """Pull market metadata (status, close_time, title) for one ticker."""
    data = _get(f"/markets/{ticker}")
    if not data:
        return None
    return data.get("market") or data

"""Historical data collection module for fetching price, trade, and open interest history."""

import json
import requests
from typing import List, Dict, Any, Optional
import time

from .config import CLOB_API_URL, DATA_API_URL, OI_API_URL, ORDERBOOK_DEPTH
from .database import (
    get_all_markets,
    insert_price_history,
    insert_trades,
    insert_orderbook_snapshot,
    insert_open_interest,
    get_latest_price_timestamp,
    get_latest_trade_timestamp,
)
from .utils import rate_limiter, with_retry, logger, parse_timestamp, safe_float


@with_retry
def fetch_price_history(
    token_id: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    interval: str = "1h",
) -> List[Dict[str, Any]]:
    """
    Fetch price history for a CLOB token.

    Args:
        token_id: The CLOB token ID
        start_ts: Start timestamp (Unix seconds)
        end_ts: End timestamp (Unix seconds)
        interval: Time interval (1m, 1h, 6h, 1d, 1w, max)
    """
    rate_limiter.wait_if_needed("clob_prices")

    url = f"{CLOB_API_URL}/prices-history"

    # Always provide time bounds - default to 30 days back if not specified
    current_time = int(time.time())
    if end_ts is None:
        end_ts = current_time
    if start_ts is None:
        start_ts = current_time - (86400 * 30)  # 30 days back

    params = {
        "market": token_id,
        "interval": interval,  # Use interval instead of fidelity
        "startTs": start_ts,
        "endTs": end_ts,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Handle different response formats
    if isinstance(data, dict):
        history = data.get("history", [])
    elif isinstance(data, list):
        history = data
    else:
        history = []

    prices = []
    for item in history:
        ts = parse_timestamp(item.get("t") or item.get("timestamp"))
        price = safe_float(item.get("p") or item.get("price"))
        if ts and price:
            prices.append({"timestamp": ts, "price": price})

    return prices


@with_retry
def fetch_trades(
    condition_id: str,
    offset: int = 0,
    limit: int = 10000,
) -> Dict[str, Any]:
    """
    Fetch trade history for a market using offset pagination.

    Args:
        condition_id: The market condition ID
        offset: Offset for pagination (number of records to skip)
        limit: Number of trades to fetch (max 10000)
    """
    rate_limiter.wait_if_needed("data_trades")

    url = f"{DATA_API_URL}/trades"
    params = {
        "market": condition_id,
        "limit": min(limit, 10000),  # API max is 10000
        "offset": offset,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


@with_retry
def fetch_orderbook(token_id: str) -> Dict[str, Any]:
    """
    Fetch current orderbook for a CLOB token.

    Args:
        token_id: The CLOB token ID

    Returns:
        Orderbook data with bids and asks
    """
    rate_limiter.wait_if_needed("clob_book")

    url = f"{CLOB_API_URL}/book"
    params = {"token_id": token_id}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


@with_retry
def fetch_open_interest(condition_id: str) -> Optional[float]:
    """
    Fetch open interest for a market condition.

    Args:
        condition_id: The market condition ID

    Returns:
        Open interest value, or None if not available.
    """
    rate_limiter.wait_if_needed("data_oi")

    url = OI_API_URL
    params = {"market": condition_id}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Handle different response formats
    if isinstance(data, dict):
        oi = data.get("openInterest") or data.get("oi") or data.get("value")
        if oi is not None:
            return safe_float(oi)
    elif isinstance(data, (int, float)):
        return float(data)
    elif isinstance(data, str):
        return safe_float(data)

    return None


def process_orderbook(book_data: Dict[str, Any], depth: int = 5) -> Dict[str, Any]:
    """
    Process orderbook data into a standardized format.

    Args:
        book_data: Raw orderbook response
        depth: Number of price levels to include

    Returns:
        Processed orderbook with best bid/ask, spread, mid_price, and depth
    """
    bids = book_data.get("bids", [])
    asks = book_data.get("asks", [])

    # Sort bids descending (highest first), asks ascending (lowest first)
    bids = sorted(bids, key=lambda x: safe_float(x.get("price", 0)), reverse=True)
    asks = sorted(asks, key=lambda x: safe_float(x.get("price", 0)))

    # Extract best bid/ask
    best_bid_price = safe_float(bids[0].get("price")) if bids else None
    best_bid_size = safe_float(bids[0].get("size")) if bids else None
    best_ask_price = safe_float(asks[0].get("price")) if asks else None
    best_ask_size = safe_float(asks[0].get("size")) if asks else None

    # Calculate spread and mid price
    spread = None
    mid_price = None
    if best_bid_price and best_ask_price:
        spread = best_ask_price - best_bid_price
        mid_price = (best_bid_price + best_ask_price) / 2

    # Get top N levels
    bid_depth = [
        {"price": safe_float(b.get("price")), "size": safe_float(b.get("size"))}
        for b in bids[:depth]
    ]
    ask_depth = [
        {"price": safe_float(a.get("price")), "size": safe_float(a.get("size"))}
        for a in asks[:depth]
    ]

    return {
        "best_bid_price": best_bid_price,
        "best_bid_size": best_bid_size,
        "best_ask_price": best_ask_price,
        "best_ask_size": best_ask_size,
        "spread": spread,
        "mid_price": mid_price,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
    }


def collect_orderbook_snapshot(market: Dict[str, Any]) -> int:
    """
    Collect orderbook snapshot for a single market.

    Args:
        market: Market data dict with clob_token_id_yes

    Returns:
        Number of snapshots collected (0 or 1)
    """
    market_id = market.get("market_id")
    token_id = market.get("clob_token_id_yes")

    if not token_id:
        logger.debug(f"No CLOB token ID for market {market_id}")
        return 0

    try:
        book_data = fetch_orderbook(token_id)
        processed = process_orderbook(book_data, depth=ORDERBOOK_DEPTH)

        timestamp = int(time.time() * 1000)  # milliseconds

        insert_orderbook_snapshot(
            market_id=market_id,
            token_id=token_id,
            timestamp=timestamp,
            best_bid_price=processed["best_bid_price"],
            best_bid_size=processed["best_bid_size"],
            best_ask_price=processed["best_ask_price"],
            best_ask_size=processed["best_ask_size"],
            spread=processed["spread"],
            mid_price=processed["mid_price"],
            bid_depth=processed["bid_depth"],
            ask_depth=processed["ask_depth"],
        )

        return 1

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            logger.debug(f"  Orderbook not available for {market_id[:20]}...")
        else:
            logger.warning(f"  Error fetching orderbook: {e}")
    except Exception as e:
        logger.warning(f"  Error fetching orderbook: {e}")

    return 0


def run_orderbook_collection(markets: Optional[List[Dict[str, Any]]] = None) -> int:
    """
    Collect orderbook snapshots for all markets.

    Args:
        markets: Optional list of markets. If None, fetches from database.

    Returns:
        Number of orderbook snapshots collected.
    """
    if markets is None:
        markets = get_all_markets()

    if not markets:
        logger.warning("No markets found in database. Run discovery first.")
        return 0

    logger.info(f"Collecting orderbook snapshots for {len(markets)} markets...")

    total_snapshots = 0
    for market in markets:
        snapshots = collect_orderbook_snapshot(market)
        total_snapshots += snapshots
        time.sleep(0.1)  # Small delay between requests

    logger.info(f"Collected {total_snapshots} orderbook snapshots")
    return total_snapshots


def collect_open_interest_for_market(market: Dict[str, Any]) -> int:
    """
    Collect open interest snapshot for a single market.

    Args:
        market: Market data dict with condition_id

    Returns:
        1 if OI was collected, 0 otherwise.
    """
    market_id = market.get("market_id")
    condition_id = market.get("condition_id")

    if not condition_id:
        logger.debug(f"No condition ID for market {market_id}")
        return 0

    try:
        oi = fetch_open_interest(condition_id)
        if oi is not None:
            timestamp = int(time.time() * 1000)
            insert_open_interest(
                market_id=market_id,
                condition_id=condition_id,
                timestamp=timestamp,
                open_interest=oi,
            )
            return 1
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            logger.debug(f"  OI not available for {market_id[:20]}...")
        else:
            logger.warning(f"  Error fetching OI: {e}")
    except Exception as e:
        logger.warning(f"  Error fetching OI for {market_id[:20]}...: {e}")

    return 0


def run_open_interest_collection(markets: Optional[List[Dict[str, Any]]] = None) -> int:
    """
    Collect open interest snapshots for all markets.

    Args:
        markets: Optional list of markets. If None, fetches from database.

    Returns:
        Number of OI snapshots collected.
    """
    if markets is None:
        markets = get_all_markets()

    if not markets:
        logger.warning("No markets found in database. Run discovery first.")
        return 0

    logger.info(f"Collecting open interest for {len(markets)} markets...")

    total = 0
    for market in markets:
        total += collect_open_interest_for_market(market)
        time.sleep(0.1)

    logger.info(f"Collected {total} open interest snapshots")
    return total


def process_trades(trades_response: Any) -> List[Dict[str, Any]]:
    """Process trades response into standard format."""
    trades = []

    # Handle different response formats
    if isinstance(trades_response, dict):
        trade_list = trades_response.get("data", []) or trades_response.get("trades", [])
    elif isinstance(trades_response, list):
        trade_list = trades_response
    else:
        trade_list = []

    for trade in trade_list:
        ts = parse_timestamp(
            trade.get("timestamp") or
            trade.get("matchTime") or
            trade.get("createdAt")
        )
        price = safe_float(trade.get("price"))
        size = safe_float(trade.get("size") or trade.get("amount"))
        side = trade.get("side", "").upper()
        outcome = trade.get("outcome") or trade.get("asset")
        trade_id = trade.get("id") or trade.get("tradeId") or f"{ts}_{price}_{size}"

        if ts and price:
            trades.append({
                "trade_id": str(trade_id),
                "timestamp": ts,
                "price": price,
                "size": size,
                "side": side,
                "outcome": outcome,
            })

    return trades


def collect_price_history_for_market(market: Dict[str, Any]) -> int:
    """
    Collect price history for a single market.

    Note: The CLOB /prices-history endpoint may require API authentication.
    If you have API credentials, set them in the request headers.
    """
    market_id = market.get("market_id")
    token_id_yes = market.get("clob_token_id_yes")

    if not token_id_yes:
        logger.debug(f"No CLOB token ID for market {market_id}")
        return 0

    total_inserted = 0

    # Get the latest timestamp we have
    latest_ts = get_latest_price_timestamp(market_id)
    start_ts = latest_ts + 1 if latest_ts else None

    # Fetch for YES token
    try:
        prices = fetch_price_history(token_id_yes, start_ts=start_ts)
        inserted = insert_price_history(market_id, prices)
        total_inserted += inserted
        if inserted > 0:
            logger.info(f"  Inserted {inserted} price records for {market_id[:20]}...")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            # Price history endpoint may require authentication
            logger.debug(f"  Price history not available for {market_id[:20]}... (may require auth)")
        else:
            logger.warning(f"  Error fetching prices: {e}")
    except Exception as e:
        logger.warning(f"  Error fetching prices: {e}")

    return total_inserted


def collect_trades_for_market(market: Dict[str, Any]) -> int:
    """Collect trade history for a single market using offset pagination."""
    market_id = market.get("market_id")
    condition_id = market.get("condition_id")

    if not condition_id:
        logger.warning(f"No condition ID for market {market_id}")
        return 0

    total_inserted = 0
    offset = 0
    limit = 10000  # API max
    max_iterations = 100  # Safety limit

    logger.info(f"Fetching trades for market {market_id[:20]}...")

    for iteration in range(max_iterations):
        try:
            response = fetch_trades(condition_id, offset=offset, limit=limit)
            trades = process_trades(response)

            if not trades:
                break

            inserted = insert_trades(market_id, trades)
            total_inserted += inserted

            # If we got fewer trades than the limit, we've reached the end
            if len(trades) < limit:
                break

            # Move offset forward for next page
            offset += len(trades)

            # Small delay between pages
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"  Error fetching trades at offset {offset}: {e}")
            break

    logger.info(f"  Inserted {total_inserted} trade records")
    return total_inserted


def run_historical_collection(markets: Optional[List[Dict[str, Any]]] = None) -> Dict[str, int]:
    """
    Run historical data collection for all markets.

    Args:
        markets: Optional list of markets. If None, fetches from database.

    Returns:
        Dictionary with counts of inserted records.
    """
    if markets is None:
        markets = get_all_markets()

    if not markets:
        logger.warning("No markets found in database. Run discovery first.")
        return {"prices": 0, "trades": 0, "open_interest": 0}

    logger.info(f"Starting historical collection for {len(markets)} markets...")

    total_prices = 0
    total_trades = 0
    total_oi = 0

    for i, market in enumerate(markets):
        question = market.get('question', 'Unknown')[:50]
        logger.info(f"Processing market {i + 1}/{len(markets)}: {question}...")

        # Collect price history (may not work without API auth)
        prices = collect_price_history_for_market(market)
        total_prices += prices

        # Collect trade history
        trades = collect_trades_for_market(market)
        total_trades += trades

        # Collect open interest
        oi = collect_open_interest_for_market(market)
        total_oi += oi

        # Respectful delay between markets
        time.sleep(0.5)

    if total_prices == 0:
        logger.info("Note: Price history endpoint may require API authentication")

    logger.info(
        f"Historical collection complete. Prices: {total_prices}, "
        f"Trades: {total_trades}, OI: {total_oi}"
    )
    return {"prices": total_prices, "trades": total_trades, "open_interest": total_oi}

"""Real-time data collection module using WebSocket streaming."""

import asyncio
import json
import time
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import OrderedDict
import websockets
from websockets.exceptions import ConnectionClosed

from .config import WEBSOCKET_URL, ORDERBOOK_POLL_INTERVAL
from .database import get_all_markets, buffer_realtime_price, flush_all_buffers, insert_orderbook_snapshot
from .historical_collector import fetch_orderbook, process_orderbook
from .utils import logger, safe_float


class DeduplicationCache:
    """
    Cache for deduplicating price updates within a time window.

    Prevents storing duplicate updates that arrive within the same second.
    """

    def __init__(self, ttl_seconds: int = 1, max_size: int = 10000):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: OrderedDict[str, float] = OrderedDict()

    def _make_key(self, market_id: str, price: Optional[float], bid: Optional[float], ask: Optional[float]) -> str:
        """Create a cache key from the update data."""
        return f"{market_id}:{price}:{bid}:{ask}"

    def is_duplicate(self, market_id: str, price: Optional[float], bid: Optional[float], ask: Optional[float]) -> bool:
        """
        Check if this update is a duplicate of a recent update.

        Returns True if we've seen this exact update within ttl_seconds.
        """
        key = self._make_key(market_id, price, bid, ask)
        now = time.time()

        # Clean expired entries
        self._cleanup(now)

        if key in self._cache:
            return True

        # Add to cache
        self._cache[key] = now

        # Enforce max size
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

        return False

    def _cleanup(self, now: float) -> None:
        """Remove expired entries from the cache."""
        expired_keys = [
            key for key, timestamp in self._cache.items()
            if now - timestamp > self.ttl_seconds
        ]
        for key in expired_keys:
            del self._cache[key]


class RealtimeCollector:
    """WebSocket-based real-time price collector with deduplication and orderbook polling."""

    def __init__(self, enable_orderbook_polling: bool = False):
        self.websocket = None
        self.subscribed_markets: Set[str] = set()
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.message_count = 0
        self.duplicate_count = 0
        self.dedup_cache = DeduplicationCache(ttl_seconds=1)
        self.enable_orderbook_polling = enable_orderbook_polling
        self.markets: List[Dict[str, Any]] = []
        self._orderbook_task: Optional[asyncio.Task] = None

    async def connect(self) -> bool:
        """Establish WebSocket connection."""
        try:
            self.websocket = await websockets.connect(
                WEBSOCKET_URL,
                ping_interval=30,
                ping_timeout=10,
            )
            logger.info("WebSocket connected")
            self.reconnect_delay = 1
            return True
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            return False

    async def subscribe(self, token_ids: List[str]) -> None:
        """Subscribe to price updates for given token IDs."""
        if not self.websocket:
            logger.error("Cannot subscribe - not connected")
            return

        # Polymarket WebSocket expects: {"assets_ids": [...], "type": "market"}
        # Send subscription in batches to avoid message size limits
        batch_size = 50
        for i in range(0, len(token_ids), batch_size):
            batch = token_ids[i:i + batch_size]
            try:
                subscribe_msg = {
                    "assets_ids": batch,
                    "type": "market"
                }
                await self.websocket.send(json.dumps(subscribe_msg))
                self.subscribed_markets.update(batch)
                logger.info(f"Subscribed to {len(batch)} tokens (batch {i // batch_size + 1})")
            except Exception as e:
                logger.error(f"Failed to subscribe to batch: {e}")

    def process_message(self, message: str) -> List[Dict[str, Any]]:
        """Process incoming WebSocket message. Returns list of price updates."""
        # Skip invalid operation messages
        if message in ("INVALID OPERATION", "INVALID MESSAGE"):
            return []

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return []

        results = []

        # Polymarket sends arrays of market data
        if isinstance(data, list):
            for item in data:
                price_data = self._extract_price_data(item)
                if price_data:
                    results.append(price_data)
        elif isinstance(data, dict):
            # Handle specific event types
            event_type = data.get("event_type") or data.get("type")

            if event_type in ("book", "last_trade_price", "price_change", None):
                price_data = self._extract_price_data(data)
                if price_data:
                    results.append(price_data)

        return results

    def _extract_price_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract price data from a market update message."""
        # Get market/asset identifier
        market_id = data.get("asset_id") or data.get("market") or data.get("token_id")
        if not market_id:
            return None

        timestamp = data.get("timestamp")
        if timestamp:
            timestamp = int(timestamp)
        else:
            timestamp = int(time.time() * 1000)

        # Extract price information
        price = safe_float(data.get("price"))
        last_trade_price = safe_float(data.get("last_trade_price"))
        best_bid = safe_float(data.get("best_bid"))
        best_ask = safe_float(data.get("best_ask"))

        # Also check for nested book data
        if "bids" in data and data["bids"]:
            bids = data["bids"]
            if isinstance(bids, list) and len(bids) > 0:
                if isinstance(bids[0], dict):
                    best_bid = safe_float(bids[0].get("price"))
                elif isinstance(bids[0], (int, float, str)):
                    best_bid = safe_float(bids[0])

        if "asks" in data and data["asks"]:
            asks = data["asks"]
            if isinstance(asks, list) and len(asks) > 0:
                if isinstance(asks[0], dict):
                    best_ask = safe_float(asks[0].get("price"))
                elif isinstance(asks[0], (int, float, str)):
                    best_ask = safe_float(asks[0])

        # Use any available price
        last_price = last_trade_price or price

        if last_price or best_bid or best_ask:
            return {
                "market_id": market_id,
                "timestamp": timestamp,
                "last_price": last_price,
                "bid": best_bid,
                "ask": best_ask,
            }

        return None

    async def listen(self) -> None:
        """Listen for incoming messages and store them with deduplication."""
        if not self.websocket:
            logger.error("Cannot listen - not connected")
            return

        try:
            async for message in self.websocket:
                price_updates = self.process_message(message)

                for price_data in price_updates:
                    try:
                        # Check for duplicates
                        if self.dedup_cache.is_duplicate(
                            price_data["market_id"],
                            price_data.get("last_price"),
                            price_data.get("bid"),
                            price_data.get("ask"),
                        ):
                            self.duplicate_count += 1
                            continue

                        buffer_realtime_price(
                            market_id=price_data["market_id"],
                            timestamp=price_data["timestamp"],
                            bid=price_data.get("bid"),
                            ask=price_data.get("ask"),
                            last_price=price_data.get("last_price"),
                        )
                        self.message_count += 1

                        # Log periodically
                        if self.message_count % 10 == 0:
                            logger.info(
                                f"Received {self.message_count} price updates "
                                f"(skipped {self.duplicate_count} duplicates)"
                            )

                    except Exception as e:
                        logger.error(f"Failed to store price data: {e}")

        except ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"Error in WebSocket listener: {e}")

    async def _poll_orderbooks(self) -> None:
        """Periodically poll orderbook data for all markets."""
        logger.info(f"Starting orderbook polling (every {ORDERBOOK_POLL_INTERVAL}s)...")

        while self.running:
            try:
                for market in self.markets:
                    if not self.running:
                        break

                    token_id = market.get("clob_token_id_yes")
                    market_id = market.get("market_id")

                    if not token_id:
                        continue

                    try:
                        book_data = fetch_orderbook(token_id)
                        processed = process_orderbook(book_data)

                        timestamp = int(time.time() * 1000)

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

                    except Exception as e:
                        logger.debug(f"Error polling orderbook for {market_id[:20]}...: {e}")

                    # Small delay between markets
                    await asyncio.sleep(0.1)

                # Wait before next polling round
                await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in orderbook polling: {e}")
                await asyncio.sleep(5)

    async def run(self, markets: Optional[List[Dict[str, Any]]] = None) -> None:
        """
        Run the real-time collector.

        Args:
            markets: Optional list of markets. If None, fetches from database.
        """
        if markets is None:
            markets = get_all_markets()

        if not markets:
            logger.warning("No markets found. Run discovery first.")
            return

        self.markets = markets

        # Extract token IDs to subscribe to
        token_ids = []
        for market in markets:
            if market.get("clob_token_id_yes"):
                token_ids.append(market["clob_token_id_yes"])
            if market.get("clob_token_id_no"):
                token_ids.append(market["clob_token_id_no"])

        if not token_ids:
            logger.warning("No token IDs found to subscribe to")
            return

        logger.info(f"Starting real-time collection for {len(token_ids)} tokens...")
        self.running = True

        # Start orderbook polling if enabled
        if self.enable_orderbook_polling:
            self._orderbook_task = asyncio.create_task(self._poll_orderbooks())

        try:
            while self.running:
                connected = await self.connect()

                if connected:
                    await self.subscribe(token_ids)
                    await self.listen()

                if self.running:
                    logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(
                        self.reconnect_delay * 2,
                        self.max_reconnect_delay
                    )
        finally:
            # Clean up orderbook polling task
            if self._orderbook_task:
                self._orderbook_task.cancel()
                try:
                    await self._orderbook_task
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        """Stop the real-time collector."""
        self.running = False
        flush_all_buffers()
        logger.info(
            f"Stopping real-time collector... "
            f"(received {self.message_count} updates, skipped {self.duplicate_count} duplicates)"
        )


async def run_realtime_collection(
    markets: Optional[List[Dict[str, Any]]] = None,
    enable_orderbook_polling: bool = False,
) -> None:
    """
    Run real-time data collection.

    Args:
        markets: Optional list of markets. If None, fetches from database.
        enable_orderbook_polling: Also poll orderbooks periodically.
    """
    collector = RealtimeCollector(enable_orderbook_polling=enable_orderbook_polling)

    try:
        await collector.run(markets)
    except KeyboardInterrupt:
        collector.stop()
        logger.info("Real-time collection stopped by user")
    except Exception as e:
        logger.error(f"Real-time collection error: {e}")
        collector.stop()

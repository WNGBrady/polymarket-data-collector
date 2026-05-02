"""Real-time data collection module using WebSocket streaming."""

import asyncio
import json
import time
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import OrderedDict
import websockets
from websockets.exceptions import ConnectionClosed

from .config import (
    WEBSOCKET_URL,
    ORDERBOOK_POLL_INTERVAL,
    FAST_ORDERBOOK_POLL_INTERVAL,
    FAST_ORDERBOOK_CONCURRENCY,
    TIER1_CS2_KEYWORDS,
)
from .database import (
    get_all_markets,
    buffer_realtime_price,
    flush_all_buffers,
    insert_orderbook_snapshot,
    get_pinnacle_link,
    upsert_pinnacle_link,
    insert_pinnacle_snapshot,
)
from .historical_collector import fetch_orderbook, process_orderbook
from . import pinnacle
from .utils import logger, safe_float


# How long to leave an `unmatched` link before re-attempting. cs2odds publishes
# matches as Pinnacle's window opens (often 24-48h pre-match), so a market created
# earlier may become matchable later. Without this TTL the linker would never retry.
UNMATCHED_RETRY_TTL_S = 4 * 3600
# Methods accepted by the snapshot writer — anything else (`unmatched`, `outright`,
# `pinnacle-down`) means we have no Pinnacle counterpart for this market.
LINK_METHODS_FOR_SNAPSHOT = ("auto-fuzzy", "manual", "event-inherited")


def compute_fast_tier_market_ids(markets: List[Dict[str, Any]]) -> Set[str]:
    """Map sub-markets of tier-1 CS2 events go on the fast tier; the BO3 parent stays slow.

    Tier-1 keywords typically only appear in the parent question (e.g. "G2 vs FaZe (BO3) -
    BLAST Rivals Playoffs"), not in the per-map sub-markets, so we scan once for tier-1
    event_ids, then promote any 'map'-flagged sub-market in those events.
    """
    tier1_events: Set[str] = set()
    for m in markets:
        if m.get("game") != "cs2":
            continue
        question = (m.get("question") or "").lower()
        if any(kw in question for kw in TIER1_CS2_KEYWORDS):
            ev = m.get("event_id")
            if ev:
                tier1_events.add(str(ev))

    fast_ids: Set[str] = set()
    for m in markets:
        if m.get("game") != "cs2":
            continue
        ev = m.get("event_id")
        if not ev or str(ev) not in tier1_events:
            continue
        if "map" in (m.get("question") or "").lower():
            mid = m.get("market_id")
            if mid:
                fast_ids.add(str(mid))
    return fast_ids


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
        self._orderbook_fast_task: Optional[asyncio.Task] = None
        # Per-bookmaker /matches cache: {bookmaker: (matches, timestamp)}.
        # Keyed by book because matchup→live promotion fans out per book.
        self._pin_matches_caches: Dict[str, Tuple[List[Dict[str, Any]], float]] = {}

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

    async def add_markets(self, new_markets: List[Dict[str, Any]]) -> int:
        """Append newly-discovered markets to the polling list and (if connected)
        subscribe to their token streams.

        Without this, new markets found by the continuous-mode discovery loop
        sit in the database but never get polled — the realtime collector took
        its market snapshot at startup and self.markets is otherwise immutable
        for the lifetime of the run.

        Returns the count of markets actually added (deduped against self.markets
        by market_id).
        """
        if not new_markets:
            return 0

        existing_ids = {m.get("market_id") for m in self.markets}
        additions = [m for m in new_markets if m.get("market_id") not in existing_ids]
        if not additions:
            return 0

        # Build a new list rather than mutating in place so the polling loops'
        # in-flight iterations finish against the old reference and pick up the
        # additions on the next round.
        self.markets = self.markets + additions

        if self.websocket is not None:
            new_token_ids: List[str] = []
            for m in additions:
                for k in ("clob_token_id_yes", "clob_token_id_no"):
                    tok = m.get(k)
                    if tok and tok not in self.subscribed_markets:
                        new_token_ids.append(tok)
            if new_token_ids:
                try:
                    await self.subscribe(new_token_ids)
                except Exception as e:
                    logger.warning(f"add_markets: WS resubscribe failed ({e}); polling will still pick them up")

        logger.info(f"realtime collector: picked up {len(additions)} new markets via discovery")
        return len(additions)

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

    def _snapshot_market(self, market: Dict[str, Any]) -> None:
        """Fetch + persist a single orderbook snapshot. Swallows per-market errors."""
        token_id = market.get("clob_token_id_yes")
        market_id = market.get("market_id")
        if not token_id:
            return
        try:
            book_data = fetch_orderbook(token_id)
            processed = process_orderbook(book_data)
            self._persist_snapshot(market, token_id, processed, int(time.time() * 1000))
        except Exception as e:
            logger.debug(f"Error polling orderbook for {str(market_id)[:20]}...: {e}")

    def _persist_snapshot(self, market: Dict[str, Any], token_id: str, processed: Dict[str, Any], timestamp: int) -> None:
        ob_id = insert_orderbook_snapshot(
            market_id=market.get("market_id"),
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
        # Pinnacle attach is best-effort: any failure (cs2odds down, unmatched
        # market, fuzzy still in flight) leaves the orderbook row unmodified.
        try:
            self._maybe_attach_pinnacle(market, ob_id, timestamp)
        except Exception as e:
            logger.debug(f"pinnacle attach failed for {str(market.get('market_id'))[:20]}...: {e}")

    def _maybe_attach_pinnacle(
        self, market: Dict[str, Any], ob_id: Optional[int], timestamp: int
    ) -> None:
        """If this market is CS2 and has (or can obtain) a pinnacle link, write
        time-aligned pinnacle_snapshots rows tied to the orderbook row — one
        row per linked bookmaker (ps3838, betway, tonybet) when canonical match
        linkage is available, otherwise just the anchor book."""
        if market.get("game") != "cs2":
            return
        market_id = market.get("market_id")
        if not market_id:
            return

        link = get_pinnacle_link(market_id)
        if link is None or self._link_is_stale_unmatched(link):
            link = self._auto_link_market(market)
            if link is None:
                return  # cs2odds unreachable — will retry next snapshot

        method = link.get("link_method")
        if method not in LINK_METHODS_FOR_SNAPSHOT:
            return

        anchor_book = link.get("bookmaker") or "ps3838"
        anchor_match_id = link.get("pin_match_id")
        canonical_id = link.get("canonical_match_id")
        if anchor_match_id is None:
            return

        # Resolve current anchor match_id (matchup -> live promotion). Only
        # meaningful when we don't have a canonical_match_id; with the canonical
        # path the /linked endpoint surfaces both feeds and we just pick live.
        if not canonical_id:
            anchor_match_id = self._maybe_promote_link_to_live(
                market_id, link, anchor_match_id, anchor_book,
            )

        pin_map_num = link.get("pin_map_num") or 0
        link_home = link.get("home_team")
        link_away = link.get("away_team")

        entries = self._collect_book_entries(
            canonical_id=canonical_id,
            anchor_book=anchor_book,
            anchor_match_id=anchor_match_id,
        )
        if not entries:
            return

        for entry in entries:
            self._write_book_snapshot(
                entry=entry,
                ob_id=ob_id,
                market_id=market_id,
                pin_map_num=pin_map_num,
                link_home=link_home,
                link_away=link_away,
                timestamp=timestamp,
            )

    def _collect_book_entries(
        self,
        canonical_id: Optional[str],
        anchor_book: str,
        anchor_match_id: Any,
    ) -> List[Dict[str, Any]]:
        """Return a list of entries shaped like /linked's `entries` items.

        With a canonical_id, prefer the live feed over the matchup feed when
        both books surface the same fixture. Without one, fall back to a
        single-book /odds fetch and synthesize a one-element list.
        """
        if canonical_id:
            linked = pinnacle.fetch_linked(canonical_id)
            if linked is None:
                return []
            entries = linked.get("entries") or []
            # When the same book surfaces both matchup + live for the canonical
            # fixture, drop the matchup. Live is what carries in-game movement.
            by_book: Dict[str, Dict[str, Any]] = {}
            for e in entries:
                book = e.get("bookmaker")
                if not book:
                    continue
                feed = (e.get("match") or {}).get("feed") or ""
                existing = by_book.get(book)
                if existing is None:
                    by_book[book] = e
                    continue
                existing_feed = (existing.get("match") or {}).get("feed") or ""
                if feed == "live" and existing_feed != "live":
                    by_book[book] = e
            return list(by_book.values())

        odds = pinnacle.fetch_match_odds(anchor_book, anchor_match_id)
        if odds is None:
            return []
        return [{
            "bookmaker": anchor_book,
            "match_id": anchor_match_id,
            "match": odds.get("match") or {},
            "periods": odds.get("periods") or {},
            "last_seen_ms": odds.get("last_seen_ms"),
        }]

    def _write_book_snapshot(
        self,
        entry: Dict[str, Any],
        ob_id: Optional[int],
        market_id: str,
        pin_map_num: int,
        link_home: Optional[str],
        link_away: Optional[str],
        timestamp: int,
    ) -> None:
        bookmaker = entry.get("bookmaker") or "ps3838"
        match_id = entry.get("match_id")
        if match_id is None:
            return
        book_match = entry.get("match") or {}
        swap = pinnacle.should_swap_home_away(
            book_match.get("home"), book_match.get("away"), link_home, link_away,
        )
        odds_view = {
            "match": book_match,
            "periods": entry.get("periods") or {},
            "last_seen_ms": entry.get("last_seen_ms"),
        }
        snap = pinnacle.extract_snapshot_for_map(odds_view, pin_map_num, swap_home_away=swap)
        if snap is None:
            return
        try:
            insert_pinnacle_snapshot(
                orderbook_snapshot_id=ob_id,
                market_id=market_id,
                pin_match_id=str(match_id),
                bookmaker=bookmaker,
                timestamp=timestamp,
                **snap,
            )
        except Exception as e:
            logger.debug(f"pinnacle: insert snapshot {bookmaker}/{match_id} failed: {e}")

    def _get_pin_matches_cached(self, bookmaker: str, ttl: float = 30.0) -> Optional[List[Dict[str, Any]]]:
        """List of pinnacle matches for a single book with a short TTL — keeps
        the matchup→live promotion cheap (one /matches call every 30s) when
        many markets are linked to the same event. Cached per bookmaker since
        cs2odds returns a mixed list by default but we promote per-book."""
        now = time.time()
        cached = self._pin_matches_caches.get(bookmaker)
        if cached is not None and (now - cached[1]) < ttl:
            return cached[0]
        try:
            data = pinnacle.list_matches(bookmaker=bookmaker)
            self._pin_matches_caches[bookmaker] = (data, now)
            return data
        except Exception:
            return cached[0] if cached else None  # stale is better than nothing

    def _maybe_promote_link_to_live(
        self, market_id: str, link: Dict[str, Any], pin_match_id: Any, bookmaker: str,
    ) -> Any:
        """If the link points to a 'matchup' (frozen pre-match) feed and a
        'live' child exists for the same book, swap the link to the live
        match_id and persist. Used only when we have no canonical_match_id —
        with canonical, /linked surfaces both feeds and we pick live there.
        """
        matches = self._get_pin_matches_cached(bookmaker)
        if not matches:
            return pin_match_id
        target_id = str(pin_match_id)
        cur = next((m for m in matches if str(m.get("match_id")) == target_id), None)
        if cur is None or (cur.get("feed") or "") == "live":
            return pin_match_id
        live = next(
            (m for m in matches
             if str(m.get("parent_match_id") or "") == target_id and (m.get("feed") or "") == "live"),
            None,
        )
        if live is None:
            return pin_match_id
        new_id = live.get("match_id")
        if new_id is None:
            return pin_match_id
        upsert_pinnacle_link(
            market_id=market_id,
            pin_match_id=new_id,
            pin_map_num=link.get("pin_map_num"),
            home_team=live.get("home"),
            away_team=live.get("away"),
            link_method=link.get("link_method") or "auto-fuzzy",
            confidence=link.get("confidence") or 1.0,
            bookmaker=bookmaker,
            canonical_match_id=live.get("canonical_match_id"),
        )
        logger.info(
            f"pinnacle: promoted market {market_id} {bookmaker}/{pin_match_id} (matchup) -> {new_id} (live)"
        )
        return new_id

    def _link_is_stale_unmatched(self, link: Dict[str, Any]) -> bool:
        """Stop treating `unmatched` as terminal: a market that wasn't in cs2odds
        when first checked may be tracked now (Pinnacle's window opens 24-48h
        pre-match). Re-attempt after UNMATCHED_RETRY_TTL_S."""
        if link.get("link_method") != "unmatched":
            return False
        linked_at = link.get("linked_at") or 0
        return (int(time.time()) - linked_at) >= UNMATCHED_RETRY_TTL_S

    def _auto_link_market(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """One-shot fuzzy-match attempt. Persists the result (or the 'unmatched'
        sentinel) so we don't re-search every snapshot. Returns the link row on
        success, or None if cs2odds is currently unreachable (in which case we
        leave the link unattempted and retry on the next snapshot)."""
        market_id = market.get("market_id")
        method, pin_match, confidence = pinnacle.attempt_link_for_market(market)

        if method == "pinnacle-down":
            return None

        if method == "outright":
            # No-counterpart markets (tournament winners, qualifiers). Don't
            # persist a link row — keeps the table free of permanently-dead rows.
            logger.debug(f"pinnacle: skipping outright '{(market.get('question') or '')[:60]}'")
            return None

        if method == "unmatched":
            upsert_pinnacle_link(
                market_id=market_id,
                pin_match_id=None, pin_map_num=None,
                home_team=None, away_team=None,
                link_method="unmatched", confidence=None,
            )
            logger.info(f"pinnacle: no match for '{(market.get('question') or '')[:60]}'")
            return get_pinnacle_link(market_id)

        # auto-fuzzy hit — fuzzy_match_market prefers ps3838 on ties so the
        # anchor row points at the most reliable book; canonical_match_id (when
        # present) lets the snapshot loop fan out to all linked books.
        map_num = pinnacle.infer_map_num(market.get("question") or "")
        anchor_book = pin_match.get("bookmaker") or "ps3838"
        canonical_id = pin_match.get("canonical_match_id")
        upsert_pinnacle_link(
            market_id=market_id,
            pin_match_id=pin_match.get("match_id"),
            pin_map_num=map_num,
            home_team=pin_match.get("home"),
            away_team=pin_match.get("away"),
            link_method="auto-fuzzy",
            confidence=confidence,
            bookmaker=anchor_book,
            canonical_match_id=canonical_id,
        )
        logger.info(
            f"pinnacle: linked '{(market.get('question') or '')[:50]}' -> "
            f"{anchor_book}/{pin_match.get('match_id')} "
            f"{pin_match.get('home')} vs {pin_match.get('away')} "
            f"(map={map_num}, canon={canonical_id}, conf={confidence:.2f})"
        )
        return get_pinnacle_link(market_id)

    async def _fetch_orderbook_async(self, market: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], str, Dict[str, Any], int]]:
        """Fetch one orderbook off the event loop. Returns a persistable tuple, or None on error/skip."""
        token_id = market.get("clob_token_id_yes")
        market_id = market.get("market_id")
        if not token_id:
            return None
        try:
            book_data = await asyncio.to_thread(fetch_orderbook, token_id)
            processed = process_orderbook(book_data)
            return (market, token_id, processed, int(time.time() * 1000))
        except Exception as e:
            logger.debug(f"Error fetching orderbook for {str(market_id)[:20]}...: {e}")
            return None

    async def _poll_orderbooks(self) -> None:
        """Default-tier orderbook polling. Skips fast-tier CS2 map markets handled by the fast loop."""
        logger.info(f"Starting orderbook polling (every {ORDERBOOK_POLL_INTERVAL}s)...")

        while self.running:
            try:
                fast_ids = compute_fast_tier_market_ids(self.markets)
                for market in self.markets:
                    if not self.running:
                        break
                    if str(market.get("market_id")) in fast_ids:
                        continue
                    self._snapshot_market(market)
                    await asyncio.sleep(0.1)

                await asyncio.sleep(ORDERBOOK_POLL_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in orderbook polling: {e}")
                await asyncio.sleep(5)

    async def _poll_orderbooks_fast(self) -> None:
        """Fast-tier orderbook polling for CS2 map sub-markets in tier-1 events (IEM, ESL Pro League, BLAST, PGL).

        HTTP fetches run in parallel via a thread pool (semaphore-limited concurrency).
        DB inserts stay sequential on the event loop to avoid SQLite contention with the
        slow loop, which shares the same connection without a write lock around inserts.
        """
        logger.info(
            f"Starting fast orderbook polling (every {FAST_ORDERBOOK_POLL_INTERVAL}s, "
            f"concurrency={FAST_ORDERBOOK_CONCURRENCY}) for tier-1 CS2 map markets..."
        )

        sem = asyncio.Semaphore(FAST_ORDERBOOK_CONCURRENCY)

        async def fetch_with_sem(m):
            async with sem:
                if not self.running:
                    return None
                return await self._fetch_orderbook_async(m)

        while self.running:
            try:
                round_start = time.time()
                fast_ids = compute_fast_tier_market_ids(self.markets)
                tier1 = [m for m in self.markets if str(m.get("market_id")) in fast_ids]

                if tier1:
                    results = await asyncio.gather(
                        *(fetch_with_sem(m) for m in tier1),
                        return_exceptions=False,
                    )
                    for r in results:
                        if r is None:
                            continue
                        market, token_id, processed, ts = r
                        try:
                            self._persist_snapshot(market, token_id, processed, ts)
                        except Exception as e:
                            logger.debug(f"Error persisting snapshot {str(market.get('market_id'))[:20]}...: {e}")

                elapsed = time.time() - round_start
                remaining = FAST_ORDERBOOK_POLL_INTERVAL - elapsed
                if remaining > 0:
                    await asyncio.sleep(remaining)
                else:
                    logger.warning(
                        f"Fast orderbook round took {elapsed:.1f}s "
                        f"(>{FAST_ORDERBOOK_POLL_INTERVAL}s target, {len(tier1)} markets)"
                    )
                    await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in fast orderbook polling: {e}")
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
            self._orderbook_fast_task = asyncio.create_task(self._poll_orderbooks_fast())

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
            # Clean up orderbook polling tasks
            for task in (self._orderbook_task, self._orderbook_fast_task):
                if task:
                    task.cancel()
                    try:
                        await task
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

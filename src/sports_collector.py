"""Sports WebSocket collector for detecting match endings and snapshotting final prices."""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

from .config import (
    SPORTS_WS_URL,
    GAME_CONFIGS,
    VALID_GAMES,
    MATCH_SNAPSHOT_DELAY,
    GAMMA_API_URL,
)
from .database import get_markets_by_game_id, insert_final_price, compute_and_store_closing_lines
from .historical_collector import fetch_orderbook, process_orderbook
from .utils import logger, rate_limiter, safe_float, with_retry

import requests


@with_retry
def fetch_market_last_trade_price(market_id: str) -> Optional[float]:
    """Fetch the last trade price for a market from Gamma API."""
    rate_limiter.wait_if_needed("gamma_markets")

    url = f"{GAMMA_API_URL}/markets/{market_id}"
    response = requests.get(url, timeout=30)

    if response.status_code == 404:
        return None

    response.raise_for_status()
    data = response.json()
    return safe_float(data.get("lastTradePrice") or data.get("outcomePrices"))


class SportsCollector:
    """
    Connects to the Polymarket sports WebSocket to detect match endings
    and snapshot final market prices.
    """

    def __init__(self, games: Optional[List[str]] = None):
        self.games = games or VALID_GAMES
        self.websocket = None
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self._snapshotted: Set[str] = set()  # game_ids already snapshotted

        # Build set of league abbreviations we care about
        self._league_abbrevs: Set[str] = set()
        for game in self.games:
            config = GAME_CONFIGS.get(game, {})
            for abbrev in config.get("league_abbreviations", []):
                self._league_abbrevs.add(abbrev.lower())

    async def connect(self) -> bool:
        """Establish WebSocket connection to sports API."""
        try:
            self.websocket = await websockets.connect(
                SPORTS_WS_URL,
                ping_interval=5,
                ping_timeout=10,
            )
            logger.info(f"Sports WebSocket connected to {SPORTS_WS_URL}")
            self.reconnect_delay = 1
            return True
        except Exception as e:
            logger.error(f"Sports WebSocket connection failed: {e}")
            return False

    def _is_relevant_message(self, data: Dict[str, Any]) -> bool:
        """Check if a sports WS message is for one of our monitored games."""
        league = (data.get("leagueAbbreviation") or "").lower()
        return league in self._league_abbrevs

    def _is_match_ended(self, data: Dict[str, Any]) -> bool:
        """Check if the message indicates a match has ended."""
        return data.get("ended", False) is True

    async def _snapshot_market(
        self,
        market: Dict[str, Any],
        game_id: str,
        match_data: Dict[str, Any],
    ) -> None:
        """Snapshot a single market's final prices."""
        market_id = market.get("market_id")
        token_id = market.get("clob_token_id_yes")
        game = market.get("game", "cod")

        # Fetch orderbook for bid/ask/spread/mid
        best_bid = None
        best_ask = None
        mid_price = None
        spread = None

        if token_id:
            try:
                book_data = fetch_orderbook(token_id)
                processed = process_orderbook(book_data)
                best_bid = processed.get("best_bid_price")
                best_ask = processed.get("best_ask_price")
                mid_price = processed.get("mid_price")
                spread = processed.get("spread")
            except Exception as e:
                logger.warning(f"  Could not fetch orderbook for {market_id}: {e}")

        # Fetch last trade price from Gamma API
        last_trade_price = None
        try:
            last_trade_price = fetch_market_last_trade_price(market_id)
        except Exception as e:
            logger.warning(f"  Could not fetch last trade price for {market_id}: {e}")

        # Extract match metadata
        match_ended_at = match_data.get("finishedTimestamp") or datetime.now(timezone.utc).isoformat()
        snapshot_taken_at = datetime.now(timezone.utc).isoformat()

        score = match_data.get("score")
        final_score = json.dumps(score) if score else None

        insert_final_price(
            market_id=market_id,
            game=game,
            game_id=game_id,
            match_ended_at=str(match_ended_at),
            snapshot_taken_at=snapshot_taken_at,
            last_trade_price=last_trade_price,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid_price,
            spread=spread,
            home_team=match_data.get("homeTeam"),
            away_team=match_data.get("awayTeam"),
            final_score=final_score,
            match_period=match_data.get("period"),
        )

        question = market.get("question", "Unknown")[:60]
        logger.info(f"  Snapshotted final price for: {question}")

    async def _handle_match_end(self, data: Dict[str, Any]) -> None:
        """Handle a match-end event: look up markets, wait, then snapshot."""
        game_id = data.get("gameId")
        if not game_id:
            logger.warning("Match ended but no gameId in message")
            return

        if game_id in self._snapshotted:
            logger.debug(f"Game {game_id} already snapshotted, skipping")
            return

        self._snapshotted.add(game_id)

        home = data.get("homeTeam", "?")
        away = data.get("awayTeam", "?")
        league = data.get("leagueAbbreviation", "?")
        logger.info(f"Match ended: [{league.upper()}] {home} vs {away} (gameId={game_id})")

        # Look up associated markets
        markets = get_markets_by_game_id(game_id)
        if not markets:
            logger.warning(f"  No markets mapped to gameId={game_id}")
            return

        logger.info(f"  Found {len(markets)} markets, waiting {MATCH_SNAPSHOT_DELAY}s before snapshot...")
        await asyncio.sleep(MATCH_SNAPSHOT_DELAY)

        for market in markets:
            try:
                await self._snapshot_market(market, game_id, data)
            except Exception as e:
                logger.error(f"  Error snapshotting market {market.get('market_id')}: {e}")

        # Compute and store closing lines
        try:
            n = compute_and_store_closing_lines(game_id, data)
            logger.info(f"  Computed {n} closing lines for gameId={game_id}")
        except Exception as e:
            logger.error(f"  Error computing closing lines for {game_id}: {e}")

    async def listen(self) -> None:
        """Listen for incoming sports WS messages."""
        if not self.websocket:
            logger.error("Cannot listen - not connected")
            return

        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                # Handle both single messages and arrays
                messages = data if isinstance(data, list) else [data]

                for msg in messages:
                    if not isinstance(msg, dict):
                        continue

                    if not self._is_relevant_message(msg):
                        continue

                    if self._is_match_ended(msg):
                        # Run snapshot as background task to not block listener
                        asyncio.create_task(self._handle_match_end(msg))

        except ConnectionClosed as e:
            logger.warning(f"Sports WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"Error in sports WebSocket listener: {e}")

    async def run(self) -> None:
        """Run the sports collector with reconnection logic."""
        logger.info(f"Starting sports collector for games: {', '.join(g.upper() for g in self.games)}")
        logger.info(f"Monitoring leagues: {', '.join(sorted(self._league_abbrevs))}")
        self.running = True

        try:
            while self.running:
                connected = await self.connect()

                if connected:
                    await self.listen()

                if self.running:
                    logger.info(f"Sports WS reconnecting in {self.reconnect_delay}s...")
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(
                        self.reconnect_delay * 2,
                        self.max_reconnect_delay,
                    )
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        """Stop the sports collector."""
        self.running = False
        logger.info(f"Sports collector stopped ({len(self._snapshotted)} matches snapshotted)")


async def run_sports_collection(games: Optional[List[str]] = None) -> None:
    """
    Run sports WebSocket collection.

    Args:
        games: List of game keys to monitor. None means all games.
    """
    collector = SportsCollector(games=games)
    try:
        await collector.run()
    except KeyboardInterrupt:
        collector.stop()
    except Exception as e:
        logger.error(f"Sports collection error: {e}")
        collector.stop()

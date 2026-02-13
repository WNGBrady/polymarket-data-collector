#!/usr/bin/env python3
"""
Polymarket Esports Data Collector

A tool for collecting and storing data from Polymarket's API for esports
prediction markets (Call of Duty, Counter-Strike 2), including historical
backfill, real-time streaming, final price snapshots, and open interest.
"""

import argparse
import asyncio
import signal
import sys
import time

from src.database import init_database, get_stats, maybe_rename_legacy_db, migrate_database, close_connection, flush_all_buffers
from src.market_discovery import run_discovery, list_stored_markets, get_available_tags
from src.historical_collector import (
    run_historical_collection,
    run_orderbook_collection,
    run_open_interest_collection,
)
from src.realtime_collector import run_realtime_collection
from src.sports_collector import SportsCollector, run_sports_collection
from src.config import DISCOVERY_INTERVAL, ORDERBOOK_POLL_INTERVAL, VALID_GAMES
from src.utils import logger


def _resolve_games(args) -> list:
    """Convert the --game argument to a list of game keys."""
    game = getattr(args, 'game', 'all')
    if game == 'all':
        return list(VALID_GAMES)
    return [game]


def cmd_discover(args):
    """Run market discovery."""
    games = _resolve_games(args)
    logger.info(f"Starting market discovery for: {', '.join(g.upper() for g in games)}...")
    include_closed = getattr(args, 'include_closed', False)
    markets = run_discovery(games=games, include_closed=include_closed)

    print(f"\nDiscovered {len(markets)} markets:")
    for market in markets:
        game_label = f"[{market.get('game', 'cod').upper()}]"
        print(f"  {game_label} {market.get('question', 'Unknown')[:65]}...")

    return 0


def cmd_historical(args):
    """Run historical data collection."""
    games = _resolve_games(args)
    markets = []
    for g in games:
        markets.extend(list_stored_markets(game=g))

    if not markets:
        logger.error("No markets found in database. Run --discover first.")
        return 1

    logger.info(f"Starting historical collection for {len(markets)} markets...")
    results = run_historical_collection(markets)

    print(f"\nHistorical collection complete:")
    print(f"  - Price records inserted: {results['prices']}")
    print(f"  - Trade records inserted: {results['trades']}")
    print(f"  - Open interest snapshots: {results['open_interest']}")

    return 0


def cmd_realtime(args):
    """Run real-time data collection."""
    games = _resolve_games(args)
    markets = []
    for g in games:
        markets.extend(list_stored_markets(game=g))

    if not markets:
        logger.error("No markets found in database. Run --discover first.")
        return 1

    enable_orderbook = getattr(args, 'with_orderbook', False)

    logger.info(f"Starting real-time collection for {len(markets)} markets...")
    if enable_orderbook:
        logger.info(f"Orderbook polling enabled (every {ORDERBOOK_POLL_INTERVAL}s)")
    print("Press Ctrl+C to stop...")

    try:
        asyncio.run(run_realtime_collection(markets, enable_orderbook_polling=enable_orderbook))
    except KeyboardInterrupt:
        print("\nStopped by user")

    return 0


def cmd_orderbook(args):
    """Collect orderbook snapshots."""
    games = _resolve_games(args)
    markets = []
    for g in games:
        markets.extend(list_stored_markets(game=g))

    if not markets:
        logger.error("No markets found in database. Run --discover first.")
        return 1

    logger.info(f"Collecting orderbook snapshots for {len(markets)} markets...")
    total = run_orderbook_collection(markets)

    print(f"\nOrderbook collection complete:")
    print(f"  - Snapshots collected: {total}")

    return 0


def cmd_discover_tags(args):
    """Show available tags for filtering."""
    logger.info("Fetching available tags...")

    tags = get_available_tags()

    if not tags:
        print("No tags found or error fetching tags.")
        return 1

    print(f"\nAvailable tags ({len(tags)}):")
    for tag in tags:
        tag_id = tag.get('id', 'N/A')
        label = tag.get('label', 'Unknown')
        slug = tag.get('slug', '')
        print(f"  - {label} (id: {tag_id}, slug: {slug})")

    return 0


def cmd_sports_ws(args):
    """Run sports WebSocket listener for final price snapshots."""
    games = _resolve_games(args)

    logger.info(f"Starting sports WebSocket for: {', '.join(g.upper() for g in games)}...")
    print("Listening for match endings. Press Ctrl+C to stop...")

    try:
        asyncio.run(run_sports_collection(games=games))
    except KeyboardInterrupt:
        print("\nStopped by user")

    return 0


def cmd_open_interest(args):
    """Collect open interest snapshots."""
    games = _resolve_games(args)
    markets = []
    for g in games:
        markets.extend(list_stored_markets(game=g))

    if not markets:
        logger.error("No markets found in database. Run --discover first.")
        return 1

    logger.info(f"Collecting open interest for {len(markets)} markets...")
    total = run_open_interest_collection(markets)

    print(f"\nOpen interest collection complete:")
    print(f"  - Snapshots collected: {total}")

    return 0


async def run_continuous_mode(args):
    """
    Run in continuous mode for tournament weekends.

    This mode:
    - Runs market discovery periodically
    - Streams real-time prices via WebSocket
    - Polls orderbooks periodically
    - Listens to sports WebSocket for match-end snapshots
    - Backfills historical data for new markets
    """
    from src.realtime_collector import RealtimeCollector

    games = _resolve_games(args)

    logger.info("Starting continuous mode...")
    logger.info(f"  - Games: {', '.join(g.upper() for g in games)}")
    logger.info(f"  - Discovery interval: {DISCOVERY_INTERVAL}s")
    logger.info(f"  - Orderbook polling: {ORDERBOOK_POLL_INTERVAL}s")
    print("Press Ctrl+C to stop...")

    shutdown_event = asyncio.Event()

    def handle_shutdown():
        logger.info("Shutdown signal received...")
        shutdown_event.set()

    # Set up signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    # Initial discovery
    include_closed = getattr(args, 'include_closed', False)
    markets = run_discovery(games=games, include_closed=include_closed)

    if not markets:
        logger.warning("No markets found. Will retry discovery...")
        markets = []

    # Start real-time collector with orderbook polling
    collector = RealtimeCollector(enable_orderbook_polling=True)

    # Start sports WebSocket collector
    sports = SportsCollector(games=games)

    last_discovery_time = time.time()

    async def discovery_loop():
        """Periodically run market discovery."""
        nonlocal markets, last_discovery_time

        while not shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Check every minute

                elapsed = time.time() - last_discovery_time
                if elapsed >= DISCOVERY_INTERVAL:
                    logger.info("Running periodic market discovery...")
                    new_markets = run_discovery(games=games, include_closed=include_closed)

                    # Check for new markets
                    existing_ids = {m.get('market_id') for m in markets}
                    new_found = [m for m in new_markets if m.get('market_id') not in existing_ids]

                    if new_found:
                        logger.info(f"Found {len(new_found)} new markets, backfilling historical data...")
                        run_historical_collection(new_found)
                        markets = new_markets

                    last_discovery_time = time.time()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in discovery loop: {e}")

    # Run historical backfill for existing markets
    if markets:
        logger.info("Running initial historical backfill...")
        run_historical_collection(markets)

    # Start tasks
    discovery_task = asyncio.create_task(discovery_loop())
    sports_task = asyncio.create_task(sports.run())

    try:
        # Run the realtime collector (this blocks until stopped)
        await collector.run(markets if markets else None)
    except Exception as e:
        logger.error(f"Error in continuous mode: {e}")
    finally:
        collector.stop()
        sports.stop()
        discovery_task.cancel()
        sports_task.cancel()
        try:
            await discovery_task
        except asyncio.CancelledError:
            pass
        try:
            await sports_task
        except asyncio.CancelledError:
            pass

    logger.info("Continuous mode stopped.")


def cmd_continuous(args):
    """Run in continuous mode for tournament weekends."""
    try:
        asyncio.run(run_continuous_mode(args))
    except KeyboardInterrupt:
        print("\nStopped by user")

    return 0


def cmd_stats(args):
    """Show database statistics."""
    stats = get_stats()

    print("\nDatabase Statistics:")
    print(f"  - Markets: {stats['markets']}")

    # Per-game breakdown
    markets_by_game = stats.get("markets_by_game", {})
    if markets_by_game:
        for game, count in sorted(markets_by_game.items()):
            print(f"    - {game.upper()}: {count}")

    print(f"  - Price history records: {stats['price_history_records']}")
    print(f"  - Trade records: {stats['trade_records']}")
    print(f"  - Real-time price records: {stats['realtime_price_records']}")
    print(f"  - Orderbook snapshots: {stats['orderbook_snapshots']}")
    print(f"  - Final price snapshots: {stats['final_price_snapshots']}")
    print(f"  - Open interest records: {stats['open_interest_records']}")

    return 0


def cmd_list(args):
    """List stored markets."""
    games = _resolve_games(args)
    markets = []
    for g in games:
        markets.extend(list_stored_markets(game=g))

    if not markets:
        print("No markets stored. Run --discover first.")
        return 0

    print(f"\nStored markets ({len(markets)}):")
    for market in markets:
        game_label = f"[{market.get('game', 'cod').upper()}]"
        print(f"\n  {game_label} ID: {market.get('market_id', 'N/A')[:30]}...")
        print(f"  Question: {market.get('question', 'N/A')[:60]}...")
        print(f"  Condition ID: {market.get('condition_id', 'N/A')[:30]}...")
        print(f"  Token YES: {market.get('clob_token_id_yes', 'N/A')[:30]}...")

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Polymarket Esports Data Collector (COD + CS2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --discover                  # Find markets for all games
  python main.py --discover --game cs2       # Find CS2 markets only
  python main.py --discover --game cod       # Find COD markets only
  python main.py --historical                # Backfill price, trade, and OI history
  python main.py --historical --game cs2     # Backfill CS2 markets only
  python main.py --realtime                  # Start live price streaming
  python main.py --orderbook                 # Collect orderbook snapshots
  python main.py --sports-ws                 # Listen for match endings + snapshot prices
  python main.py --open-interest             # Collect open interest snapshots
  python main.py --continuous                # Tournament mode (all collectors)
  python main.py --stats                     # Show database statistics
  python main.py --list --game cs2           # List stored CS2 markets
        """
    )

    parser.add_argument(
        "--game",
        choices=["all", "cod", "cs2"],
        default="all",
        help="Game to target (default: all)"
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="Discover and store esports markets"
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Collect historical price, trade, and open interest data"
    )
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="Start real-time price collection via WebSocket"
    )
    parser.add_argument(
        "--orderbook",
        action="store_true",
        help="Collect orderbook snapshots for all markets"
    )
    parser.add_argument(
        "--sports-ws",
        action="store_true",
        help="Start sports WebSocket listener for final price snapshots"
    )
    parser.add_argument(
        "--open-interest",
        action="store_true",
        help="Collect open interest snapshots"
    )
    parser.add_argument(
        "--discover-tags",
        action="store_true",
        help="Show available tags for filtering"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run in continuous mode (discovery + realtime + sports WS + orderbooks)"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List stored markets"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run discover + historical collection"
    )
    parser.add_argument(
        "--include-closed",
        action="store_true",
        help="Include closed/resolved markets in discovery"
    )
    parser.add_argument(
        "--with-orderbook",
        action="store_true",
        help="Enable orderbook polling during realtime collection"
    )

    args = parser.parse_args()

    # Database initialization: rename legacy DB, migrate existing schema, then init
    logger.info("Initializing database...")
    maybe_rename_legacy_db()
    migrate_database()
    init_database()

    # Check if any command was specified
    commands = [
        args.discover, args.historical, args.realtime, args.stats,
        args.list, args.all, args.orderbook, args.discover_tags,
        args.continuous, args.sports_ws, args.open_interest,
    ]
    if not any(commands):
        parser.print_help()
        return 1

    # Run requested commands (with clean shutdown of DB connection)
    try:
        if args.continuous:
            return cmd_continuous(args)

        if args.all:
            cmd_discover(args)
            cmd_historical(args)
            return 0

        if args.discover:
            return cmd_discover(args)

        if args.discover_tags:
            return cmd_discover_tags(args)

        if args.historical:
            return cmd_historical(args)

        if args.orderbook:
            return cmd_orderbook(args)

        if args.realtime:
            return cmd_realtime(args)

        if args.sports_ws:
            return cmd_sports_ws(args)

        if args.open_interest:
            return cmd_open_interest(args)

        if args.stats:
            return cmd_stats(args)

        if args.list:
            return cmd_list(args)

        return 0
    finally:
        flush_all_buffers()
        close_connection()


if __name__ == "__main__":
    sys.exit(main())

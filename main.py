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
from src.config import (
    DISCOVERY_INTERVAL, ORDERBOOK_POLL_INTERVAL, VALID_GAMES,
    TRADING_ENABLED, SIGNAL_SCAN_INTERVAL, ORDER_CHECK_INTERVAL,
    SPORTSBOOK_ENABLED,
)
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


def cmd_verify_sportsbook(args):
    """Verify BoltOdds esports coverage."""
    from src.sportsbook.client import verify_esports_coverage, boltodds_get_games

    print("\nVerifying BoltOdds esports coverage...\n")
    result = verify_esports_coverage()

    if "error" in result and result["error"]:
        print(f"  Error: {result['error']}")
        return 1

    sports = result.get("sports", [])
    print(f"  Available sports ({len(sports)}):")
    for s in sorted(sports):
        print(f"    - {s}")

    print(f"\n  COD / Call of Duty: {'YES' if result.get('cod') else 'NO'}")
    print(f"  CS2 / Counter-Strike: {'YES' if result.get('cs2') else 'NO'}")

    # Try to fetch games for esports-like sport keys
    for sport_key in sports:
        s_lower = sport_key.lower()
        if any(kw in s_lower for kw in ["esport", "cod", "cs2", "csgo", "counter"]):
            print(f"\n  Fetching games for '{sport_key}'...")
            games = boltodds_get_games(sport_key)
            if games:
                print(f"    Found {len(games)} games")
                for g in games[:5]:
                    if isinstance(g, dict):
                        print(f"      {g.get('game', g.get('name', str(g)[:80]))}")
                    else:
                        print(f"      {str(g)[:80]}")
            else:
                print("    No games found")

    if not result.get("cod") and not result.get("cs2"):
        print("\n  WARNING: No esports coverage detected.")
        print("  Consider alternatives: The Odds API, OddsJam, or manual Pinnacle entry.")

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


async def trading_loop(games: list, shutdown_event: asyncio.Event):
    """Periodically scan for signals and execute trades.

    - Every SIGNAL_SCAN_INTERVAL: scan for signals and execute
    - Every ORDER_CHECK_INTERVAL: check open order status
    """
    from src.trading.signals import scan_for_signals
    from src.trading.executor import execute_signals, check_open_orders

    last_order_check = time.time()

    while not shutdown_event.is_set():
        try:
            # Scan for signals
            signals = scan_for_signals(games)
            if signals:
                logger.info(f"Trading: {len(signals)} signals detected, executing...")
                results = execute_signals(signals)
                placed = sum(1 for r in results if r.success)
                logger.info(f"Trading: {placed}/{len(results)} orders placed")

            # Periodically check open orders
            if time.time() - last_order_check >= ORDER_CHECK_INTERVAL:
                check_open_orders()
                last_order_check = time.time()

        except Exception as e:
            logger.error(f"Error in trading loop: {e}")

        # Wait for next scan interval
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=SIGNAL_SCAN_INTERVAL)
            break  # shutdown_event was set
        except asyncio.TimeoutError:
            pass  # Normal timeout — continue loop


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

    # Determine trading state early (needed by sportsbook setup)
    enable_trading = getattr(args, 'trading', False) and TRADING_ENABLED

    # Start BoltOdds sportsbook feed if enabled
    boltodds_client = None
    boltodds_task = None
    if SPORTSBOOK_ENABLED:
        from src.sportsbook.client import BoltOddsClient, SportsBookCache
        sportsbook_cache = SportsBookCache()
        boltodds_client = BoltOddsClient(sportsbook_cache)

        # Wire the cache into the signal scanner
        import src.trading.signals as _signals_mod
        _signals_mod._sportsbook_cache = sportsbook_cache

        # Set bankroll from balance if trading is active, else default
        try:
            if enable_trading:
                from src.trading import client as _tc
                bal = _tc.get_balance()
                _signals_mod._sportsbook_bankroll = bal
                logger.info(f"Sportsbook bankroll set to ${bal:.2f}")
        except Exception:
            pass

        logger.info("Sportsbook EV engine enabled — starting BoltOdds feed")

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

    if boltodds_client:
        boltodds_task = asyncio.create_task(boltodds_client.run())

    # Optionally start trading loop
    trading_task = None
    if enable_trading:
        logger.info("Trading engine enabled — starting signal scanner")
        trading_task = asyncio.create_task(trading_loop(games, shutdown_event))

    try:
        # Run the realtime collector (this blocks until stopped)
        await collector.run(markets if markets else None)
    except Exception as e:
        logger.error(f"Error in continuous mode: {e}")
    finally:
        collector.stop()
        sports.stop()
        if boltodds_client:
            boltodds_client.stop()
        discovery_task.cancel()
        sports_task.cancel()
        if boltodds_task:
            boltodds_task.cancel()
        if trading_task:
            trading_task.cancel()
        try:
            await discovery_task
        except asyncio.CancelledError:
            pass
        try:
            await sports_task
        except asyncio.CancelledError:
            pass
        if boltodds_task:
            try:
                await boltodds_task
            except asyncio.CancelledError:
                pass
        if trading_task:
            try:
                await trading_task
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


def cmd_trading(args):
    """Run standalone trading engine (signal scan + execution only)."""
    if not TRADING_ENABLED:
        logger.error("Trading is disabled. Set TRADING_ENABLED=true environment variable.")
        return 1

    games = _resolve_games(args)
    logger.info(f"Starting standalone trading engine for: {', '.join(g.upper() for g in games)}")
    logger.info(f"  - Signal scan interval: {SIGNAL_SCAN_INTERVAL}s")
    logger.info(f"  - Order check interval: {ORDER_CHECK_INTERVAL}s")
    print("Press Ctrl+C to stop...")

    shutdown_event = asyncio.Event()

    async def _run():
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except NotImplementedError:
                pass
        await trading_loop(games, shutdown_event)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("\nTrading engine stopped by user")

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


def cmd_pinnacle_status(args):
    """Show pinnacle link coverage across CS2 markets."""
    from src.database import get_pinnacle_link_summary

    summary = get_pinnacle_link_summary(game="cs2")
    linked = summary["linked"]
    unmatched = summary["unmatched"]
    unattempted = summary["unattempted"]

    print(f"\nPinnacle link summary (CS2 markets):")
    print(f"  Linked:      {len(linked)}")
    print(f"  Unmatched:   {len(unmatched)}  (auto-fuzzy gave up; use --link-pinnacle to override)")
    print(f"  Unattempted: {len(unattempted)} (no pinnacle fetch yet — collector will try on next snapshot)")

    if linked:
        print("\nLinked markets:")
        for r in linked[:20]:
            print(
                f"  [{r['link_method']:10s} {r['confidence']:.2f}] "
                f"{(r['question'] or '')[:55]} -> "
                f"{r['home_team']} vs {r['away_team']} (match_id={r['pin_match_id']}, map={r['pin_map_num']})"
            )
        if len(linked) > 20:
            print(f"  ... +{len(linked) - 20} more")

    if unmatched:
        print("\nUnmatched markets (manual override candidates):")
        for r in unmatched[:20]:
            print(f"  market_id={r['market_id']}  q={(r['question'] or '')[:60]}")
        if len(unmatched) > 20:
            print(f"  ... +{len(unmatched) - 20} more")

    return 0


def cmd_link_pinnacle(args):
    """Manually attach a polymarket market to a pinnacle match/map."""
    from src.database import upsert_pinnacle_link
    from src import pinnacle as pin

    if not args.market_id or args.pin_match_id is None or args.pin_map_num is None:
        logger.error("--link-pinnacle requires --market-id, --pin-match-id, and --pin-map-num")
        return 1

    # Look up the home/away from cs2odds for the audit trail
    home_team = away_team = None
    odds = pin.fetch_match_odds(args.pin_match_id)
    if odds:
        meta = odds.get("match") or {}
        home_team = meta.get("home")
        away_team = meta.get("away")
    else:
        logger.warning(f"cs2odds doesn't currently know match_id={args.pin_match_id}; saving link anyway")

    upsert_pinnacle_link(
        market_id=args.market_id,
        pin_match_id=args.pin_match_id,
        pin_map_num=args.pin_map_num,
        home_team=home_team,
        away_team=away_team,
        link_method="manual",
        confidence=1.0,
    )
    print(
        f"Linked market_id={args.market_id} -> "
        f"pin_match_id={args.pin_match_id} map={args.pin_map_num} "
        f"({home_team} vs {away_team})"
    )
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
  python main.py --continuous --trading      # Tournament mode + automated trading
  python main.py --trading --game cod        # Standalone trading engine
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
        "--trading",
        action="store_true",
        help="Enable automated trading (requires TRADING_ENABLED=true env var)"
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
    parser.add_argument(
        "--verify-sportsbook",
        action="store_true",
        help="Check BoltOdds API for esports coverage (COD/CS2)"
    )
    parser.add_argument(
        "--pinnacle-status",
        action="store_true",
        help="Show pinnacle link coverage across CS2 markets"
    )
    parser.add_argument(
        "--link-pinnacle",
        action="store_true",
        help="Manually link a polymarket market to a pinnacle match (use with --market-id, --pin-match-id, --pin-map-num)"
    )
    parser.add_argument("--market-id", type=str, default=None)
    parser.add_argument("--pin-match-id", type=int, default=None)
    parser.add_argument("--pin-map-num", type=int, default=None)

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
        args.trading, args.verify_sportsbook,
        args.pinnacle_status, args.link_pinnacle,
    ]
    if not any(commands):
        parser.print_help()
        return 1

    # Run requested commands (with clean shutdown of DB connection)
    try:
        if args.verify_sportsbook:
            return cmd_verify_sportsbook(args)

        if args.continuous:
            return cmd_continuous(args)

        if args.trading and not args.continuous:
            return cmd_trading(args)

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

        if args.pinnacle_status:
            return cmd_pinnacle_status(args)

        if args.link_pinnacle:
            return cmd_link_pinnacle(args)

        return 0
    finally:
        flush_all_buffers()
        close_connection()


if __name__ == "__main__":
    sys.exit(main())

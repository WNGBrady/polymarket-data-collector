"""Aggregate per-trade rows into the wallets table.

Most fields are pure SQL group-bys off `trades`. A handful (cadence CV, round-size
share, cross-market burst) need per-wallet trade timestamps, so we stream them in
batches keyed on proxy_wallet.
"""

import json
import math
import statistics
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

from .config import BOT_HEURISTICS
from .database import get_connection
from .utils import logger


def _round_size_share(sizes: Iterable[float]) -> float:
    """Fraction of trades whose size matches one of the configured round bucket values."""
    rounds = BOT_HEURISTICS["round_sizes"]
    tol = BOT_HEURISTICS["round_size_tolerance"]
    n = 0
    rnd = 0
    for s in sizes:
        if s is None or s <= 0:
            continue
        n += 1
        if any(abs(s - r) <= tol for r in rounds):
            rnd += 1
    return rnd / n if n else 0.0


def _cv(values):
    """Coefficient of variation = stdev/mean. Returns 0 for tiny samples."""
    vals = [v for v in values if v is not None and v > 0]
    if len(vals) < 2:
        return 0.0
    mean = statistics.mean(vals)
    if mean <= 0:
        return 0.0
    sd = statistics.pstdev(vals)
    return sd / mean


def _cross_market_bursts(trades) -> int:
    """Count windows where ≥ N distinct markets were traded within W seconds.

    `trades` is an iterable of (timestamp, market_id) sorted ascending.
    """
    window = BOT_HEURISTICS["cross_market_burst_window_s"]
    distinct_min = BOT_HEURISTICS["cross_market_burst_distinct"]
    trades = list(trades)
    if len(trades) < distinct_min:
        return 0

    bursts = 0
    j = 0
    for i in range(len(trades)):
        # Advance j to the start of the window
        while trades[i][0] - trades[j][0] > window:
            j += 1
        markets_in_window = {m for _, m in trades[j : i + 1]}
        if len(markets_in_window) >= distinct_min:
            bursts += 1
    return bursts


def _two_sided_ratio(market_side_counts) -> float:
    """Average of min(buy,sell)/max(buy,sell) across markets with ≥ N trades.

    `market_side_counts` is a dict {market_id: {'BUY': n, 'SELL': n}}.
    """
    min_n = BOT_HEURISTICS["two_sided_min_trades"]
    ratios = []
    for sides in market_side_counts.values():
        b = sides.get("BUY", 0)
        s = sides.get("SELL", 0)
        if b + s < min_n:
            continue
        denom = max(b, s)
        if denom == 0:
            continue
        ratios.append(min(b, s) / denom)
    return sum(ratios) / len(ratios) if ratios else 0.0


def _aggregate_one_wallet(rows) -> dict:
    """Aggregate a list of trade rows for a single wallet into the wallets schema."""
    rows = list(rows)
    if not rows:
        return {}

    # Sort ascending by ts so cadence + bursts are correct
    rows.sort(key=lambda r: r["timestamp"])

    sizes = [r["size"] or 0.0 for r in rows]
    timestamps = [r["timestamp"] for r in rows]
    inter_trade = [t2 - t1 for t1, t2 in zip(timestamps, timestamps[1:])]

    games = {}
    market_ids = set()
    market_side_counts: dict[str, dict[str, int]] = {}
    buy = sell = 0
    hours_utc = set()
    days_utc = set()
    night_hours = set(BOT_HEURISTICS["night_hours_utc"])
    night_count = 0
    pseudonym = None
    name = None

    for r in rows:
        g = r["game"] or "unknown"
        gd = games.setdefault(g, {"trades": 0, "volume": 0.0})
        gd["trades"] += 1
        gd["volume"] += r["size"] or 0.0

        market_ids.add(r["market_id"])
        side = (r["side"] or "").upper()
        if side == "BUY":
            buy += 1
        elif side == "SELL":
            sell += 1
        market_side_counts.setdefault(r["market_id"], {}).setdefault(side, 0)
        market_side_counts[r["market_id"]][side] += 1

        ts = r["timestamp"]
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        hours_utc.add(dt.hour)
        days_utc.add(dt.date())
        if dt.hour in night_hours:
            night_count += 1

        # Most-recent non-null pseudonym / name wins
        if r["pseudonym"]:
            pseudonym = r["pseudonym"]
        if r["name"]:
            name = r["name"]

    total_trades = len(rows)
    total_volume = sum(sizes)
    median_size = statistics.median(sizes) if sizes else 0.0
    median_inter = statistics.median(inter_trade) if inter_trade else 0.0
    cv_size = _cv(sizes)
    cv_inter = _cv(inter_trade)
    bursts = _cross_market_bursts((r["timestamp"], r["market_id"]) for r in rows)
    two_sided = _two_sided_ratio(market_side_counts)
    round_share = _round_size_share(sizes)
    night_share = (night_count / total_trades) if total_trades else 0.0
    active_days = len(days_utc)
    markets_per_day = (len(market_ids) / active_days) if active_days else 0.0
    days_per_week = min(7.0, active_days / max(1.0, (timestamps[-1] - timestamps[0]) / 86400.0 / 7.0)) if active_days > 1 else (1.0 if active_days else 0.0)

    return {
        "proxy_wallet": rows[0]["proxy_wallet"],
        "pseudonym": pseudonym,
        "name": name,
        "first_seen_ts": timestamps[0],
        "last_seen_ts": timestamps[-1],
        "total_trades": total_trades,
        "total_volume_usd": total_volume,
        "distinct_markets": len(market_ids),
        "distinct_games": len(games),
        "games_json": json.dumps(games, sort_keys=True),
        "buy_count": buy,
        "sell_count": sell,
        "median_trade_size": median_size,
        "trade_size_cv": cv_size,
        "median_inter_trade_s": median_inter,
        "inter_trade_cv": cv_inter,
        "active_hours": len(hours_utc),
        "active_days_per_week": days_per_week,
        "round_size_share": round_share,
        "night_share": night_share,
        "cross_market_burst": bursts,
        "markets_per_day": markets_per_day,
        "two_sided_ratio": two_sided,
    }


def _iter_wallet_groups(cursor, since_ts: Optional[int], game: Optional[str]):
    """Yield (proxy_wallet, [trade rows]) tuples streamed from the DB.

    Filters out trades without a wallet — they predate the schema migration.
    """
    where = ["t.proxy_wallet IS NOT NULL", "t.proxy_wallet != ''"]
    params: list = []
    if since_ts is not None:
        where.append("t.timestamp >= ?")
        params.append(since_ts)
    if game and game != "all":
        where.append("m.game = ?")
        params.append(game)

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT t.proxy_wallet, t.market_id, t.timestamp, t.price, t.size, t.side,
               t.outcome, t.outcome_index, t.asset, t.pseudonym, t.name,
               COALESCE(m.game, 'unknown') AS game
        FROM trades t
        LEFT JOIN markets m ON t.market_id = m.market_id
        WHERE {where_sql}
        ORDER BY t.proxy_wallet
    """

    current_wallet = None
    bucket: list = []
    for row in cursor.execute(sql, params):
        d = dict(row)
        if d["proxy_wallet"] != current_wallet:
            if bucket:
                yield current_wallet, bucket
            current_wallet = d["proxy_wallet"]
            bucket = []
        bucket.append(d)
    if bucket:
        yield current_wallet, bucket


def recompute_wallets(game: Optional[str] = None, since_ts: Optional[int] = None) -> int:
    """Aggregate trades into the wallets table.

    Args:
        game: Restrict to a single game ('cs2', 'cod') or None for all games.
              When restricted, totals reflect only that game's trades — useful
              for fast incremental refresh during a tournament.
        since_ts: Only consider trades at or after this Unix timestamp.

    Returns:
        Number of wallet rows upserted.
    """
    conn = get_connection()
    cursor = conn.cursor()
    write_cursor = conn.cursor()

    now = int(time.time())
    upserted = 0

    for wallet, rows in _iter_wallet_groups(cursor, since_ts, game):
        agg = _aggregate_one_wallet(rows)
        if not agg:
            continue
        agg["last_recomputed_ts"] = now

        write_cursor.execute("""
            INSERT INTO wallets (
                proxy_wallet, pseudonym, name, first_seen_ts, last_seen_ts,
                total_trades, total_volume_usd, distinct_markets, distinct_games,
                games_json, buy_count, sell_count, median_trade_size, trade_size_cv,
                median_inter_trade_s, inter_trade_cv, active_hours, active_days_per_week,
                round_size_share, night_share, cross_market_burst, markets_per_day,
                two_sided_ratio, last_recomputed_ts
            ) VALUES (
                :proxy_wallet, :pseudonym, :name, :first_seen_ts, :last_seen_ts,
                :total_trades, :total_volume_usd, :distinct_markets, :distinct_games,
                :games_json, :buy_count, :sell_count, :median_trade_size, :trade_size_cv,
                :median_inter_trade_s, :inter_trade_cv, :active_hours, :active_days_per_week,
                :round_size_share, :night_share, :cross_market_burst, :markets_per_day,
                :two_sided_ratio, :last_recomputed_ts
            )
            ON CONFLICT(proxy_wallet) DO UPDATE SET
                pseudonym            = COALESCE(excluded.pseudonym, wallets.pseudonym),
                name                 = COALESCE(excluded.name, wallets.name),
                first_seen_ts        = MIN(wallets.first_seen_ts, excluded.first_seen_ts),
                last_seen_ts         = MAX(wallets.last_seen_ts, excluded.last_seen_ts),
                total_trades         = excluded.total_trades,
                total_volume_usd     = excluded.total_volume_usd,
                distinct_markets     = excluded.distinct_markets,
                distinct_games       = excluded.distinct_games,
                games_json           = excluded.games_json,
                buy_count            = excluded.buy_count,
                sell_count           = excluded.sell_count,
                median_trade_size    = excluded.median_trade_size,
                trade_size_cv        = excluded.trade_size_cv,
                median_inter_trade_s = excluded.median_inter_trade_s,
                inter_trade_cv       = excluded.inter_trade_cv,
                active_hours         = excluded.active_hours,
                active_days_per_week = excluded.active_days_per_week,
                round_size_share     = excluded.round_size_share,
                night_share          = excluded.night_share,
                cross_market_burst   = excluded.cross_market_burst,
                markets_per_day      = excluded.markets_per_day,
                two_sided_ratio      = excluded.two_sided_ratio,
                last_recomputed_ts   = excluded.last_recomputed_ts
        """, agg)
        upserted += 1

    conn.commit()
    logger.info(f"recompute_wallets: upserted {upserted} wallet rows (game={game}, since={since_ts})")
    return upserted

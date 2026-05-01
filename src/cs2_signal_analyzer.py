"""Compute CS2-specific bot signals: Pinnacle following, score reactions,
tier-1 tournament concentration, pre-match vs in-game split.

Each signal is stored as one row in cs2_wallet_signals keyed by
(proxy_wallet, signal_name) so the API can join lazily and we can re-derive
without touching the wallets table.

Pinnacle linkage uses pinnacle_match_links → pinnacle_snapshots, both
already populated by the collector (src/database.py:254, 271).
"""

import statistics
import time
from typing import Optional

from .config import CS2_SIGNAL_HEURISTICS, TIER1_CS2_KEYWORDS
from .database import get_connection
from .utils import logger


SIGNAL_NAMES = (
    "pinnacle_lag_ms_p50",
    "pinnacle_sign_match",
    "score_reaction_share",
    "tier1_volume_share",
    "in_game_volume_share",
)


def _is_tier1_question(question: Optional[str]) -> bool:
    if not question:
        return False
    q = question.lower()
    return any(kw in q for kw in TIER1_CS2_KEYWORDS)


def _upsert_signal(cursor, wallet: str, name: str, value: Optional[float], n: int, now: int) -> None:
    if value is None:
        return
    cursor.execute(
        """
        INSERT INTO cs2_wallet_signals (proxy_wallet, signal_name, signal_value, n_observations, computed_at_ts)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(proxy_wallet, signal_name) DO UPDATE SET
            signal_value   = excluded.signal_value,
            n_observations = excluded.n_observations,
            computed_at_ts = excluded.computed_at_ts
        """,
        (wallet, name, float(value), int(n), now),
    )


def _compute_pinnacle_signals(conn, now: int) -> int:
    """Per-wallet: median lag and sign-match rate vs the most recent Pinnacle move.

    For each CS2 trade by wallet W on market M, find the latest pinnacle_snapshots
    row for that market within `pinnacle_lookback_window_s` BEFORE the trade. Get
    the row immediately before that one (any earlier snapshot for the same match).
    Compute the implied-probability move (home side); ignore moves smaller than
    the configured threshold. Compare the sign of that move to the sign of the
    trade (BUY = positive direction, SELL = negative). Lag is trade_ts -
    snapshot_ts (seconds, then *1000 for ms).
    """
    h = CS2_SIGNAL_HEURISTICS
    lookback = h["pinnacle_lookback_window_s"]
    min_move = h["pinnacle_min_move_implied"]

    # We need each CS2 trade with the matched pinnacle snapshot + the
    # snapshot immediately preceding it for the same match. SQLite's
    # window functions handle this efficiently in a single pass.
    sql = """
    WITH cs2_trades AS (
        SELECT t.proxy_wallet, t.market_id, t.timestamp AS trade_ts, t.side
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        WHERE m.game = 'cs2'
          AND t.proxy_wallet IS NOT NULL
          AND t.proxy_wallet != ''
    ),
    matched AS (
        SELECT
            ct.proxy_wallet,
            ct.market_id,
            ct.trade_ts,
            ct.side,
            (SELECT ps.timestamp
               FROM pinnacle_snapshots ps
              WHERE ps.market_id = ct.market_id
                AND ps.timestamp <= ct.trade_ts
                AND ps.timestamp >= ct.trade_ts - ?
              ORDER BY ps.timestamp DESC LIMIT 1) AS snap_ts,
            (SELECT ps.ml_home_implied
               FROM pinnacle_snapshots ps
              WHERE ps.market_id = ct.market_id
                AND ps.timestamp <= ct.trade_ts
                AND ps.timestamp >= ct.trade_ts - ?
              ORDER BY ps.timestamp DESC LIMIT 1) AS snap_implied,
            (SELECT ps2.ml_home_implied
               FROM pinnacle_snapshots ps2
              WHERE ps2.market_id = ct.market_id
                AND ps2.timestamp < (
                    SELECT ps.timestamp FROM pinnacle_snapshots ps
                    WHERE ps.market_id = ct.market_id
                      AND ps.timestamp <= ct.trade_ts
                    ORDER BY ps.timestamp DESC LIMIT 1
                )
              ORDER BY ps2.timestamp DESC LIMIT 1) AS prev_implied
        FROM cs2_trades ct
    )
    SELECT proxy_wallet, market_id, trade_ts, side, snap_ts, snap_implied, prev_implied
    FROM matched
    WHERE snap_ts IS NOT NULL
      AND snap_implied IS NOT NULL
      AND prev_implied IS NOT NULL
    """

    cursor = conn.cursor()
    write_cursor = conn.cursor()

    by_wallet: dict[str, dict] = {}
    rows = cursor.execute(sql, (lookback, lookback)).fetchall()
    for r in rows:
        wallet = r["proxy_wallet"]
        move = (r["snap_implied"] or 0.0) - (r["prev_implied"] or 0.0)
        if abs(move) < min_move:
            continue
        lag_ms = max(0, (r["trade_ts"] - r["snap_ts"]) * 1000)
        side_sign = 1 if r["side"] == "BUY" else -1 if r["side"] == "SELL" else 0
        move_sign = 1 if move > 0 else -1
        sign_match = 1 if side_sign == move_sign else 0

        bucket = by_wallet.setdefault(wallet, {"lags": [], "matches": []})
        bucket["lags"].append(lag_ms)
        bucket["matches"].append(sign_match)

    written = 0
    for wallet, data in by_wallet.items():
        n = len(data["lags"])
        if n == 0:
            continue
        _upsert_signal(write_cursor, wallet, "pinnacle_lag_ms_p50",
                       statistics.median(data["lags"]), n, now)
        _upsert_signal(write_cursor, wallet, "pinnacle_sign_match",
                       sum(data["matches"]) / n, n, now)
        written += 1

    conn.commit()
    return written


def _compute_score_reaction_signals(conn, now: int) -> int:
    """% of wallet's CS2 trades landing within `score_event_window_s` of a
    final_prices snapshot (which marks a match-end / score event).
    """
    window = CS2_SIGNAL_HEURISTICS["score_event_window_s"]

    sql = """
    WITH cs2_trades AS (
        SELECT t.proxy_wallet, t.market_id, t.timestamp AS trade_ts
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        WHERE m.game = 'cs2'
          AND t.proxy_wallet IS NOT NULL
          AND t.proxy_wallet != ''
    ),
    fp_events AS (
        SELECT market_id, CAST(strftime('%s', match_ended_at) AS INTEGER) AS evt_ts
        FROM final_prices
        WHERE game = 'cs2' AND match_ended_at IS NOT NULL
    )
    SELECT
        ct.proxy_wallet,
        COUNT(*) AS total,
        SUM(CASE WHEN EXISTS (
            SELECT 1 FROM fp_events fp
             WHERE fp.market_id = ct.market_id
               AND ct.trade_ts BETWEEN fp.evt_ts AND fp.evt_ts + ?
        ) THEN 1 ELSE 0 END) AS reacted
    FROM cs2_trades ct
    GROUP BY ct.proxy_wallet
    """

    cursor = conn.cursor()
    write_cursor = conn.cursor()
    written = 0

    for r in cursor.execute(sql, (window,)).fetchall():
        total = r["total"] or 0
        if total < CS2_SIGNAL_HEURISTICS["min_cs2_trades_for_signals"]:
            continue
        share = (r["reacted"] or 0) / total if total else 0.0
        _upsert_signal(write_cursor, r["proxy_wallet"], "score_reaction_share",
                       share, total, now)
        written += 1

    conn.commit()
    return written


def _compute_tier1_and_in_game_signals(conn, now: int) -> int:
    """Two pure-volume signals per wallet:
    - tier1_volume_share: tier-1 CS2 volume / total CS2 volume
    - in_game_volume_share: trades after game_start_time / total CS2 trades

    in_game share is skipped when the markets table is missing
    game_start_time (which can happen on a fresh DB until api/migrate.py is
    run; the column is added unconditionally there).
    """
    cursor = conn.cursor()
    market_cols = {r[1] for r in cursor.execute("PRAGMA table_info(markets)")}
    has_gst = "game_start_time" in market_cols
    gst_select = "m.game_start_time" if has_gst else "NULL AS game_start_time"

    sql = f"""
    SELECT
        t.proxy_wallet,
        m.question,
        {gst_select},
        t.timestamp,
        t.size
    FROM trades t
    JOIN markets m ON t.market_id = m.market_id
    WHERE m.game = 'cs2'
      AND t.proxy_wallet IS NOT NULL
      AND t.proxy_wallet != ''
    """

    cursor = conn.cursor()
    write_cursor = conn.cursor()

    from datetime import datetime, timezone

    by_wallet: dict[str, dict] = {}
    for r in cursor.execute(sql).fetchall():
        wallet = r["proxy_wallet"]
        bucket = by_wallet.setdefault(wallet, {
            "total_vol": 0.0, "tier1_vol": 0.0,
            "total_n": 0, "in_game_n": 0,
        })
        size = r["size"] or 0.0
        bucket["total_vol"] += size
        bucket["total_n"] += 1
        if _is_tier1_question(r["question"]):
            bucket["tier1_vol"] += size

        gst = r["game_start_time"]
        if gst:
            try:
                gst_ts = int(datetime.fromisoformat(gst.replace("Z", "+00:00")).timestamp())
                if r["timestamp"] >= gst_ts:
                    bucket["in_game_n"] += 1
            except (ValueError, AttributeError):
                pass

    written = 0
    for wallet, b in by_wallet.items():
        if b["total_n"] < CS2_SIGNAL_HEURISTICS["min_cs2_trades_for_signals"]:
            continue
        tier1_share = (b["tier1_vol"] / b["total_vol"]) if b["total_vol"] > 0 else 0.0
        in_game_share = (b["in_game_n"] / b["total_n"]) if b["total_n"] else 0.0
        _upsert_signal(write_cursor, wallet, "tier1_volume_share", tier1_share, b["total_n"], now)
        _upsert_signal(write_cursor, wallet, "in_game_volume_share", in_game_share, b["total_n"], now)
        written += 1

    conn.commit()
    return written


def compute_cs2_signals() -> dict:
    """Run all CS2 signal computations. Returns a dict of {signal_group: rows_written}."""
    conn = get_connection()
    now = int(time.time())

    counts = {
        "pinnacle": _compute_pinnacle_signals(conn, now),
        "score_reaction": _compute_score_reaction_signals(conn, now),
        "tier1_and_in_game": _compute_tier1_and_in_game_signals(conn, now),
    }
    logger.info(f"compute_cs2_signals: {counts}")
    return counts

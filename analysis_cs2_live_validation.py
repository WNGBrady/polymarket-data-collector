"""Post-match validation: did Polymarket per-map odds reflect game state, or
was there a baked-in delay?

Compares the GOTV demo's ground-truth round_end ticks (converted to UTC via a
manually-marked map_start tap) against the largest mid-price moves on the
Polymarket per-map market and the paired Pinnacle (ps3838) snapshots.

Usage:
    python analysis_cs2_live_validation.py --market-id <id> --demo-path data/demos/<m>/map1.dem
    python analysis_cs2_live_validation.py --market-id <id> --demo-path <path> --stream-delay-ms 18000
    python analysis_cs2_live_validation.py --market-id <id> --demo-path <path> --map-num 1 --output data/foo.csv

Reuses helpers from hltv_demos.py for demo parsing and anchor cross-checks.
Writes a per-round CSV and prints a verdict on Polymarket lead/lag vs the
demo's ground-truth clock.
"""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_REPO_ROOT))

from src.database import get_connection, init_database, migrate_database  # noqa: E402

# hltv_demos lives at the repo root, not inside src/
import hltv_demos  # noqa: E402


# ---------------------------------------------------------------------------
# DB pulls
# ---------------------------------------------------------------------------

def fetch_live_events(market_id: str, map_num: Optional[int]) -> List[Dict[str, Any]]:
    cur = get_connection().cursor()
    if map_num is not None:
        cur.execute(
            """
            SELECT * FROM cs2_live_events
            WHERE market_id = ? AND map_num = ?
            ORDER BY wall_clock_ms_utc ASC
            """,
            (market_id, map_num),
        )
    else:
        cur.execute(
            "SELECT * FROM cs2_live_events WHERE market_id = ? ORDER BY wall_clock_ms_utc ASC",
            (market_id,),
        )
    return [dict(r) for r in cur.fetchall()]


def fetch_orderbook_snaps(
    market_id: str,
    start_ms: int,
    end_ms: int,
) -> List[Tuple[int, Optional[float], Optional[float], Optional[float]]]:
    """Returns (timestamp_ms, best_bid, best_ask, mid_price) sorted ascending."""
    cur = get_connection().cursor()
    cur.execute(
        """
        SELECT timestamp, best_bid_price, best_ask_price, mid_price
        FROM orderbook_snapshots
        WHERE market_id = ? AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
        """,
        (market_id, start_ms, end_ms),
    )
    return [(int(r["timestamp"]), r["best_bid_price"], r["best_ask_price"], r["mid_price"])
            for r in cur.fetchall()]


def fetch_pinnacle_snaps(
    market_id: str,
    start_ms: int,
    end_ms: int,
    bookmaker: str = "ps3838",
) -> List[Tuple[int, Optional[float], Optional[float], Optional[int]]]:
    """Returns (timestamp_ms, ml_home_novig, ml_away_novig, is_live) sorted asc."""
    cur = get_connection().cursor()
    cur.execute(
        """
        SELECT timestamp, ml_home_novig, ml_away_novig, is_live
        FROM pinnacle_snapshots
        WHERE market_id = ? AND bookmaker = ? AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
        """,
        (market_id, bookmaker, start_ms, end_ms),
    )
    return [(int(r["timestamp"]), r["ml_home_novig"], r["ml_away_novig"], r["is_live"])
            for r in cur.fetchall()]


def fetch_trades_for_anchor(market_id: str) -> List[Tuple[int, float]]:
    """Returns (timestamp_seconds, price) for use with detect_trades_anchor."""
    cur = get_connection().cursor()
    cur.execute(
        "SELECT timestamp, price FROM trades WHERE market_id = ? ORDER BY timestamp ASC",
        (market_id,),
    )
    out = []
    for r in cur.fetchall():
        ts, px = r["timestamp"], r["price"]
        if ts is None or px is None:
            continue
        out.append((int(ts), float(px)))
    return out


# ---------------------------------------------------------------------------
# Anchor selection
# ---------------------------------------------------------------------------

def pick_demo_anchor(
    meta: hltv_demos.DemoMeta,
    live_events: List[Dict[str, Any]],
    map_num: int,
    stream_delay_ms: int,
    market_id: str,
) -> Tuple[Optional[int], int, str, Dict[str, Optional[int]]]:
    """Return (anchor_tick, anchor_ms, source, debug_dict).

    Preference: user's `map_start` tap minus stream_delay_ms (because the tap
    is bounded by the user's stream delay). Cross-checked against
    detect_orderbook_anchor over a wide window. If no map_start tap is
    available, falls back to the orderbook anchor.
    """
    debug: Dict[str, Optional[int]] = {
        "map_start_tap_ms": None,
        "map_start_minus_delay_ms": None,
        "orderbook_anchor_ms": None,
        "trades_anchor_ms": None,
    }

    # round_starts in DemoMeta is List[(round_num, tick)]; we want round 1 START.
    anchor_tick = None
    for rn, tk in meta.round_starts:
        if rn == 1:
            anchor_tick = tk
            break
    if anchor_tick is None:
        # Fall back to round 1 end if start is missing.
        anchor_tick = hltv_demos.find_round1_end_tick(meta)
    if anchor_tick is None:
        return (None, 0, "no-demo-anchor-tick", debug)

    map_start_tap = next(
        (e["wall_clock_ms_utc"] for e in live_events
         if e.get("event_type") == "map_start" and (e.get("map_num") or 0) == map_num),
        None,
    )
    debug["map_start_tap_ms"] = map_start_tap

    # Cross-check anchors from the orderbook + trades. Use a wide window
    # centered loosely on the tap (or the last 12 hours if no tap).
    if map_start_tap is not None:
        win_lo = map_start_tap - 30 * 60 * 1000
        win_hi = map_start_tap + 4 * 60 * 60 * 1000
    else:
        # Last 24h of data — reasonable for a same-day analysis.
        cur = get_connection().cursor()
        cur.execute("SELECT MAX(timestamp) AS m FROM orderbook_snapshots WHERE market_id = ?", (market_id,))
        row = cur.fetchone()
        if not row or row["m"] is None:
            return (None, 0, "no-snapshots-for-market", debug)
        win_hi = int(row["m"])
        win_lo = win_hi - 24 * 60 * 60 * 1000

    snaps = fetch_orderbook_snaps(market_id, win_lo, win_hi)
    ob_anchor = hltv_demos.detect_orderbook_anchor(
        [(t, bb, ba) for (t, bb, ba, _mid) in snaps],
    )
    debug["orderbook_anchor_ms"] = ob_anchor

    trades = [(ts, px) for ts, px in fetch_trades_for_anchor(market_id) if win_lo // 1000 <= ts <= win_hi // 1000]
    tr_anchor = hltv_demos.detect_trades_anchor(trades) if trades else None
    debug["trades_anchor_ms"] = tr_anchor

    if map_start_tap is not None:
        # Tap captures USER perception (stream-delayed). Subtract the supplied
        # delay to anchor at actual game time.
        anchor_ms = map_start_tap - stream_delay_ms
        debug["map_start_minus_delay_ms"] = anchor_ms
        source = "map_start_tap"
    elif ob_anchor is not None:
        # Orderbook anchor is the FIRST live snapshot (~= round 1 start since
        # the market goes live as the map begins).
        anchor_ms = ob_anchor
        source = "orderbook_anchor"
    elif tr_anchor is not None:
        anchor_ms = tr_anchor
        source = "trades_anchor"
    else:
        return (None, 0, "no-anchor-available", debug)

    return (anchor_tick, anchor_ms, source, debug)


# ---------------------------------------------------------------------------
# Per-round move detection
# ---------------------------------------------------------------------------

def find_largest_move(
    series: List[Tuple[int, Optional[float]]],
    center_ms: int,
    pre_ms: int = 30_000,
    post_ms: int = 90_000,
) -> Tuple[Optional[int], Optional[float]]:
    """Within [center_ms - pre_ms, center_ms + post_ms], find the consecutive-
    snapshot Δprice with the largest |Δ| and return (timestamp_of_change, Δprice).

    Δ is signed (later − earlier). The returned timestamp is the timestamp of
    the LATER snapshot (i.e. when the new price was observed).
    """
    lo, hi = center_ms - pre_ms, center_ms + post_ms
    window = [(t, p) for (t, p) in series if lo <= t <= hi and p is not None]
    if len(window) < 2:
        return (None, None)
    best_ts: Optional[int] = None
    best_dp: float = 0.0
    for i in range(1, len(window)):
        prev_p = window[i - 1][1]
        cur_p = window[i][1]
        if prev_p is None or cur_p is None:
            continue
        dp = cur_p - prev_p
        if abs(dp) > abs(best_dp):
            best_dp = dp
            best_ts = window[i][0]
    if best_ts is None:
        return (None, None)
    return (best_ts, best_dp)


# ---------------------------------------------------------------------------
# CSV + verdict
# ---------------------------------------------------------------------------

def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print(f"  no rows to write to {path}")
        return
    cols = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  wrote {len(rows)} rows -> {path}")


def median_or_none(xs: List[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def iqr_or_none(xs: List[float]) -> Optional[Tuple[float, float]]:
    xs = sorted([x for x in xs if x is not None])
    if len(xs) < 4:
        return None
    q1 = xs[len(xs) // 4]
    q3 = xs[(3 * len(xs)) // 4]
    return (q1, q3)


def verdict_text(
    polymarket_lags_ms: List[float],
    pinnacle_lags_ms: List[float],
    user_tap_lags_ms: List[float],
    stream_delay_ms: int,
) -> str:
    pm_med = median_or_none(polymarket_lags_ms)
    pn_med = median_or_none(pinnacle_lags_ms)
    user_med = median_or_none(user_tap_lags_ms)
    lines = []
    lines.append("=== verdict ===")
    lines.append(f"  empirical stream delay (median user_tap - demo_round_end): {user_med}ms")
    lines.append(f"  Polymarket  median lag (move_ts - demo_round_end): {pm_med}ms")
    lines.append(f"  Pinnacle    median lag (move_ts - demo_round_end): {pn_med}ms")
    lines.append(f"  --stream-delay-ms supplied: {stream_delay_ms}ms")
    if pm_med is None:
        lines.append("  insufficient Polymarket data for a verdict.")
        return "\n".join(lines)
    # Use the user-tap-derived stream delay if available, else the supplied one.
    delay = user_med if user_med is not None else stream_delay_ms
    diff = pm_med - delay
    if diff < 5_000:
        lines.append(
            f"  ==> Polymarket appears to reflect game state on the stream "
            f"(lag - stream_delay = {diff:.0f}ms within a 5s tolerance)."
        )
    elif diff < 30_000:
        lines.append(
            f"  ==> Polymarket lags game state by {diff:.0f}ms beyond stream delay -- "
            f"reactive but not delayed by a full round."
        )
    else:
        lines.append(
            f"  ==> Polymarket lags game state by {diff:.0f}ms beyond stream delay -- "
            f"this is large enough to suspect a baked-in delay (e.g. ~one round)."
        )
    if pn_med is not None:
        lines.append(f"  Polymarket vs Pinnacle median lag: {pm_med - pn_med:.0f}ms")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Validate Polymarket lead/lag vs CS2 demo")
    p.add_argument("--market-id", required=True, help="Per-map Polymarket market_id")
    p.add_argument("--demo-path", required=True, help="Path to .dem file for the map")
    p.add_argument("--map-num", type=int, default=None,
                   help="Map number (filters cs2_live_events). Default: infer from demo filename, else 1.")
    p.add_argument("--stream-delay-ms", type=int, default=0,
                   help="Estimated stream delay to subtract from the map_start tap (ms).")
    p.add_argument("--pre-ms", type=int, default=30_000,
                   help="Lookback window before round_end UTC for move detection (ms).")
    p.add_argument("--post-ms", type=int, default=90_000,
                   help="Lookahead window after round_end UTC for move detection (ms).")
    p.add_argument("--output", default=None,
                   help="Output CSV path. Default: data/<market_id>_map<N>_round_alignment.csv")
    args = p.parse_args()

    migrate_database()
    init_database()

    demo_path = Path(args.demo_path)
    if not demo_path.exists():
        print(f"! demo path not found: {demo_path}", file=sys.stderr)
        sys.exit(2)

    map_num = args.map_num
    if map_num is None:
        from_name = hltv_demos.map_n_from_demo_path(demo_path)
        map_num = from_name if from_name is not None else 1
    print(f"Parsing demo {demo_path.name} (map_num={map_num})...")
    meta = hltv_demos.parse_demo(demo_path)
    print(f"  map={meta.map_name} tickrate={meta.tickrate} rounds={len(meta.round_ends)}")

    live_events = fetch_live_events(args.market_id, map_num)
    print(f"  pulled {len(live_events)} cs2_live_events for market={args.market_id} map={map_num}")

    anchor_tick, anchor_ms, anchor_source, anchor_debug = pick_demo_anchor(
        meta, live_events, map_num, args.stream_delay_ms, args.market_id,
    )
    if anchor_tick is None:
        print(f"! no usable anchor: {anchor_source}", file=sys.stderr)
        print(f"  debug: {anchor_debug}", file=sys.stderr)
        sys.exit(2)
    print(f"  anchor: source={anchor_source} tick={anchor_tick} ms={anchor_ms}  debug={anchor_debug}")

    # Pull a wide window of price snapshots covering all round_ends.
    if not meta.round_ends:
        print("! demo has no round_end events", file=sys.stderr)
        sys.exit(2)
    last_tick = max(re.tick for re in meta.round_ends)
    span_ms = max(args.post_ms + args.pre_ms, 60 * 60 * 1000)  # at least one hour
    window_lo = anchor_ms - args.pre_ms
    window_hi = hltv_demos.tick_to_wallclock_ms(last_tick, anchor_tick, anchor_ms, meta.tickrate) + span_ms

    ob_snaps = fetch_orderbook_snaps(args.market_id, window_lo, window_hi)
    pn_snaps = fetch_pinnacle_snaps(args.market_id, window_lo, window_hi)
    print(f"  orderbook snapshots in window: {len(ob_snaps)}; pinnacle snapshots: {len(pn_snaps)}")

    pm_series = [(t, mid) for (t, _bb, _ba, mid) in ob_snaps]
    # Pinnacle: track |Δp| on home side; symmetry means away gives the negated value.
    pn_series = [(t, h) for (t, h, _a, _live) in pn_snaps]

    # Map user round_end taps to demo rounds (nearest demo round in time).
    user_round_taps = [e for e in live_events if e.get("event_type") == "round_end"]

    rows: List[Dict[str, Any]] = []
    polymarket_lags: List[float] = []
    pinnacle_lags: List[float] = []
    user_tap_lags: List[float] = []

    for re_ev in sorted(meta.round_ends, key=lambda r: r.round_num):
        demo_ts = hltv_demos.tick_to_wallclock_ms(re_ev.tick, anchor_tick, anchor_ms, meta.tickrate)
        pm_ts, pm_dp = find_largest_move(pm_series, demo_ts, args.pre_ms, args.post_ms)
        pn_ts, pn_dp = find_largest_move(pn_series, demo_ts, args.pre_ms, args.post_ms)

        # Find nearest user_tap within the same window
        user_tap = None
        if user_round_taps:
            best_diff = None
            for tap in user_round_taps:
                d = abs(tap["wall_clock_ms_utc"] - demo_ts)
                if best_diff is None or d < best_diff:
                    best_diff = d
                    user_tap = tap
            # Only count if within the lag window (avoid pairing far-off taps)
            if user_tap is not None and abs(user_tap["wall_clock_ms_utc"] - demo_ts) > (args.pre_ms + args.post_ms):
                user_tap = None

        pm_lag = (pm_ts - demo_ts) if pm_ts is not None else None
        pn_lag = (pn_ts - demo_ts) if pn_ts is not None else None
        user_lag = (user_tap["wall_clock_ms_utc"] - demo_ts) if user_tap else None

        if pm_lag is not None:
            polymarket_lags.append(pm_lag)
        if pn_lag is not None:
            pinnacle_lags.append(pn_lag)
        if user_lag is not None:
            user_tap_lags.append(user_lag)

        rows.append({
            "round_num": re_ev.round_num,
            "winner_side": re_ev.winner_side,
            "winner_team": re_ev.winner_team,
            "loser_team": re_ev.loser_team,
            "reason": re_ev.reason,
            "demo_tick": re_ev.tick,
            "demo_ts_ms": demo_ts,
            "user_tap_ts_ms": user_tap["wall_clock_ms_utc"] if user_tap else None,
            "user_tap_side": user_tap["winning_side"] if user_tap else None,
            "user_tap_lag_ms": user_lag,
            "polymarket_move_ts_ms": pm_ts,
            "polymarket_dmid": pm_dp,
            "polymarket_lag_ms": pm_lag,
            "pinnacle_move_ts_ms": pn_ts,
            "pinnacle_dhome": pn_dp,
            "pinnacle_lag_ms": pn_lag,
            "polymarket_vs_pinnacle_lag_ms": (pm_ts - pn_ts) if (pm_ts is not None and pn_ts is not None) else None,
        })

    out_path = Path(args.output) if args.output else (
        Path("data") / f"{args.market_id}_map{map_num}_round_alignment.csv"
    )
    write_csv(out_path, rows)

    # Summary stats
    print()
    print(f"  rounds analyzed: {len(rows)}")
    print(f"  Polymarket lags found: {len(polymarket_lags)} | median: {median_or_none(polymarket_lags)}ms | IQR: {iqr_or_none(polymarket_lags)}")
    print(f"  Pinnacle lags found:   {len(pinnacle_lags)} | median: {median_or_none(pinnacle_lags)}ms | IQR: {iqr_or_none(pinnacle_lags)}")
    print(f"  user-tap lags found:   {len(user_tap_lags)} | median: {median_or_none(user_tap_lags)}ms | IQR: {iqr_or_none(user_tap_lags)}")
    print()
    print(verdict_text(polymarket_lags, pinnacle_lags, user_tap_lags, args.stream_delay_ms))


if __name__ == "__main__":
    main()

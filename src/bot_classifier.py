"""Score each wallet against the bot heuristics in BOT_HEURISTICS.

Reads the per-wallet features already aggregated into the `wallets` table,
computes a 0-1 bot_score, and writes back a coarse bot_label. Runs as a
standalone pass after recompute_wallets() so the heavy aggregation isn't
duplicated.

Designed to be cheap and re-runnable: changing BOT_HEURISTICS thresholds and
re-running scores the existing wallet rows without touching trades.
"""

import time
from typing import Optional

from .config import BOT_HEURISTICS
from .database import get_connection
from .utils import logger


def _flag_value(name: str, row: dict) -> float:
    """Return the per-feature contribution in [0, 1]; threshold-and-clip."""
    h = BOT_HEURISTICS

    if name == "inter_trade_cv":
        cv = row.get("inter_trade_cv")
        if cv is None or row.get("total_trades", 0) < h["min_trades_for_classification"]:
            return 0.0
        # Lower CV = more bot-like. Map [0, threshold] -> [1, 0].
        thresh = h["inter_trade_cv_max"]
        if cv >= thresh:
            return 0.0
        return max(0.0, min(1.0, (thresh - cv) / thresh))

    if name == "round_size_share":
        share = row.get("round_size_share") or 0.0
        thresh = h["round_size_share_min"]
        if share <= thresh:
            return 0.0
        # Linearly scale (thresh, 1.0) -> (0, 1)
        return max(0.0, min(1.0, (share - thresh) / max(1e-6, 1.0 - thresh)))

    if name == "night_share":
        share = row.get("night_share") or 0.0
        thresh = h["night_share_min"]
        if share <= thresh:
            return 0.0
        return max(0.0, min(1.0, (share - thresh) / max(1e-6, 1.0 - thresh)))

    if name == "cross_market_burst":
        bursts = row.get("cross_market_burst") or 0
        # Any burst is suspicious; saturate at 10
        return max(0.0, min(1.0, bursts / 10.0))

    if name == "markets_per_day":
        m = row.get("markets_per_day") or 0.0
        thresh = h["markets_per_active_day_min"]
        if m <= thresh:
            return 0.0
        # Saturate at 3x threshold
        return max(0.0, min(1.0, (m - thresh) / (2 * thresh)))

    if name == "two_sided_ratio":
        r = row.get("two_sided_ratio") or 0.0
        thresh = h["two_sided_ratio_min"]
        if r < thresh:
            return 0.0
        # 0.4 -> 0, 1.0 -> 1
        return max(0.0, min(1.0, (r - thresh) / max(1e-6, 1.0 - thresh)))

    return 0.0


def classify_wallet(row: dict) -> tuple[float, str]:
    """Return (bot_score in [0,1], bot_label)."""
    h = BOT_HEURISTICS

    if (row.get("total_trades") or 0) < h["min_trades_for_classification"]:
        return 0.0, "human"

    weights = h["weights"]
    weighted_sum = sum(_flag_value(name, row) * w for name, w in weights.items())
    total_weight = sum(weights.values())
    score = weighted_sum / total_weight if total_weight else 0.0

    # Hard rule: if two-sided ratio is high and has enough trades, label market_maker
    two_sided = row.get("two_sided_ratio") or 0.0
    if two_sided >= h["two_sided_ratio_min"] and (row.get("total_trades") or 0) >= h["two_sided_min_trades"]:
        return score, "market_maker"

    if score >= h["score_bot_threshold"]:
        return score, "bot"
    if score >= h["score_likely_bot_threshold"]:
        return score, "likely_bot"
    return score, "human"


def classify_all_wallets() -> int:
    """Score every row in `wallets` and write bot_score / bot_label back.

    Returns:
        Number of wallets re-labelled.
    """
    conn = get_connection()
    cursor = conn.cursor()
    write_cursor = conn.cursor()

    now = int(time.time())
    updated = 0

    rows = cursor.execute("SELECT * FROM wallets").fetchall()
    for r in rows:
        d = dict(r)
        score, label = classify_wallet(d)
        write_cursor.execute(
            """
            UPDATE wallets
            SET bot_score = ?, bot_label = ?, last_recomputed_ts = ?
            WHERE proxy_wallet = ?
            """,
            (score, label, now, d["proxy_wallet"]),
        )
        updated += 1

    conn.commit()
    logger.info(f"classify_all_wallets: updated {updated} rows")
    return updated

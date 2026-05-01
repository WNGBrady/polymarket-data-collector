"""GET /api/cs2-bot-signals — distribution of CS2 signal values across wallets,
optionally bucketed by bot_label.

Powers the chart "do bots react to Pinnacle moves faster than humans?". Returns
histograms for each signal_name plus per-label summary stats.
"""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


_SIGNAL_NAMES = [
    "pinnacle_lag_ms_p50",
    "pinnacle_sign_match",
    "score_reaction_share",
    "tier1_volume_share",
    "in_game_volume_share",
]


def _histogram(values: list[float], n_bins: int = 20) -> dict:
    if not values:
        return {"bins": [], "counts": []}
    lo = min(values)
    hi = max(values)
    if hi == lo:
        return {"bins": [lo, lo], "counts": [len(values)]}
    width = (hi - lo) / n_bins
    counts = [0] * n_bins
    for v in values:
        idx = min(n_bins - 1, int((v - lo) / width))
        counts[idx] += 1
    bins = [lo + i * width for i in range(n_bins + 1)]
    return {"bins": bins, "counts": counts}


@router.get("/cs2-bot-signals")
def cs2_bot_signals(
    bot_label: str = Query("", pattern="^(human|likely_bot|bot|market_maker|unknown|)$"),
    min_observations: int = Query(5, ge=1),
):
    key = f"cs2_bot_signals:{bot_label}:{min_observations}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    label_filter = ""
    params: tuple
    if bot_label:
        label_filter = "AND COALESCE(w.bot_label, 'unknown') = ?"
        params = (min_observations,)
    else:
        params = (min_observations,)

    out: dict[str, dict] = {}
    for sig in _SIGNAL_NAMES:
        rows = db.query_all(
            f"""
            SELECT s.signal_value, COALESCE(w.bot_label, 'unknown') AS bot_label
            FROM cs2_wallet_signals s
            LEFT JOIN wallets w ON w.proxy_wallet = s.proxy_wallet
            WHERE s.signal_name = ?
              AND s.n_observations >= ?
              {label_filter}
            """,
            (sig, *params, bot_label) if bot_label else (sig, *params),
        )
        values = [r["signal_value"] for r in rows]
        per_label: dict[str, list[float]] = {}
        for r in rows:
            per_label.setdefault(r["bot_label"], []).append(r["signal_value"])

        out[sig] = {
            "n": len(values),
            "histogram": _histogram(values),
            "by_label_summary": {
                lab: {
                    "n": len(vs),
                    "median": sorted(vs)[len(vs) // 2] if vs else None,
                    "min": min(vs) if vs else None,
                    "max": max(vs) if vs else None,
                }
                for lab, vs in per_label.items()
            },
        }

    cache.put(key, out, CACHE_TTL["charts"])
    return out

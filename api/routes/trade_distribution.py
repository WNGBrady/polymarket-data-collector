"""GET /api/trade-distribution — Histogram buckets + stats for trade sizes."""

import math

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


def _build_linear_buckets(sizes: list[float], n_buckets: int = 30) -> list[dict]:
    if not sizes:
        return []
    mn, mx = min(sizes), max(sizes)
    if mx == mn:
        return [{"bucket_min": mn, "bucket_max": mx, "count": len(sizes)}]
    step = (mx - mn) / n_buckets
    buckets = [{"bucket_min": mn + i * step, "bucket_max": mn + (i + 1) * step, "count": 0}
               for i in range(n_buckets)]
    for s in sizes:
        idx = min(int((s - mn) / step), n_buckets - 1)
        buckets[idx]["count"] += 1
    return buckets


def _build_log_buckets(sizes: list[float], n_buckets: int = 25) -> list[dict]:
    positive = [s for s in sizes if s > 0]
    if not positive:
        return []
    log_min = math.log10(min(positive))
    log_max = math.log10(max(positive))
    if log_max == log_min:
        return [{"bucket_min": min(positive), "bucket_max": max(positive), "count": len(positive)}]
    step = (log_max - log_min) / n_buckets
    buckets = [
        {"bucket_min": 10 ** (log_min + i * step),
         "bucket_max": 10 ** (log_min + (i + 1) * step),
         "count": 0}
        for i in range(n_buckets)
    ]
    for s in positive:
        idx = min(int((math.log10(s) - log_min) / step), n_buckets - 1)
        buckets[idx]["count"] += 1
    return buckets


@router.get("/trade-distribution")
def trade_distribution(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("trade_distribution", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_clauses = []
    params: list = []

    if game != "all":
        where_clauses.append("m.game = ?")
        params.append(game)
    if date_start:
        where_clauses.append("t.timestamp >= strftime('%s', ?)")
        params.append(date_start)
    if date_end:
        where_clauses.append("t.timestamp <= strftime('%s', ?, '+1 day')")
        params.append(date_end)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    rows = db.query_all(f"""
        SELECT t.size
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
    """, tuple(params))

    sizes = [r["size"] for r in rows]

    if sizes:
        sizes_sorted = sorted(sizes)
        n = len(sizes_sorted)
        median = sizes_sorted[n // 2]
        mean = sum(sizes) / n
        stats = {
            "count": n,
            "mean": round(mean, 2),
            "median": round(median, 2),
            "min": round(min(sizes), 2),
            "max": round(max(sizes), 2),
        }
    else:
        stats = {"count": 0, "mean": 0, "median": 0, "min": 0, "max": 0}

    result = {
        "linear_buckets": _build_linear_buckets(sizes),
        "log_buckets": _build_log_buckets(sizes),
        "stats": stats,
    }

    cache.put(key, result, CACHE_TTL["charts"])
    return result

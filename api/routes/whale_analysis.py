"""GET /api/whale-analysis — Whale vs retail volume/count split."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL, WHALE_THRESHOLD

router = APIRouter()


@router.get("/whale-analysis")
def whale_analysis(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("whale_analysis", game, date_start, date_end)
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

    row = db.query_one(f"""
        SELECT
            COALESCE(SUM(CASE WHEN t.size >= ? THEN t.size ELSE 0 END), 0) as whale_volume,
            COALESCE(SUM(CASE WHEN t.size < ? THEN t.size ELSE 0 END), 0) as retail_volume,
            COUNT(CASE WHEN t.size >= ? THEN 1 END) as whale_count,
            COUNT(CASE WHEN t.size < ? THEN 1 END) as retail_count
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
    """, (WHALE_THRESHOLD, WHALE_THRESHOLD, WHALE_THRESHOLD, WHALE_THRESHOLD, *params))

    result = {
        "whale_volume": row["whale_volume"] if row else 0,
        "retail_volume": row["retail_volume"] if row else 0,
        "whale_count": row["whale_count"] if row else 0,
        "retail_count": row["retail_count"] if row else 0,
        "threshold": WHALE_THRESHOLD,
    }

    cache.put(key, result, CACHE_TTL["charts"])
    return result

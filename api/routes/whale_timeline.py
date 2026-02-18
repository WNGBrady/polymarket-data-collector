"""GET /api/whale-timeline — Whale trades as scatter data."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL, WHALE_THRESHOLD

router = APIRouter()


@router.get("/whale-timeline")
def whale_timeline(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("whale_timeline", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_clauses = ["t.size >= ?"]
    params: list = [WHALE_THRESHOLD]

    if game != "all":
        where_clauses.append("m.game = ?")
        params.append(game)
    if date_start:
        where_clauses.append("t.timestamp >= strftime('%s', ?)")
        params.append(date_start)
    if date_end:
        where_clauses.append("t.timestamp <= strftime('%s', ?, '+1 day')")
        params.append(date_end)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    rows = db.query_all(f"""
        SELECT
            t.timestamp,
            t.price,
            t.size,
            t.side,
            t.outcome,
            m.question,
            m.game
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
        ORDER BY t.timestamp ASC
    """, tuple(params))

    result = {"data": rows, "threshold": WHALE_THRESHOLD}
    cache.put(key, result, CACHE_TTL["charts"])
    return result

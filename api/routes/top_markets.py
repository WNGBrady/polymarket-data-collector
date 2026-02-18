"""GET /api/top-markets — Top N markets by volume."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/top-markets")
def top_markets(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    limit: int = Query(10, ge=1, le=50),
):
    key = cache.make_key(f"top_markets:{limit}", game, date_start, date_end)
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
        SELECT
            m.market_id,
            m.question,
            m.game,
            SUM(t.size) as volume,
            COUNT(*) as trade_count
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
        GROUP BY m.market_id
        ORDER BY volume DESC
        LIMIT ?
    """, (*params, limit))

    result = {"data": rows}
    cache.put(key, result, CACHE_TTL["charts"])
    return result

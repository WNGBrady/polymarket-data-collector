"""GET /api/spread-analysis — Average spread per market from orderbook snapshots."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/spread-analysis")
def spread_analysis(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    limit: int = Query(15, ge=1, le=50),
):
    key = cache.make_key(f"spread_analysis:{limit}", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_clauses = ["o.spread > 0", "o.spread < 1"]
    params: list = []

    if game != "all":
        where_clauses.append("m.game = ?")
        params.append(game)
    if date_start:
        where_clauses.append("o.timestamp >= strftime('%s', ?) * 1000")
        params.append(date_start)
    if date_end:
        where_clauses.append("o.timestamp <= strftime('%s', ?, '+1 day') * 1000")
        params.append(date_end)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    rows = db.query_all(f"""
        SELECT
            m.market_id,
            m.question,
            m.game,
            AVG(o.spread) as avg_spread,
            MIN(o.spread) as min_spread,
            MAX(o.spread) as max_spread,
            COUNT(*) as snapshot_count
        FROM orderbook_snapshots o
        JOIN markets m ON o.market_id = m.market_id
        {where_sql}
        GROUP BY m.market_id
        ORDER BY avg_spread ASC
        LIMIT ?
    """, (*params, limit))

    result = {"data": rows}
    cache.put(key, result, CACHE_TTL["charts"])
    return result

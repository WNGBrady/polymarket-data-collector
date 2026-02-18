"""GET /api/collection-health — Daily record counts + table totals."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/collection-health")
def collection_health(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("collection_health", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_clauses: list[str] = []
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

    # Daily trade counts by game
    daily_trades = db.query_all(f"""
        SELECT
            date(t.timestamp, 'unixepoch') as date,
            m.game,
            COUNT(*) as count
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
        GROUP BY date(t.timestamp, 'unixepoch'), m.game
        ORDER BY date ASC
    """, tuple(params))

    # Daily orderbook counts by game
    ob_where: list[str] = ["o.spread IS NOT NULL"]
    ob_params: list = []
    if game != "all":
        ob_where.append("m.game = ?")
        ob_params.append(game)
    if date_start:
        ob_where.append("o.timestamp >= strftime('%s', ?) * 1000")
        ob_params.append(date_start)
    if date_end:
        ob_where.append("o.timestamp <= strftime('%s', ?, '+1 day') * 1000")
        ob_params.append(date_end)

    daily_orderbook = db.query_all(f"""
        SELECT
            date(o.timestamp / 1000, 'unixepoch') as date,
            m.game,
            COUNT(*) as count
        FROM orderbook_snapshots o
        JOIN markets m ON o.market_id = m.market_id
        WHERE {" AND ".join(ob_where)}
        GROUP BY date(o.timestamp / 1000, 'unixepoch'), m.game
        ORDER BY date ASC
    """, tuple(ob_params))

    # Table totals
    tables = ["markets", "trades", "price_history", "realtime_prices",
              "orderbook_snapshots", "final_prices", "open_interest"]
    totals = {}
    for table in tables:
        row = db.query_one(f"SELECT COUNT(*) as cnt FROM {table}")
        totals[table] = row["cnt"] if row else 0

    result = {
        "daily_trades": daily_trades,
        "daily_orderbook": daily_orderbook,
        "table_totals": totals,
    }

    cache.put(key, result, CACHE_TTL["health"])
    return result

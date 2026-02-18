"""GET /api/markets — Paginated market list. GET /api/markets/{id} — Market detail."""

from fastapi import APIRouter, Query, HTTPException

from .. import cache, db
from ..config import CACHE_TTL, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

router = APIRouter()


@router.get("/markets")
def list_markets(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    sort: str = Query("volume", pattern="^(volume|trades|question|spread)$"),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    search: str = Query(""),
):
    key = cache.make_key(f"markets:{page}:{page_size}:{sort}:{order}:{search}", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    where_clauses = []
    params: list = []

    if game != "all":
        where_clauses.append("m.game = ?")
        params.append(game)
    if search:
        where_clauses.append("m.question LIKE ?")
        params.append(f"%{search}%")

    trade_where = []
    trade_params: list = []
    if date_start:
        trade_where.append("t.timestamp >= strftime('%s', ?)")
        trade_params.append(date_start)
    if date_end:
        trade_where.append("t.timestamp <= strftime('%s', ?, '+1 day')")
        trade_params.append(date_end)

    trade_filter = ("AND " + " AND ".join(trade_where)) if trade_where else ""
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sort_map = {
        "volume": "volume",
        "trades": "trade_count",
        "question": "m.question",
        "spread": "avg_spread",
    }
    sort_col = sort_map.get(sort, "volume")
    order_sql = "DESC" if order == "desc" else "ASC"

    offset = (page - 1) * page_size

    # Count total
    count_row = db.query_one(f"""
        SELECT COUNT(DISTINCT m.market_id) as cnt
        FROM markets m
        {where_sql}
    """, tuple(params))
    total = count_row["cnt"] if count_row else 0

    all_params = tuple(params) + tuple(trade_params)

    rows = db.query_all(f"""
        SELECT
            m.market_id,
            m.question,
            m.game,
            m.outcomes,
            m.start_date,
            m.end_date,
            COALESCE(stats.volume, 0) as volume,
            COALESCE(stats.trade_count, 0) as trade_count,
            stats.latest_price,
            stats.avg_spread
        FROM markets m
        LEFT JOIN (
            SELECT
                t.market_id,
                SUM(t.size) as volume,
                COUNT(*) as trade_count,
                (SELECT t2.price FROM trades t2
                 WHERE t2.market_id = t.market_id
                 ORDER BY t2.timestamp DESC LIMIT 1) as latest_price,
                (SELECT AVG(o.spread) FROM orderbook_snapshots o
                 WHERE o.market_id = t.market_id AND o.spread > 0 AND o.spread < 1) as avg_spread
            FROM trades t
            WHERE 1=1 {trade_filter}
            GROUP BY t.market_id
        ) stats ON m.market_id = stats.market_id
        {where_sql}
        ORDER BY {sort_col} {order_sql} NULLS LAST
        LIMIT ? OFFSET ?
    """, all_params + (page_size, offset))

    result = {
        "markets": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0,
    }

    cache.put(key, result, CACHE_TTL["markets"])
    return result


@router.get("/markets/{market_id}")
def market_detail(market_id: str):
    key = f"market_detail:{market_id}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    market = db.query_one(
        "SELECT * FROM markets WHERE market_id = ?", (market_id,)
    )
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")

    # Recent trades
    trades = db.query_all("""
        SELECT trade_id, timestamp, price, size, side, outcome
        FROM trades
        WHERE market_id = ?
        ORDER BY timestamp DESC
        LIMIT 200
    """, (market_id,))

    # Price timeline (all trades ordered asc for charting)
    price_timeline = db.query_all("""
        SELECT timestamp, price, outcome
        FROM trades
        WHERE market_id = ?
        ORDER BY timestamp ASC
    """, (market_id,))

    # Latest orderbook
    orderbook = db.query_all("""
        SELECT token_id, best_bid_price, best_bid_size,
               best_ask_price, best_ask_size, spread, mid_price,
               bid_depth, ask_depth, timestamp
        FROM orderbook_snapshots
        WHERE market_id = ?
        ORDER BY timestamp DESC
        LIMIT 2
    """, (market_id,))

    # Volume stats
    stats = db.query_one("""
        SELECT
            COUNT(*) as trade_count,
            COALESCE(SUM(size), 0) as total_volume,
            AVG(size) as avg_trade_size,
            MAX(size) as max_trade_size
        FROM trades
        WHERE market_id = ?
    """, (market_id,))

    result = {
        "market": market,
        "trades": trades,
        "price_timeline": price_timeline,
        "orderbook": orderbook,
        "stats": stats,
    }

    cache.put(key, result, CACHE_TTL["markets"])
    return result

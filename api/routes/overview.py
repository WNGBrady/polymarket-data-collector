"""GET /api/overview — Summary stats across all markets."""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL, WHALE_THRESHOLD

router = APIRouter()


@router.get("/overview")
def overview(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("overview", game, date_start, date_end)
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
            COUNT(DISTINCT m.market_id) as total_markets,
            COUNT(t.id) as total_trades,
            COALESCE(SUM(t.size), 0) as total_volume,
            COALESCE(SUM(CASE WHEN t.size >= ? THEN t.size ELSE 0 END), 0) as whale_volume,
            COUNT(CASE WHEN t.size >= ? THEN 1 END) as whale_trades
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        {where_sql}
    """, (WHALE_THRESHOLD, WHALE_THRESHOLD, *params))

    # Per-game breakdown
    games_data = {}
    for g in ["cod", "cs2"]:
        if game != "all" and game != g:
            continue

        # Build per-game WHERE: always filter by game, plus date filters
        g_clauses = ["m.game = ?"]
        g_params: list = [g]
        if date_start:
            g_clauses.append("t.timestamp >= strftime('%s', ?)")
            g_params.append(date_start)
        if date_end:
            g_clauses.append("t.timestamp <= strftime('%s', ?, '+1 day')")
            g_params.append(date_end)

        g_where = "WHERE " + " AND ".join(g_clauses)

        g_row = db.query_one(f"""
            SELECT
                COUNT(DISTINCT m.market_id) as markets,
                COUNT(t.id) as trades,
                COALESCE(SUM(t.size), 0) as volume,
                COALESCE(SUM(CASE WHEN t.size >= ? THEN t.size ELSE 0 END), 0) as whale_volume
            FROM trades t
            JOIN markets m ON t.market_id = m.market_id
            {g_where}
        """, (WHALE_THRESHOLD, *g_params))

        vol = g_row["volume"] if g_row else 0
        wv = g_row["whale_volume"] if g_row else 0
        games_data[g] = {
            "markets": g_row["markets"] if g_row else 0,
            "trades": g_row["trades"] if g_row else 0,
            "volume": vol,
            "whale_volume": wv,
            "whale_pct": round(wv / vol * 100, 1) if vol > 0 else 0,
        }

    total_vol = row["total_volume"] if row else 0
    whale_vol = row["whale_volume"] if row else 0

    result = {
        "total_markets": row["total_markets"] if row else 0,
        "total_trades": row["total_trades"] if row else 0,
        "total_volume": total_vol,
        "whale_volume": whale_vol,
        "whale_pct": round(whale_vol / total_vol * 100, 1) if total_vol > 0 else 0,
        "whale_trades": row["whale_trades"] if row else 0,
        "games": games_data,
    }

    cache.put(key, result, CACHE_TTL["overview"])
    return result

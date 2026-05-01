"""GET /api/bot-overview — bot vs human volume/count split + top bots.

Joins the per-trade `trades` table to per-wallet bot_label in `wallets`. Trades
without an attached wallet (pre-migration) are bucketed under 'unknown' so the
totals here always match `/api/overview`.
"""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/bot-overview")
def bot_overview(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    top_n: int = Query(10, ge=1, le=100),
):
    key = cache.make_key("bot_overview", game, date_start, date_end) + f":{top_n}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    where = []
    params: list = []
    if game != "all":
        where.append("m.game = ?")
        params.append(game)
    if date_start:
        where.append("t.timestamp >= strftime('%s', ?)")
        params.append(date_start)
    if date_end:
        where.append("t.timestamp <= strftime('%s', ?, '+1 day')")
        params.append(date_end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Bucket every trade by the wallet's bot_label. NULL/unknown trades go in
    # an 'unknown' bucket — typically pre-migration trades without proxy_wallet.
    rows = db.query_all(f"""
        SELECT
            COALESCE(w.bot_label, 'unknown') AS label,
            COUNT(*) AS n,
            COALESCE(SUM(t.size), 0) AS volume
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        LEFT JOIN wallets w ON w.proxy_wallet = t.proxy_wallet
        {where_sql}
        GROUP BY label
    """, tuple(params))

    by_label = {
        r["label"]: {"trades": r["n"], "volume": r["volume"]}
        for r in rows
    }
    total_vol = sum(d["volume"] for d in by_label.values())
    total_n = sum(d["trades"] for d in by_label.values())
    for d in by_label.values():
        d["volume_pct"] = round(d["volume"] / total_vol * 100, 1) if total_vol else 0.0
        d["trades_pct"] = round(d["trades"] / total_n * 100, 1) if total_n else 0.0

    # Top wallets by volume within the same filter window
    top_rows = db.query_all(f"""
        SELECT
            t.proxy_wallet,
            w.pseudonym,
            COALESCE(w.bot_label, 'unknown') AS bot_label,
            w.bot_score,
            COUNT(*) AS n_trades,
            COALESCE(SUM(t.size), 0) AS volume
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        LEFT JOIN wallets w ON w.proxy_wallet = t.proxy_wallet
        {where_sql}
        {('AND' if where_sql else 'WHERE')} t.proxy_wallet IS NOT NULL
        GROUP BY t.proxy_wallet
        ORDER BY volume DESC
        LIMIT ?
    """, (*params, top_n))

    result = {
        "by_label": by_label,
        "total_volume": total_vol,
        "total_trades": total_n,
        "top_wallets": top_rows,
    }

    cache.put(key, result, CACHE_TTL["charts"])
    return result

"""GET /api/wallet/{proxy_wallet} — full wallet profile.

Returns the row from `wallets`, all CS2 signals, and a paginated tail of recent
trades. Wallets are case-insensitive and stored lowercased by the trade
collector, so the address is normalised here too.
"""

from fastapi import APIRouter, HTTPException, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/wallet/{proxy_wallet}")
def wallet_profile(
    proxy_wallet: str,
    recent_trades: int = Query(50, ge=0, le=500),
):
    wallet = (proxy_wallet or "").strip().lower()
    if not wallet.startswith("0x") or len(wallet) < 6:
        raise HTTPException(status_code=400, detail="proxy_wallet must be a 0x-prefixed address")

    key = f"wallet_profile:{wallet}:{recent_trades}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    profile = db.query_one("SELECT * FROM wallets WHERE proxy_wallet = ?", (wallet,))
    if not profile:
        # Wallet may have trades but no aggregation yet
        seen = db.query_one(
            "SELECT MIN(timestamp) AS first_seen, MAX(timestamp) AS last_seen, COUNT(*) AS n "
            "FROM trades WHERE proxy_wallet = ?",
            (wallet,),
        )
        if not seen or not seen["n"]:
            raise HTTPException(status_code=404, detail="wallet not found")
        profile = {
            "proxy_wallet": wallet,
            "first_seen_ts": seen["first_seen"],
            "last_seen_ts": seen["last_seen"],
            "total_trades": seen["n"],
            "bot_label": None,
            "bot_score": None,
        }

    signals = db.query_all(
        "SELECT signal_name, signal_value, n_observations, computed_at_ts "
        "FROM cs2_wallet_signals WHERE proxy_wallet = ?",
        (wallet,),
    )

    trades = db.query_all(
        """
        SELECT t.timestamp, t.market_id, m.question, m.game, t.price, t.size,
               t.side, t.outcome, t.transaction_hash
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        WHERE t.proxy_wallet = ?
        ORDER BY t.timestamp DESC
        LIMIT ?
        """,
        (wallet, recent_trades),
    ) if recent_trades > 0 else []

    # Per-market summary so the UI can render a quick "most-traded markets" panel
    by_market = db.query_all(
        """
        SELECT t.market_id, m.question, m.game,
               COUNT(*) AS n, COALESCE(SUM(t.size), 0) AS volume,
               MIN(t.timestamp) AS first_ts, MAX(t.timestamp) AS last_ts
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        WHERE t.proxy_wallet = ?
        GROUP BY t.market_id
        ORDER BY volume DESC
        LIMIT 50
        """,
        (wallet,),
    )

    result = {
        "profile": profile,
        "cs2_signals": {s["signal_name"]: s for s in signals},
        "recent_trades": trades,
        "top_markets": by_market,
    }

    cache.put(key, result, CACHE_TTL["charts"])
    return result

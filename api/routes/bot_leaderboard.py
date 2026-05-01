"""GET /api/bot-leaderboard — top wallets by volume with bot_label + signals.

Drives a "who are the top bots" table. Joins each wallet to its CS2 signals
in a single query so the frontend can render label + Pinnacle-follow score
without a per-row request.
"""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/bot-leaderboard")
def bot_leaderboard(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    bot_label: str = Query("", pattern="^(human|likely_bot|bot|market_maker|)$"),
    sort_by: str = Query("volume", pattern="^(volume|trades|markets|bot_score)$"),
    limit: int = Query(50, ge=1, le=500),
):
    key = f"bot_leaderboard:{game}:{bot_label}:{sort_by}:{limit}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    where = ["w.proxy_wallet IS NOT NULL"]
    params: list = []
    if bot_label:
        where.append("w.bot_label = ?")
        params.append(bot_label)

    if game != "all":
        # Per-game volume comes from re-aggregating trades to enforce the game
        # filter; total_volume_usd in the wallets table is cross-game.
        sql = f"""
        WITH per_wallet AS (
            SELECT
                t.proxy_wallet,
                COUNT(*) AS trades,
                COUNT(DISTINCT t.market_id) AS markets,
                COALESCE(SUM(t.size), 0) AS volume
            FROM trades t
            JOIN markets m ON t.market_id = m.market_id
            WHERE m.game = ?
              AND t.proxy_wallet IS NOT NULL
            GROUP BY t.proxy_wallet
        )
        SELECT
            w.proxy_wallet, w.pseudonym, w.name, w.bot_label, w.bot_score,
            w.first_seen_ts, w.last_seen_ts, w.inter_trade_cv, w.round_size_share,
            w.markets_per_day, w.two_sided_ratio,
            p.trades, p.markets, p.volume
        FROM per_wallet p
        JOIN wallets w ON w.proxy_wallet = p.proxy_wallet
        WHERE {' AND '.join(where)}
        ORDER BY {{order_col}} DESC
        LIMIT ?
        """
        order_col = {
            "volume": "p.volume",
            "trades": "p.trades",
            "markets": "p.markets",
            "bot_score": "w.bot_score",
        }[sort_by]
        sql = sql.format(order_col=order_col)
        rows = db.query_all(sql, (game, *params, limit))
    else:
        order_col = {
            "volume": "w.total_volume_usd",
            "trades": "w.total_trades",
            "markets": "w.distinct_markets",
            "bot_score": "w.bot_score",
        }[sort_by]
        sql = f"""
        SELECT
            w.proxy_wallet, w.pseudonym, w.name, w.bot_label, w.bot_score,
            w.first_seen_ts, w.last_seen_ts, w.inter_trade_cv, w.round_size_share,
            w.markets_per_day, w.two_sided_ratio,
            w.total_trades AS trades, w.distinct_markets AS markets,
            w.total_volume_usd AS volume
        FROM wallets w
        WHERE {' AND '.join(where)}
        ORDER BY {order_col} DESC
        LIMIT ?
        """
        rows = db.query_all(sql, (*params, limit))

    # Pull all signals for the listed wallets in one query
    if rows:
        wallet_addrs = [r["proxy_wallet"] for r in rows]
        placeholders = ",".join("?" for _ in wallet_addrs)
        sig_rows = db.query_all(
            f"SELECT proxy_wallet, signal_name, signal_value, n_observations "
            f"FROM cs2_wallet_signals WHERE proxy_wallet IN ({placeholders})",
            tuple(wallet_addrs),
        )
        sigs_by_wallet: dict[str, dict] = {}
        for s in sig_rows:
            sigs_by_wallet.setdefault(s["proxy_wallet"], {})[s["signal_name"]] = {
                "value": s["signal_value"],
                "n": s["n_observations"],
            }
        for r in rows:
            r["cs2_signals"] = sigs_by_wallet.get(r["proxy_wallet"], {})

    result = {"wallets": rows, "filter": {"game": game, "bot_label": bot_label, "sort_by": sort_by}}
    cache.put(key, result, CACHE_TTL["charts"])
    return result

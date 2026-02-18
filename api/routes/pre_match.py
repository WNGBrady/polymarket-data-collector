"""GET /api/pre-match-movement — Active pre-match markets with trade-derived price timelines.

A market is "pre-match" when game_start_time > NOW().
"""

import time

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/pre-match-movement")
def pre_match_movement(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
):
    key = f"pre_match:{game}"
    cached = cache.get(key)
    if cached is not None:
        return cached

    # Check if game_start_time column exists on markets
    col_check = db.query_all("PRAGMA table_info(markets)")
    has_gst = any(c["name"] == "game_start_time" for c in col_check)

    if not has_gst:
        return {"markets": [], "note": "game_start_time column not yet added to markets table"}

    now_iso = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    where_clauses = ["m.game_start_time > ?"]
    params: list = [now_iso]

    if game != "all":
        where_clauses.append("m.game = ?")
        params.append(game)

    where_sql = "WHERE " + " AND ".join(where_clauses)

    # Get pre-match markets
    pre_match_markets = db.query_all(f"""
        SELECT m.market_id, m.question, m.game, m.game_start_time, m.outcomes
        FROM markets m
        {where_sql}
        ORDER BY m.game_start_time ASC
    """, tuple(params))

    results = []
    for market in pre_match_markets:
        mid = market["market_id"]

        # Get all trades for price timeline
        trades = db.query_all("""
            SELECT timestamp, price, size, side, outcome
            FROM trades
            WHERE market_id = ?
            ORDER BY timestamp ASC
        """, (mid,))

        if not trades:
            continue

        # Group by outcome for per-team timelines
        outcomes: dict[str, list[dict]] = {}
        for t in trades:
            outcome = t["outcome"] or "Unknown"
            if outcome not in outcomes:
                outcomes[outcome] = []
            outcomes[outcome].append(t)

        # Current price per outcome (most recent trade)
        current_prices: dict[str, float] = {}
        for outcome, outcome_trades in outcomes.items():
            current_prices[outcome] = outcome_trades[-1]["price"]

        # If only one outcome has trades, infer the other
        if len(current_prices) == 1:
            known_outcome = list(current_prices.keys())[0]
            known_price = current_prices[known_outcome]
            # Try to find the other outcome name from the market outcomes field
            inferred_price = round(1 - known_price, 4)
            current_prices[f"Other (inferred)"] = inferred_price

        # Determine favored side
        favored = max(current_prices, key=current_prices.get) if current_prices else None

        results.append({
            "market_id": mid,
            "question": market["question"],
            "game": market["game"],
            "game_start_time": market["game_start_time"],
            "current_prices": current_prices,
            "favored": favored,
            "trade_count": len(trades),
            "timeline": {outcome: [{"timestamp": t["timestamp"], "price": t["price"]}
                                   for t in outcome_trades]
                         for outcome, outcome_trades in outcomes.items()},
        })

    result = {"markets": results}
    cache.put(key, result, CACHE_TTL["pre_match"])
    return result

"""GET /api/closing-lines — Match closing prices and favorite win stats.

Reads from the closing_lines table (populated from CSV or cron).
"""

from fastapi import APIRouter, Query

from .. import cache, db
from ..config import CACHE_TTL

router = APIRouter()


@router.get("/closing-lines")
def closing_lines(
    game: str = Query("all", pattern="^(cod|cs2|all)$"),
    date_start: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    date_end: str = Query("", pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
):
    key = cache.make_key("closing_lines", game, date_start, date_end)
    cached = cache.get(key)
    if cached is not None:
        return cached

    # Check if table exists
    table_check = db.query_one(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='closing_lines'"
    )
    if not table_check:
        return {"data": [], "stats": {"total_matches": 0, "favorite_win_rate": 0, "avg_confidence": 0}}

    where_clauses: list[str] = []
    params: list = []

    if date_start:
        where_clauses.append("game_start_time >= ?")
        params.append(date_start)
    if date_end:
        where_clauses.append("game_start_time <= ? || ' 23:59:59'")
        params.append(date_end)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    rows = db.query_all(f"""
        SELECT
            game_id, market_id, home_team, away_team, team,
            is_home, question, game_start_time,
            closing_price, min_price, max_price,
            final_score, team_won, n_trades
        FROM closing_lines
        {where_sql}
        ORDER BY game_start_time DESC, is_home DESC
    """, tuple(params))

    # Compute favorite win stats from home-team rows
    total_matches = 0
    fav_wins = 0
    fav_confidences: list[float] = []

    # Group by game_id
    matches: dict[str, dict] = {}
    for r in rows:
        gid = r["game_id"]
        if gid not in matches:
            matches[gid] = {}
        if r["is_home"]:
            matches[gid]["home"] = r
        else:
            matches[gid]["away"] = r

    for gid, m in matches.items():
        home = m.get("home")
        away = m.get("away")
        if not home or not away:
            continue

        fav_is_home = home["closing_price"] > 0.5
        fav_cl = home["closing_price"] if fav_is_home else away["closing_price"]
        fav_won = home["team_won"] if fav_is_home else away["team_won"]

        if fav_won is not None:
            total_matches += 1
            fav_wins += int(fav_won)
            fav_confidences.append(fav_cl)

    fav_win_rate = round(fav_wins / total_matches * 100, 1) if total_matches > 0 else 0
    avg_conf = round(sum(fav_confidences) / len(fav_confidences) * 100, 1) if fav_confidences else 0

    result = {
        "data": rows,
        "stats": {
            "total_matches": total_matches,
            "favorite_wins": fav_wins,
            "favorite_win_rate": fav_win_rate,
            "avg_confidence": avg_conf,
        },
    }

    cache.put(key, result, CACHE_TTL["closing_lines"])
    return result

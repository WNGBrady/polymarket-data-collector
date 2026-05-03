"""Seed kalshi_markets with the eight NaVi vs Vitality (BLAST Rivals 2026-05-03)
ticker rows, linked back to their Polymarket equivalents.

Run once before the match (idempotent — re-running just refreshes status/close_time):

    python tools/seed_kalshi_markets.py

Polymarket runbook is RUNBOOK_NAVI_VIT_2026-05-03.md.

Note on format: Polymarket lists this as BO5 (parent + maps 1-4 + an optional
map 5 if 2-2). Kalshi books it as BO3 (parent + maps 1-3). The map_num column
on each kalshi_markets row is the *Kalshi* map number; the polymarket_market_id
column points at the Polymarket per-map market with the same map number. There
is no Polymarket counterpart to a Kalshi BO3 parent — we point Kalshi's BO3
parent at the Polymarket BO5 parent (2143221) since both resolve on overall
match winner.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `src` importable when run from the repo root
_repo_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_repo_root))

from src.database import init_database, migrate_database, upsert_kalshi_market
from src.kalshi import fetch_market


# (kalshi_ticker, polymarket_market_id, team_label, side, map_num)
SEEDS = [
    # BO3 / overall match winner
    ("KXCS2GAME-26MAY031330VITNAVI-VIT",  "2143221", "Vitality",       "yes", None),
    ("KXCS2GAME-26MAY031330VITNAVI-NAVI", "2143221", "Natus Vincere",  "yes", None),
    # Per-map
    ("KXCS2MAP-26MAY031330VITNAVI-1-VIT",  "2143222", "Vitality",      "yes", 1),
    ("KXCS2MAP-26MAY031330VITNAVI-1-NAVI", "2143222", "Natus Vincere", "yes", 1),
    ("KXCS2MAP-26MAY031330VITNAVI-2-VIT",  "2143223", "Vitality",      "yes", 2),
    ("KXCS2MAP-26MAY031330VITNAVI-2-NAVI", "2143223", "Natus Vincere", "yes", 2),
    ("KXCS2MAP-26MAY031330VITNAVI-3-VIT",  "2143224", "Vitality",      "yes", 3),
    ("KXCS2MAP-26MAY031330VITNAVI-3-NAVI", "2143224", "Natus Vincere", "yes", 3),
]


def derive_series(event_ticker: str | None) -> str | None:
    if not event_ticker:
        return None
    # KXCS2MAP-26MAY031330VITNAVI-1 -> KXCS2MAP
    return event_ticker.split("-", 1)[0]


def derive_match_id(event_ticker: str | None) -> str | None:
    if not event_ticker:
        return None
    parts = event_ticker.split("-")
    return parts[1] if len(parts) > 1 else None


def main() -> int:
    migrate_database()
    init_database()

    rows_seeded = 0
    for ticker, poly_id, team, side, map_num in SEEDS:
        meta = fetch_market(ticker) or {}
        event_ticker = meta.get("event_ticker")
        upsert_kalshi_market({
            "ticker": ticker,
            "event_ticker": event_ticker,
            "series_ticker": derive_series(event_ticker),
            "title": meta.get("title"),
            "team_label": team,
            "side": side,
            "polymarket_market_id": poly_id,
            "map_num": map_num,
            "match_id_text": derive_match_id(event_ticker),
            "status": meta.get("status"),
            "open_time": meta.get("open_time"),
            "close_time": meta.get("close_time"),
        })
        rows_seeded += 1
        print(f"  seeded {ticker} -> polymarket {poly_id} ({team}, map={map_num}, status={meta.get('status')})")

    print(f"Done. {rows_seeded} kalshi_markets rows upserted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Pre-warm pinnacle_match_links for the per-map sub-markets of a Polymarket
event before kickoff, so the live collector has zero startup delay.

Usage:
    python tools/prewarm_pinnacle.py --slug cs2-navi-vit-2026-05-03
    python tools/prewarm_pinnacle.py --event-id 12345
    python tools/prewarm_pinnacle.py --slug ... --force-pin <market_id>=<pin_match_id>[:<bookmaker>]

Without --force-pin: runs the same fuzzy match the realtime collector uses
(`pinnacle.attempt_link_for_market`) and persists hits via `upsert_pinnacle_link`.
With --force-pin: looks up the pinnacle match by id (across all books or the
specified one) and persists the resulting link manually for the given market.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Make `src` importable when invoked as a script
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from src import pinnacle  # noqa: E402
from src.config import GAMMA_API_URL  # noqa: E402
from src.database import (  # noqa: E402
    get_connection,
    get_pinnacle_link,
    init_database,
    migrate_database,
    upsert_market,
    upsert_pinnacle_link,
)
from src.market_discovery import extract_market_data  # noqa: E402


def fetch_event_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    r = requests.get(f"{GAMMA_API_URL}/events", params={"slug": slug}, timeout=15)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    return None


def fetch_event_by_id(event_id: str) -> Optional[Dict[str, Any]]:
    r = requests.get(f"{GAMMA_API_URL}/events/{event_id}", timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def upsert_event_markets(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Upsert every market under the event into the local DB and return them."""
    raw_markets = event.get("markets") or []
    out: List[Dict[str, Any]] = []
    for raw in raw_markets:
        md = extract_market_data(raw, event=event)
        # extract_market_data returns the polymarket schema. Force CS2 since
        # this tool is CS2-specific (matches pinnacle's CS2-only coverage).
        md["game"] = "cs2"
        upsert_market(md)
        out.append(md)
    return out


def find_pinnacle_match(
    pin_match_id: str,
    bookmaker: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    matches = pinnacle.list_matches(bookmaker=bookmaker)
    for m in matches:
        if str(m.get("match_id")) == str(pin_match_id):
            return m
    return None


def parse_force_pin(spec: str) -> Dict[str, Any]:
    """Parse a single --force-pin spec: market_id=pin_match_id[:bookmaker]"""
    if "=" not in spec:
        raise ValueError(f"--force-pin must be MARKET_ID=PIN_MATCH_ID[:BOOK], got {spec!r}")
    market_id, rhs = spec.split("=", 1)
    if ":" in rhs:
        pin_id, book = rhs.split(":", 1)
    else:
        pin_id, book = rhs, None
    return {"market_id": market_id.strip(), "pin_match_id": pin_id.strip(), "bookmaker": book}


def force_link(market_id: str, pin_match_id: str, bookmaker: Optional[str]) -> bool:
    pin_match = find_pinnacle_match(pin_match_id, bookmaker=bookmaker)
    if pin_match is None:
        print(f"  ! pin_match_id {pin_match_id} not found in cs2odds (book={bookmaker or 'any'})")
        return False
    map_num = pinnacle.infer_map_num(_market_question(market_id) or "")
    upsert_pinnacle_link(
        market_id=market_id,
        pin_match_id=pin_match.get("match_id"),
        pin_map_num=map_num,
        home_team=pin_match.get("home"),
        away_team=pin_match.get("away"),
        link_method="manual",
        confidence=1.0,
        bookmaker=pin_match.get("bookmaker") or bookmaker or "ps3838",
        canonical_match_id=pin_match.get("canonical_match_id"),
    )
    print(
        f"  [ok] FORCED  {market_id} -> {pin_match.get('bookmaker')}/{pin_match.get('match_id')} "
        f"{pin_match.get('home')} vs {pin_match.get('away')} (map={map_num})"
    )
    return True


def _market_question(market_id: str) -> Optional[str]:
    cur = get_connection().cursor()
    cur.execute("SELECT question FROM markets WHERE market_id = ?", (market_id,))
    row = cur.fetchone()
    return row["question"] if row else None


def attempt_and_persist(market: Dict[str, Any], cached_matches: List[Dict[str, Any]]) -> str:
    """Replicates RealtimeCollector._auto_link_market without the collector instance."""
    market_id = market.get("market_id")
    method, pin_match, confidence = pinnacle.attempt_link_for_market(market, matches=cached_matches)

    if method == "pinnacle-down":
        print(f"  ! cs2odds unreachable while linking {market_id}")
        return method
    if method == "outright":
        print(f"  - skipped outright '{(market.get('question') or '')[:60]}'")
        return method
    if method == "unmatched":
        upsert_pinnacle_link(
            market_id=market_id,
            pin_match_id=None, pin_map_num=None,
            home_team=None, away_team=None,
            link_method="unmatched", confidence=None,
        )
        print(f"  ? UNMATCHED  {market_id}  '{(market.get('question') or '')[:60]}'")
        return method

    map_num = pinnacle.infer_map_num(market.get("question") or "")
    anchor_book = pin_match.get("bookmaker") or "ps3838"
    canonical_id = pin_match.get("canonical_match_id")
    upsert_pinnacle_link(
        market_id=market_id,
        pin_match_id=pin_match.get("match_id"),
        pin_map_num=map_num,
        home_team=pin_match.get("home"),
        away_team=pin_match.get("away"),
        link_method="auto-fuzzy",
        confidence=confidence,
        bookmaker=anchor_book,
        canonical_match_id=canonical_id,
    )
    print(
        f"  [ok] LINKED    {market_id}  '{(market.get('question') or '')[:50]}' -> "
        f"{anchor_book}/{pin_match.get('match_id')} "
        f"{pin_match.get('home')} vs {pin_match.get('away')} "
        f"(map={map_num}, canon={canonical_id}, conf={confidence:.2f})"
    )
    return method


def print_summary(markets: List[Dict[str, Any]]) -> None:
    rows = []
    for m in markets:
        link = get_pinnacle_link(m.get("market_id")) or {}
        rows.append({
            "market_id": m.get("market_id"),
            "question": (m.get("question") or "")[:60],
            "method": link.get("link_method"),
            "bookmaker": link.get("bookmaker"),
            "pin_match_id": link.get("pin_match_id"),
            "canonical": link.get("canonical_match_id"),
            "home": link.get("home_team"),
            "away": link.get("away_team"),
            "map": link.get("pin_map_num"),
        })
    print("\n=== Final pinnacle_match_links state ===")
    for r in rows:
        print(json.dumps(r, default=str))


def list_unmatched_candidates(markets: List[Dict[str, Any]]) -> None:
    """For markets that didn't link, show the top 5 fuzzy candidates from cs2odds."""
    matches = pinnacle.list_matches()
    print("\n=== Top fuzzy candidates for UNMATCHED markets ===")
    for m in markets:
        link = get_pinnacle_link(m.get("market_id")) or {}
        if link.get("link_method") in ("auto-fuzzy", "manual"):
            continue
        if pinnacle.is_outright_question(m.get("question") or ""):
            continue
        cands = pinnacle.extract_team_candidates(m.get("question") or "")
        print(f"\n  market {m.get('market_id')}  '{(m.get('question') or '')[:80]}'")
        print(f"    extracted candidates: {cands!r}")
        scored = []
        for pm in matches:
            if not pm.get("home") or not pm.get("away"):
                continue
            if len(cands) >= 2:
                a = max(pinnacle._ratio(cands[0], pm.get("home") or ""),
                        pinnacle._ratio(cands[0], pm.get("away") or ""))
                b = max(pinnacle._ratio(cands[1], pm.get("home") or ""),
                        pinnacle._ratio(cands[1], pm.get("away") or ""))
                score = (a + b) / 2
            else:
                score = 0.0
            scored.append((score, pm))
        scored.sort(key=lambda x: x[0], reverse=True)
        for score, pm in scored[:5]:
            print(
                f"      [{score:.2f}] {pm.get('bookmaker')}/{pm.get('match_id')} "
                f"{pm.get('home')} vs {pm.get('away')} feed={pm.get('feed')}"
            )


def main():
    p = argparse.ArgumentParser(description="Prewarm pinnacle_match_links for an event")
    p.add_argument("--slug", help="Polymarket event slug (e.g. cs2-navi-vit-2026-05-03)")
    p.add_argument("--event-id", help="Polymarket event id")
    p.add_argument(
        "--force-pin",
        action="append",
        default=[],
        help="Force a manual link: MARKET_ID=PIN_MATCH_ID[:BOOK]. Repeatable.",
    )
    p.add_argument(
        "--show-candidates",
        action="store_true",
        help="For markets that don't auto-link, print the top fuzzy candidates from cs2odds.",
    )
    args = p.parse_args()

    if not args.slug and not args.event_id and not args.force_pin:
        p.error("Pass at least --slug, --event-id, or --force-pin")

    migrate_database()
    init_database()

    if not pinnacle.is_available():
        print("! cs2odds at PINNACLE_API_URL is not reachable. Check the daemon.", file=sys.stderr)
        sys.exit(2)

    event = None
    if args.event_id:
        event = fetch_event_by_id(args.event_id)
    elif args.slug:
        event = fetch_event_by_slug(args.slug)

    markets: List[Dict[str, Any]] = []
    if event:
        print(f"Event: id={event.get('id')} slug={event.get('slug')} title={event.get('title')!r}")
        markets = upsert_event_markets(event)
        print(f"  upserted {len(markets)} market(s) into local DB")

    # Apply forced pins first; they take precedence over auto-link.
    if args.force_pin:
        print("\n=== Applying --force-pin overrides ===")
        for spec in args.force_pin:
            cfg = parse_force_pin(spec)
            force_link(cfg["market_id"], cfg["pin_match_id"], cfg["bookmaker"])

    # Auto-link any market without a manual override.
    if markets:
        forced_ids = {parse_force_pin(s)["market_id"] for s in args.force_pin}
        try:
            cached_matches = pinnacle.list_matches()
        except requests.RequestException as e:
            print(f"! cs2odds list_matches failed: {e}", file=sys.stderr)
            sys.exit(2)

        print(f"\n=== Auto-linking {len(markets) - len(forced_ids)} market(s) ===")
        for m in markets:
            if m.get("market_id") in forced_ids:
                continue
            link = get_pinnacle_link(m.get("market_id"))
            if link and link.get("link_method") in ("auto-fuzzy", "manual"):
                # already linked from a previous run; skip unless it's stale
                print(
                    f"  = already linked  {m.get('market_id')} -> "
                    f"{link.get('bookmaker')}/{link.get('pin_match_id')} "
                    f"{link.get('home_team')} vs {link.get('away_team')}"
                )
                continue
            attempt_and_persist(m, cached_matches)

        print_summary(markets)

        if args.show_candidates:
            list_unmatched_candidates(markets)


if __name__ == "__main__":
    main()

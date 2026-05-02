"""Batch Pinnacle link backfill.

The realtime collector only attempts fuzzy-matching when an orderbook is polled,
and only against the cs2odds in-memory match list at that exact moment. Markets
that were already closed when the collector picked them up — or that were created
before cs2odds was tracking the underlying match — never get a link.

This module walks every CS2 market and tries to link them in two passes:

  Phase A (fuzzy): for each unlinked or stale-unmatched H2H market, run the
  same attempt_link_for_market pass that the collector uses, but against a
  single cached /matches response so 4000+ markets cost one HTTP call.

  Phase B (event inheritance): sub-markets like "Games Total: O/U 2.5" or
  "Map Handicap: NAVI (-1.5) vs GamerLegion" are unmatchable by question text
  alone, but share an event_id with a parent H2H market that does have a link.
  Copy the parent's pin_match_id (and per-sub-market pin_map_num inferred from
  the question) onto every unlinked sibling under a linked event.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from . import pinnacle
from .database import get_connection, upsert_pinnacle_link
from .utils import logger


# Re-attempt unmatched links older than this many hours. Mirrors the realtime
# collector's TTL, but exposed as a parameter for ad-hoc backfill runs.
DEFAULT_UNMATCHED_RETRY_HOURS = 4


def _select_candidates(
    conn,
    game: str,
    include_stale_hours: int,
) -> List[Dict[str, Any]]:
    """Markets needing a link attempt: never tried, or unmatched older than TTL."""
    cutoff = int(time.time()) - include_stale_hours * 3600
    sql = """
        SELECT m.market_id, m.question, m.event_id, m.end_date,
               l.link_method, l.linked_at
        FROM markets m
        LEFT JOIN pinnacle_match_links l ON l.market_id = m.market_id
        WHERE m.game = ?
          AND (
            l.market_id IS NULL
            OR (l.link_method = 'unmatched' AND COALESCE(l.linked_at, 0) < ?)
          )
        ORDER BY m.end_date DESC
    """
    cur = conn.cursor()
    rows = cur.execute(sql, (game, cutoff)).fetchall()
    return [dict(r) for r in rows]


def _phase_fuzzy(
    conn,
    candidates: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Run attempt_link_for_market against a single cached /matches response."""
    counts = {"linked": 0, "unmatched": 0, "outright": 0, "skipped_no_pinnacle": 0}

    if not candidates:
        return counts

    try:
        matches = pinnacle.list_matches()
    except Exception as e:
        logger.warning(f"pinnacle backfill: list_matches failed ({e}); skipping phase A")
        counts["skipped_no_pinnacle"] = len(candidates)
        return counts

    logger.info(f"pinnacle backfill phase A: trying {len(candidates)} markets against {len(matches)} cs2odds matches")

    for c in candidates:
        method, pin_match, confidence = pinnacle.attempt_link_for_market(c, matches=matches)

        if method == "outright":
            counts["outright"] += 1
            continue

        if method == "unmatched":
            upsert_pinnacle_link(
                market_id=c["market_id"],
                pin_match_id=None, pin_map_num=None,
                home_team=None, away_team=None,
                link_method="unmatched", confidence=None,
            )
            counts["unmatched"] += 1
            continue

        if method == "auto-fuzzy" and pin_match is not None:
            map_num = pinnacle.infer_map_num(c.get("question") or "")
            upsert_pinnacle_link(
                market_id=c["market_id"],
                pin_match_id=pin_match.get("match_id"),
                pin_map_num=map_num,
                home_team=pin_match.get("home"),
                away_team=pin_match.get("away"),
                link_method="auto-fuzzy",
                confidence=confidence,
                bookmaker=pin_match.get("bookmaker") or "ps3838",
                canonical_match_id=pin_match.get("canonical_match_id"),
            )
            counts["linked"] += 1
            logger.debug(
                f"pinnacle backfill: linked '{(c.get('question') or '')[:50]}' -> "
                f"{pin_match.get('bookmaker')}/{pin_match.get('match_id')} "
                f"{pin_match.get('home')} vs {pin_match.get('away')} (conf={confidence:.2f})"
            )

    return counts


def _phase_event_inherit(conn) -> Dict[str, int]:
    """Copy auto-fuzzy links to unlinked siblings sharing the same event_id.

    Sub-markets like "Games Total: O/U 2.5" don't carry team names but belong
    to the same event_id as their H2H parent. Per-sibling pin_map_num is
    re-inferred from the sibling's own question (so "Map 2: Odd/Even" inherits
    the parent's match_id with map_num=2).
    """
    counts = {"inherited": 0, "no_event_id": 0}

    sql = """
        SELECT
            child.market_id    AS child_market_id,
            child.question     AS child_question,
            parent.pin_match_id,
            parent.home_team,
            parent.away_team,
            parent.confidence,
            parent.bookmaker,
            parent.canonical_match_id
        FROM markets child
        JOIN markets pm ON pm.event_id = child.event_id AND pm.market_id != child.market_id
        JOIN pinnacle_match_links parent ON parent.market_id = pm.market_id
        LEFT JOIN pinnacle_match_links existing ON existing.market_id = child.market_id
        WHERE child.game = 'cs2'
          AND child.event_id IS NOT NULL
          AND parent.link_method = 'auto-fuzzy'
          AND parent.pin_match_id IS NOT NULL
          AND (existing.market_id IS NULL OR existing.link_method != 'auto-fuzzy')
        GROUP BY child.market_id
    """
    cur = conn.cursor()
    rows = cur.execute(sql).fetchall()

    for r in rows:
        d = dict(r)
        map_num = pinnacle.infer_map_num(d.get("child_question") or "")
        # Inherited links are weaker: they're true if the parent's link is true,
        # so cap the confidence at 0.99 to mark them as derivative.
        conf = min(0.99, d.get("confidence") or 0.99)
        upsert_pinnacle_link(
            market_id=d["child_market_id"],
            pin_match_id=d["pin_match_id"],
            pin_map_num=map_num,
            home_team=d["home_team"],
            away_team=d["away_team"],
            link_method="event-inherited",
            confidence=conf,
            bookmaker=d.get("bookmaker") or "ps3838",
            canonical_match_id=d.get("canonical_match_id"),
        )
        counts["inherited"] += 1

    return counts


def backfill_links(
    game: str = "cs2",
    include_stale_hours: int = DEFAULT_UNMATCHED_RETRY_HOURS,
) -> Dict[str, Any]:
    """Run both backfill phases. Returns a summary dict suitable for logging."""
    conn = get_connection()

    candidates = _select_candidates(conn, game=game, include_stale_hours=include_stale_hours)
    phase_a = _phase_fuzzy(conn, candidates)
    phase_b = _phase_event_inherit(conn)

    summary = {
        "game": game,
        "candidates": len(candidates),
        "phase_fuzzy": phase_a,
        "phase_event_inherit": phase_b,
    }
    logger.info(f"pinnacle backfill complete: {summary}")
    return summary

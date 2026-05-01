"""Client + matcher for the cs2odds local HTTP API.

The cs2odds daemon (a separate systemd service) runs an aiohttp server bound to
127.0.0.1:8765 that exposes the freshest in-memory pinnacle CS2 odds. We hit it
synchronously at the moment of each polymarket orderbook snapshot so the two
sides are time-aligned, then store a no-vig snapshot in pinnacle_snapshots.

Match linking is best-effort fuzzy team-name matching. Polymarket questions are
free-form ("Will Vitality beat Spirit?", "Vitality vs G2 — Map 3 winner") so we
extract candidate team names from the question, compare to pinnacle's home/away
strings, and emit a confidence score using SequenceMatcher's ratio. Above the
threshold we record the link; below, we mark 'unmatched' and skip future
attempts until manually overridden.
"""
from __future__ import annotations

import re
import time
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import requests

from .config import PINNACLE_API_URL, PINNACLE_FUZZY_THRESHOLD, PINNACLE_HTTP_TIMEOUT
from .utils import logger


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


def list_matches() -> List[Dict[str, Any]]:
    """GET /matches — every match cs2odds currently knows about."""
    r = requests.get(f"{PINNACLE_API_URL}/matches", timeout=PINNACLE_HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_match_odds(pin_match_id: int) -> Optional[Dict[str, Any]]:
    """GET /odds?match_id=... — returns None if cs2odds doesn't know the match."""
    try:
        r = requests.get(
            f"{PINNACLE_API_URL}/odds",
            params={"match_id": pin_match_id},
            timeout=PINNACLE_HTTP_TIMEOUT,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.debug(f"pinnacle: fetch_match_odds({pin_match_id}) failed: {e}")
        return None


def is_available() -> bool:
    """Cheap liveness check used by the realtime collector before the first fetch."""
    try:
        r = requests.get(f"{PINNACLE_API_URL}/health", timeout=PINNACLE_HTTP_TIMEOUT)
        return r.ok
    except requests.RequestException:
        return False


# ---------------------------------------------------------------------------
# No-vig math
# ---------------------------------------------------------------------------


def implied(dec: Optional[float]) -> Optional[float]:
    """Implied probability from decimal odds. Carries the vig — for direct
    polymarket comparison, but not for fair-value modeling."""
    if dec is None or dec <= 1:
        return None
    return 1.0 / dec


def novig_two_way(dec_a: Optional[float], dec_b: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """Strip the vig from a two-way market via proportional method.

    Returns (p_a, p_b) on the 0-1 scale. Both legs must be present and >1.0
    decimal — Pinnacle suspended legs come through as None.
    """
    p_a, p_b = implied(dec_a), implied(dec_b)
    if p_a is None or p_b is None:
        return (None, None)
    total = p_a + p_b
    if total <= 0:
        return (None, None)
    return (p_a / total, p_b / total)


def novig_three_way(
    dec_home: Optional[float],
    dec_away: Optional[float],
    dec_draw: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Strip the vig from a three-way (home/away/draw) market. If draw isn't
    offered (None), falls back to two-way and returns (h, a, None)."""
    if dec_draw is None:
        h, a = novig_two_way(dec_home, dec_away)
        return (h, a, None)
    p_h, p_a, p_d = implied(dec_home), implied(dec_away), implied(dec_draw)
    if p_h is None or p_a is None or p_d is None:
        return (None, None, None)
    total = p_h + p_a + p_d
    if total <= 0:
        return (None, None, None)
    return (p_h / total, p_a / total, p_d / total)


# ---------------------------------------------------------------------------
# Snapshot extraction
# ---------------------------------------------------------------------------


def extract_snapshot_for_map(
    odds: Dict[str, Any],
    pin_map_num: int,
) -> Optional[Dict[str, Any]]:
    """Pull the pin_map_num period out of an /odds response and shape it for
    insert_pinnacle_snapshot.

    Returns a dict ready to **kwarg into the DB helper, minus the orderbook FK
    and market_id (the caller adds those). Returns None if the requested period
    isn't present (e.g. pre-match BO winner with no per-map prices yet).
    """
    periods = odds.get("periods") or {}
    rows = periods.get(str(pin_map_num)) or periods.get(pin_map_num)
    if not rows:
        return None

    ml_home_dec = ml_away_dec = ml_draw_dec = None
    spreads_by_line: Dict[float, Dict[str, float]] = {}
    totals_by_line: Dict[float, Dict[str, float]] = {}

    for r in rows:
        market = r.get("market")
        side = r.get("side")
        line = r.get("line")
        price = r.get("price_decimal")  # cs2odds always serves decimal here
        if price is None:
            continue
        if market == "moneyline":
            if side == "home":
                ml_home_dec = price
            elif side == "away":
                ml_away_dec = price
            elif side == "draw":
                ml_draw_dec = price
        elif market == "spread" and isinstance(line, (int, float)):
            # Pinnacle emits home -1.5 and away +1.5 as separate rows for the
            # same handicap. Normalize to home's perspective so the two legs
            # land in one bucket and can be no-vig-paired.
            norm_line = float(line) if side == "home" else -float(line)
            slot = spreads_by_line.setdefault(norm_line, {})
            slot[side] = price
        elif market == "total" and isinstance(line, (int, float)):
            slot = totals_by_line.setdefault(float(line), {})
            slot[side] = price

    ml_h_nv, ml_a_nv, ml_d_nv = novig_three_way(ml_home_dec, ml_away_dec, ml_draw_dec)

    spreads = []
    for ln in sorted(spreads_by_line):
        slot = spreads_by_line[ln]
        h_dec, a_dec = slot.get("home"), slot.get("away")
        h_nv, a_nv = novig_two_way(h_dec, a_dec)
        spreads.append({
            "line": -ln,
            "home_implied": implied(a_dec), "away_implied": implied(h_dec),
            "home_novig": a_nv, "away_novig": h_nv,
        })

    totals = []
    for ln in sorted(totals_by_line):
        slot = totals_by_line[ln]
        o_dec, u_dec = slot.get("over"), slot.get("under")
        o_nv, u_nv = novig_two_way(o_dec, u_dec)
        totals.append({
            "line": ln,
            "over_implied": implied(o_dec), "under_implied": implied(u_dec),
            "over_novig": o_nv, "under_novig": u_nv,
        })

    match_meta = odds.get("match") or {}
    # cs2odds labels team strings as home=event[1]/away=event[2] but the price
    # arrays (moneyline[0], spread[3]) are anchored to a different team than the
    # event[1] string for CS2 — empirically verified against Polymarket prices on
    # 2026-05-01 G2 vs FaZe (PM mid aligns with Pinnacle's "away" no-vig within
    # 8pp; "home" diverges 30-80pp). Swap here so ml_home_* reflects the team
    # named in pinnacle_match_links.home_team. Spreads also flip the line sign.
    return {
        "pin_map_num": pin_map_num,
        "is_live": match_meta.get("feed") == "live",
        "ml_home_implied": implied(ml_away_dec),
        "ml_away_implied": implied(ml_home_dec),
        "ml_draw_implied": implied(ml_draw_dec),
        "ml_home_novig": ml_a_nv,
        "ml_away_novig": ml_h_nv,
        "ml_draw_novig": ml_d_nv,
        "spreads": spreads if spreads else None,
        "totals": totals if totals else None,
        "pin_observed_at_ms": odds.get("last_seen_ms"),
    }


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------


_TEAM_TOKEN_RE = re.compile(r"[A-Za-z0-9.\-']+(?:\s+[A-Za-z0-9.\-']+){0,3}")
_VS_RE = re.compile(r"\bvs?\.?\b|\b@\b|\bversus\b", re.IGNORECASE)
_MAP_NUM_RE = re.compile(r"\b(?:map|game)\s*(\d)\b", re.IGNORECASE)


_TEAM_NOISE_TOKENS = {"team", "esports", "esport", "gaming", "the"}


def _normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", " ", s.lower()).strip()


def _tokens(s: str) -> set[str]:
    return {t for t in _normalize(s).split() if t and t not in _TEAM_NOISE_TOKENS}


def _ratio(a: str, b: str) -> float:
    """Hybrid score: max of (token-overlap fraction, SequenceMatcher ratio).

    Token overlap handles "Vitality" vs "Team Vitality" (1.0). SequenceMatcher
    handles typos and short-form names ("NaVi" vs "Natus Vincere"). Whichever
    is more permissive wins — these are dual-use cues.
    """
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()
    smaller, larger = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    token_score = sum(1 for t in smaller if t in larger) / len(smaller)
    seq_score = SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()
    return max(token_score, seq_score)


def infer_map_num(question: str) -> int:
    """Map num inferred from the question text. 0 = match-level (BO winner)."""
    if not question:
        return 0
    m = _MAP_NUM_RE.search(question)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return 0
    return 0


def _clean_team_side(s: str) -> str:
    """Strip game/league prefixes and trailing details from one side of a vs split.

    Real polymarket samples we need to handle:
      "Counter-Strike: Natus Vincere"        -> "Natus Vincere"
      "Map Handicap: NAVI (-1.5)"            -> "NAVI"
      "Will Vitality"                        -> "Vitality"
      "GamerLegion (BO3) - BLAST Rivals..."  -> "GamerLegion"
      "Spirit - Map 2 Winner"                -> "Spirit"
    """
    # Strip leading "X:" prefix (game name, market type, etc.). Bound to the
    # first colon — runs at most once so a team name with a colon survives.
    s = re.sub(r"^[^:]+:\s*", "", s, count=1).strip()
    # Strip leading "Will " / "Will the " conversational lead-ins
    s = re.sub(r"^will(\s+the)?\s+", "", s, flags=re.IGNORECASE).strip()
    # Strip trailing parenthetical (and anything after it: tournament tags, etc.)
    s = re.sub(r"\s*\([^)]*\).*$", "", s).strip()
    # Strip trailing " - <suffix>" or " — <suffix>"
    s = re.sub(r"\s*[—\-]\s.*$", "", s).strip()
    return s


def extract_team_candidates(question: str) -> List[str]:
    """Best-effort split of a polymarket question into [home, away] team names.

    Polymarket CS2 questions take many shapes:
      "Will Vitality beat Spirit?"
      "Vitality vs G2"
      "Counter-Strike: Natus Vincere vs GamerLegion - Map 2 Winner"
      "Map Handicap: NAVI (-1.5) vs GamerLegion (+1.5)"
    """
    if not question:
        return []
    q = question.strip().rstrip("?")

    parts = _VS_RE.split(q, maxsplit=1)
    if len(parts) != 2:
        bm = re.search(r"\b(beats?|defeats?|wins against)\b", q, flags=re.IGNORECASE)
        if bm:
            parts = [q[:bm.start()], q[bm.end():]]
        else:
            return [q.strip()] if q.strip() else []

    left = _clean_team_side(parts[0])
    right = _clean_team_side(parts[1])
    return [c for c in [left, right] if c]


def fuzzy_match_market(
    question: str,
    matches: List[Dict[str, Any]],
    threshold: float = PINNACLE_FUZZY_THRESHOLD,
) -> Optional[Tuple[Dict[str, Any], float]]:
    """Pick the best pinnacle match for a polymarket question.

    Scoring: average of (best home-team match ratio, best away-team match ratio)
    across the candidate splits. Both teams must exceed the threshold for a hit.

    Once the match starts, cs2odds emits both a 'matchup' (frozen pre-match) and
    'live' (in-game) entry for the same teams — both score 1.0 here. Prefer the
    live one so we capture in-game line movement instead of stale pre-match.

    Returns (match_dict, confidence) or None.
    """
    if not matches:
        return None
    candidates = extract_team_candidates(question)
    if len(candidates) < 2:
        return None

    cand_a, cand_b = candidates[0], candidates[1]

    hits: List[Tuple[Dict[str, Any], float]] = []
    for m in matches:
        home = m.get("home") or ""
        away = m.get("away") or ""
        if not home or not away:
            continue
        # Try both orderings — polymarket question order may not match pinnacle's
        score_ab_min = min(_ratio(cand_a, home), _ratio(cand_b, away))
        score_ba_min = min(_ratio(cand_a, away), _ratio(cand_b, home))
        score = max(score_ab_min, score_ba_min)
        if score >= threshold:
            hits.append((m, score))

    if not hits:
        return None

    hits.sort(key=lambda h: (1 if (h[0].get("feed") or "") == "live" else 0, h[1]), reverse=True)
    return hits[0]


def attempt_link_for_market(market: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]], Optional[float]]:
    """Run a one-shot fuzzy-match attempt for a polymarket market.

    Returns (link_method, pinnacle_match_dict_or_None, confidence_or_None).
    link_method is one of: 'auto-fuzzy', 'unmatched', 'pinnacle-down'.
    """
    try:
        matches = list_matches()
    except requests.RequestException as e:
        logger.debug(f"pinnacle: list_matches failed during link attempt: {e}")
        return ("pinnacle-down", None, None)

    question = market.get("question") or ""
    hit = fuzzy_match_market(question, matches)
    if hit is None:
        return ("unmatched", None, None)
    match, confidence = hit
    return ("auto-fuzzy", match, confidence)

"""Client + matcher for the cs2odds local HTTP API.

The cs2odds daemon runs an aiohttp server (bound to 0.0.0.0:8765 on the windows
host, reached over Tailscale at 100.110.66.95) that exposes the freshest
in-memory CS2 odds for THREE bookmakers — ps3838, Betway, and Tonybet. cs2odds
maintains a cross-book canonical_match_id so the same fixture on different
books is linked. We hit /linked at each polymarket orderbook snapshot to fetch
all linked books in a single call, then write one pinnacle_snapshots row per
book.

Match linking is best-effort fuzzy team-name matching against the combined
match list. Polymarket questions are free-form ("Will Vitality beat Spirit?",
"Vitality vs G2 — Map 3 winner") so we extract candidate team names from the
question, compare to each book's home/away strings, and pick the highest-
scoring match — preferring ps3838 over the other books when scores tie because
ps3838 has the deepest market coverage and most reliable identifiers.
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


def list_matches(bookmaker: Optional[str] = None) -> List[Dict[str, Any]]:
    """GET /matches[?bookmaker=...] — every match cs2odds knows about.

    Without `bookmaker`, returns matches from all books (each entry tagged with
    bookmaker + canonical_match_id). Pass a book name to filter.
    """
    params = {"bookmaker": bookmaker} if bookmaker else {}
    r = requests.get(f"{PINNACLE_API_URL}/matches", params=params, timeout=PINNACLE_HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_match_odds(bookmaker: str, pin_match_id: Any) -> Optional[Dict[str, Any]]:
    """GET /odds?bookmaker=...&match_id=... — single-book fallback.

    Used when the link has no canonical_match_id (the fixture is only tracked on
    one book). Returns None if cs2odds doesn't know the match.
    """
    try:
        r = requests.get(
            f"{PINNACLE_API_URL}/odds",
            params={"bookmaker": bookmaker, "match_id": str(pin_match_id)},
            timeout=PINNACLE_HTTP_TIMEOUT,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.debug(f"pinnacle: fetch_match_odds({bookmaker}, {pin_match_id}) failed: {e}")
        return None


def fetch_linked(canonical_match_id: str) -> Optional[Dict[str, Any]]:
    """GET /linked?canonical_match_id=... — every book tracking this fixture.

    Returns {canonical_match_id, entries: [{bookmaker, match_id, match, periods,
    last_seen_ms}, ...]}, or None if cs2odds has no in-memory entries linked to
    that canonical id (e.g. all books rolled the fixture off after settlement).
    """
    try:
        r = requests.get(
            f"{PINNACLE_API_URL}/linked",
            params={"canonical_match_id": canonical_match_id},
            timeout=PINNACLE_HTTP_TIMEOUT,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.debug(f"pinnacle: fetch_linked({canonical_match_id}) failed: {e}")
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
    swap_home_away: bool = False,
) -> Optional[Dict[str, Any]]:
    """Pull the pin_map_num period out of an /odds (or /linked entry) response
    and shape it for insert_pinnacle_snapshot.

    Returns a dict ready to **kwarg into the DB helper, minus the orderbook FK,
    market_id, and bookmaker (the caller adds those). Returns None if the
    requested period isn't present (e.g. pre-match BO winner with no per-map
    prices yet).

    `swap_home_away` flips home↔away in the output (and flips the spread line
    sign) so the snapshot stays in the link's canonical home_team perspective
    even when the source book labels the teams in the opposite order. Betway,
    for example, reports Vitality as home for fixtures where ps3838 reports
    GamerLegion as home — the collector resolves that against the link's stored
    home_team and passes swap=True when needed.
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
            # cs2odds emits home -1.5 and away +1.5 as separate rows for the
            # same handicap. Normalize to home's perspective so the two legs
            # land in one bucket and can be no-vig-paired.
            norm_line = float(line) if side == "home" else -float(line)
            slot = spreads_by_line.setdefault(norm_line, {})
            slot[side] = price
        elif market == "total" and isinstance(line, (int, float)):
            slot = totals_by_line.setdefault(float(line), {})
            slot[side] = price

    if swap_home_away:
        ml_home_dec, ml_away_dec = ml_away_dec, ml_home_dec
        # Flip line sign and swap legs: a -1.5 home line becomes a +1.5 line for
        # the new home (which was the old away).
        flipped: Dict[float, Dict[str, float]] = {}
        for ln, slot in spreads_by_line.items():
            new_slot: Dict[str, float] = {}
            if "home" in slot:
                new_slot["away"] = slot["home"]
            if "away" in slot:
                new_slot["home"] = slot["away"]
            flipped[-ln] = new_slot
        spreads_by_line = flipped
        # Totals are direction-keyed (over/under), not team-keyed — no change.

    ml_h_nv, ml_a_nv, ml_d_nv = novig_three_way(ml_home_dec, ml_away_dec, ml_draw_dec)

    spreads = []
    for ln in sorted(spreads_by_line):
        slot = spreads_by_line[ln]
        h_dec, a_dec = slot.get("home"), slot.get("away")
        h_nv, a_nv = novig_two_way(h_dec, a_dec)
        spreads.append({
            "line": ln,
            "home_implied": implied(h_dec), "away_implied": implied(a_dec),
            "home_novig": h_nv, "away_novig": a_nv,
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
    return {
        "pin_map_num": pin_map_num,
        "is_live": match_meta.get("feed") == "live",
        "ml_home_implied": implied(ml_home_dec),
        "ml_away_implied": implied(ml_away_dec),
        "ml_draw_implied": implied(ml_draw_dec),
        "ml_home_novig": ml_h_nv,
        "ml_away_novig": ml_a_nv,
        "ml_draw_novig": ml_d_nv,
        "spreads": spreads if spreads else None,
        "totals": totals if totals else None,
        "pin_observed_at_ms": odds.get("last_seen_ms"),
    }


def should_swap_home_away(
    book_home: Optional[str],
    book_away: Optional[str],
    link_home: Optional[str],
    link_away: Optional[str],
) -> bool:
    """True if the source book's home is actually the link's away (and vice
    versa). Used by the collector to normalize cross-book home/away ordering.

    Compares book_home against link_home vs link_away by fuzzy ratio. Falls
    back to no-swap when any side is missing or the comparison is ambiguous.
    """
    if not book_home or not book_away or not link_home or not link_away:
        return False
    home_to_home = _ratio(book_home, link_home)
    home_to_away = _ratio(book_home, link_away)
    return home_to_away > home_to_home


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------


_TEAM_TOKEN_RE = re.compile(r"[A-Za-z0-9.\-']+(?:\s+[A-Za-z0-9.\-']+){0,3}")
_VS_RE = re.compile(r"\bvs?\.?\b|\b@\b|\bversus\b", re.IGNORECASE)
_MAP_NUM_RE = re.compile(r"\b(?:map|game)\s*(\d)\b", re.IGNORECASE)

# Outright/futures markets that have no per-match Pinnacle counterpart. Examples:
#   "Will MOUZ win IEM Cologne Major 2026?"
#   "Will Team Liquid qualify to IEM Cologne Major 2026?"
# Detecting these up front avoids polluting pinnacle_match_links with thousands of
# permanently-unmatchable rows.
_OUTRIGHT_PATTERNS = (
    re.compile(r"\bqualif(?:y|ies|ied)\s+(?:to|for)\b", re.IGNORECASE),
    re.compile(r"\bwin\s+(?:the\s+)?(?:iem|esl|blast|pgl|major|championship|tournament)", re.IGNORECASE),
    re.compile(r"\b(?:reach|advance to)\s+(?:the\s+)?(?:final|grand final|playoffs|semis?)", re.IGNORECASE),
)


_TEAM_NOISE_TOKENS = {"team", "esports", "esport", "gaming", "the"}


def is_outright_question(question: Optional[str]) -> bool:
    """True if the question is a tournament outright/qualifier (no head-to-head Pinnacle counterpart)."""
    if not question:
        return False
    # Outrights never have a "X vs Y" structure — that's the cheap signal.
    if _VS_RE.search(question):
        return False
    return any(p.search(question) for p in _OUTRIGHT_PATTERNS)


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
    """Pick the best pinnacle match for a polymarket question across all books.

    Scoring: average of (best home-team match ratio, best away-team match ratio)
    across the candidate splits. Both teams must exceed the threshold for a hit.

    Tie-breakers, in order of preference:
      1. live feed > matchup feed (in-game line movement is what we want)
      2. ps3838 > betway > tonybet (deepest market coverage and most reliable
         identifiers — the anchor row's bookmaker)
      3. raw fuzzy score
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
        # Try both orderings — polymarket question order may not match the book's
        score_ab_min = min(_ratio(cand_a, home), _ratio(cand_b, away))
        score_ba_min = min(_ratio(cand_a, away), _ratio(cand_b, home))
        score = max(score_ab_min, score_ba_min)
        if score >= threshold:
            hits.append((m, score))

    if not hits:
        return None

    _book_pref = {"ps3838": 2, "betway": 1, "tonybet": 0}
    hits.sort(
        key=lambda h: (
            1 if (h[0].get("feed") or "") == "live" else 0,
            _book_pref.get(h[0].get("bookmaker") or "", -1),
            h[1],
        ),
        reverse=True,
    )
    return hits[0]


def attempt_link_for_market(
    market: Dict[str, Any],
    matches: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Optional[float]]:
    """Run a one-shot fuzzy-match attempt for a polymarket market.

    `matches` lets the caller pass a cached match list — useful in batch backfills
    where one /matches call serves thousands of attempts. When omitted, fetches fresh.

    Returns (link_method, pinnacle_match_dict_or_None, confidence_or_None).
    link_method is one of: 'auto-fuzzy', 'unmatched', 'outright', 'pinnacle-down'.
    """
    question = market.get("question") or ""
    if is_outright_question(question):
        return ("outright", None, None)

    if matches is None:
        try:
            matches = list_matches()
        except requests.RequestException as e:
            logger.debug(f"pinnacle: list_matches failed during link attempt: {e}")
            return ("pinnacle-down", None, None)

    hit = fuzzy_match_market(question, matches)
    if hit is None:
        return ("unmatched", None, None)
    match, confidence = hit
    return ("auto-fuzzy", match, confidence)

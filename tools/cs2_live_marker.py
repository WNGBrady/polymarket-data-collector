"""Hotkey CLI for marking live CS2 game events during a match.

Run alongside the realtime collector. Each command typed into the terminal
(followed by Enter) records a row in cs2_live_events with the wall-clock UTC
timestamp captured at the moment Enter is pressed. Joined to
orderbook_snapshots / pinnacle_snapshots in post-match analysis.

Usage:
    python tools/cs2_live_marker.py --market-id <id1>,<id2>,<id3>
    python tools/cs2_live_marker.py --parent-event-id <event_id>
    python tools/cs2_live_marker.py --market-id <id> --dry-run

Commands (each followed by Enter):
    m            map start. Defaults to the next map. Resets score to 0-0.
    m <N>        map start, force map number N.
    t            round end won by T side (auto-increments t_score).
    c            round end won by CT side (auto-increments ct_score).
    n <text>     freeform note (e.g. "vp clutch 1v3").
    s            print current state (active map, score, last 10 events).
    u            undo last event recorded in this session.
    q            quit.

Multi-map sessions: the active market is the one matching the current map
number from the markets table; switching maps via `m` updates which market
subsequent round_end rows are attached to.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from src.database import (  # noqa: E402
    DATABASE_PATH,
    delete_live_event,
    get_connection,
    get_pinnacle_link,
    get_recent_live_events,
    init_database,
    insert_live_event,
    migrate_database,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_ms(ms: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000)) + f".{ms % 1000:03d}"


def load_markets(
    market_ids: List[str],
    parent_event_id: Optional[str],
) -> List[Dict[str, Any]]:
    """Resolve the set of markets to track. Pulls map_num from pinnacle link if
    present, else parses it from the question text."""
    cur = get_connection().cursor()
    rows: List[Dict[str, Any]] = []
    if market_ids:
        placeholders = ",".join("?" for _ in market_ids)
        cur.execute(
            f"SELECT market_id, question, event_id FROM markets WHERE market_id IN ({placeholders})",
            tuple(market_ids),
        )
        rows = [dict(r) for r in cur.fetchall()]
    if parent_event_id:
        cur.execute(
            "SELECT market_id, question, event_id FROM markets WHERE event_id = ?",
            (parent_event_id,),
        )
        for r in cur.fetchall():
            d = dict(r)
            if d["market_id"] not in {x["market_id"] for x in rows}:
                rows.append(d)
    if not rows:
        return []

    # Filter to per-map sub-markets only (the BO3 winner has no useful map_num
    # for round-by-round tracking -- but we keep it as map_num=0 if it's the only
    # market the user passed).
    enriched: List[Dict[str, Any]] = []
    for r in rows:
        link = get_pinnacle_link(r["market_id"]) or {}
        map_num = link.get("pin_map_num")
        if map_num is None:
            from src import pinnacle  # late import to avoid cycle
            map_num = pinnacle.infer_map_num(r.get("question") or "")
        r["map_num"] = map_num or 0
        r["pin_match_id"] = link.get("pin_match_id")
        enriched.append(r)

    enriched.sort(key=lambda m: (m["map_num"] == 0, m["map_num"]))
    return enriched


def market_for_map(markets: List[Dict[str, Any]], map_num: int) -> Optional[Dict[str, Any]]:
    """Return the market whose map_num matches; if no per-map match, fall back to
    the parent (map_num=0). Returns None if neither is available."""
    for m in markets:
        if m.get("map_num") == map_num:
            return m
    for m in markets:
        if m.get("map_num") == 0:
            return m
    return markets[0] if markets else None


class Session:
    def __init__(self, markets: List[Dict[str, Any]], parent_event_id: Optional[str], dry_run: bool):
        self.markets = markets
        self.parent_event_id = parent_event_id
        self.dry_run = dry_run
        self.current_map = 1
        self.ct_score = 0
        self.t_score = 0
        # IDs inserted this session, for undo
        self.session_ids: List[int] = []

    def reset_score(self) -> None:
        self.ct_score = 0
        self.t_score = 0

    def active_market(self) -> Optional[Dict[str, Any]]:
        return market_for_map(self.markets, self.current_map)

    def record(
        self,
        event_type: str,
        winning_side: Optional[str] = None,
        notes: Optional[str] = None,
        ts_ms: Optional[int] = None,
    ) -> Optional[int]:
        ts = ts_ms if ts_ms is not None else _now_ms()
        market = self.active_market()
        if market is None:
            print("  ! no active market -- pass --market-id or --parent-event-id with valid markets")
            return None
        if self.dry_run:
            print(
                f"  [DRY-RUN] would insert: type={event_type} ts={_fmt_ms(ts)} "
                f"market={market['market_id']} map={self.current_map} "
                f"score=ct{self.ct_score}-t{self.t_score} side={winning_side} "
                f"notes={notes!r}"
            )
            return -1
        new_id = insert_live_event(
            market_id=market["market_id"],
            event_type=event_type,
            wall_clock_ms_utc=ts,
            parent_event_id=market.get("event_id") or self.parent_event_id,
            pin_match_id=str(market.get("pin_match_id")) if market.get("pin_match_id") is not None else None,
            map_num=self.current_map,
            ct_score=self.ct_score if event_type == "round_end" else None,
            t_score=self.t_score if event_type == "round_end" else None,
            winning_side=winning_side,
            notes=notes,
        )
        self.session_ids.append(new_id)
        return new_id

    def cmd_map_start(self, map_num_arg: Optional[int]) -> None:
        if map_num_arg is not None:
            self.current_map = map_num_arg
        elif self._current_map_has_round_end():
            # Advance only after rounds have been logged for the current map;
            # the first `m` press anchors map 1 without bumping.
            self.current_map += 1
        self.reset_score()
        market = self.active_market()
        if market is None:
            print(f"  ! no market matches map {self.current_map} -- staying anyway")
        else:
            print(f"  --> MAP {self.current_map} START on market {market['market_id']}  '{(market.get('question') or '')[:60]}'")
        self.record("map_start")

    def _current_map_has_round_end(self) -> bool:
        # Cheap check: did we insert anything this session whose event_type is round_end
        # for the current map? Easiest is to query the DB directly.
        if self.dry_run:
            return False
        cur = get_connection().cursor()
        market = self.active_market()
        if not market:
            return False
        cur.execute(
            "SELECT 1 FROM cs2_live_events WHERE market_id = ? AND event_type = 'round_end' AND map_num = ? LIMIT 1",
            (market["market_id"], self.current_map),
        )
        return cur.fetchone() is not None

    def cmd_round_end(self, side: str) -> None:
        side = side.upper()
        if side not in ("T", "CT"):
            print(f"  ! unknown side {side!r}")
            return
        if side == "T":
            self.t_score += 1
        else:
            self.ct_score += 1
        new_id = self.record("round_end", winning_side=side)
        market = self.active_market()
        mid = market["market_id"] if market else "?"
        print(
            f"  [ok] round_end map={self.current_map} side={side}  "
            f"ct{self.ct_score}-t{self.t_score}  id={new_id}  market={mid}"
        )

    def cmd_note(self, text: str) -> None:
        if not text:
            print("  ! note text required")
            return
        new_id = self.record("note", notes=text)
        print(f"  [ok] note id={new_id} text={text!r}")

    def cmd_status(self) -> None:
        market = self.active_market()
        print(
            f"  state: map={self.current_map} ct={self.ct_score} t={self.t_score} "
            f"market={market['market_id'] if market else 'none'}"
        )
        ids = [m["market_id"] for m in self.markets]
        recent = get_recent_live_events(ids, limit=10) if not self.dry_run else []
        for r in reversed(recent):
            print(
                f"    {r['id']:>5}  {_fmt_ms(r['wall_clock_ms_utc'])}  "
                f"map={r.get('map_num')}  {r['event_type']}  side={r.get('winning_side')}  "
                f"score=ct{r.get('ct_score')}-t{r.get('t_score')}  notes={r.get('notes')!r}"
            )

    def cmd_undo(self) -> None:
        if not self.session_ids:
            print("  ! nothing to undo in this session")
            return
        if self.dry_run:
            popped = self.session_ids.pop()
            print(f"  [DRY-RUN] would undo id={popped}")
            return
        last_id = self.session_ids.pop()
        # Look up the row before deleting so we can roll back score state.
        cur = get_connection().cursor()
        cur.execute("SELECT * FROM cs2_live_events WHERE id = ?", (last_id,))
        row = cur.fetchone()
        if row is None:
            print(f"  ! id={last_id} no longer in DB")
            return
        if row["event_type"] == "round_end":
            if row["winning_side"] == "T" and self.t_score > 0:
                self.t_score -= 1
            elif row["winning_side"] == "CT" and self.ct_score > 0:
                self.ct_score -= 1
        deleted = delete_live_event(last_id)
        print(f"  [ok] undid id={last_id} ({'ok' if deleted else 'already gone'}); score now ct{self.ct_score}-t{self.t_score}")


def parse_command(line: str) -> Tuple[str, str]:
    """Returns (cmd, arg_text)."""
    s = line.strip()
    if not s:
        return ("", "")
    head, _, rest = s.partition(" ")
    return (head.lower(), rest.strip())


def main():
    p = argparse.ArgumentParser(description="Mark live CS2 events into cs2_live_events")
    p.add_argument("--market-id", help="Comma-separated market_ids to track")
    p.add_argument("--parent-event-id", help="Polymarket event_id; auto-loads all sub-markets")
    p.add_argument("--db", help="Override DATABASE_PATH (default: project DB)")
    p.add_argument("--dry-run", action="store_true", help="Don't write to DB; just echo")
    args = p.parse_args()

    if args.db:
        # late override via env var would be cleaner; simplest is to just fail loud
        print(f"! --db override not supported; project DB is {DATABASE_PATH}", file=sys.stderr)
        sys.exit(2)

    market_ids = [s.strip() for s in (args.market_id or "").split(",") if s.strip()]
    if not market_ids and not args.parent_event_id:
        p.error("Pass --market-id or --parent-event-id")

    migrate_database()
    init_database()

    markets = load_markets(market_ids, args.parent_event_id)
    if not markets:
        print("! no markets resolved -- run discovery or pass valid --market-id", file=sys.stderr)
        sys.exit(2)

    print("Tracking markets:")
    for m in markets:
        print(
            f"  map={m['map_num']}  {m['market_id']}  '{(m.get('question') or '')[:60]}' "
            f"pin={m.get('pin_match_id')}"
        )
    if args.dry_run:
        print("(dry run -- nothing will be written)")

    session = Session(markets, args.parent_event_id, args.dry_run)
    print("\nReady. Commands: m [N], t, c, n <text>, s, u, q\n")

    try:
        while True:
            try:
                line = input("> ")
            except EOFError:
                break
            cmd, arg = parse_command(line)
            if not cmd:
                continue
            if cmd in ("q", "quit", "exit"):
                break
            elif cmd == "m":
                map_num = None
                if arg:
                    try:
                        map_num = int(arg)
                    except ValueError:
                        print(f"  ! map number must be int, got {arg!r}")
                        continue
                session.cmd_map_start(map_num)
            elif cmd in ("t", "rt"):
                session.cmd_round_end("T")
            elif cmd in ("c", "rc"):
                session.cmd_round_end("CT")
            elif cmd == "n":
                session.cmd_note(arg)
            elif cmd == "s":
                session.cmd_status()
            elif cmd == "u":
                session.cmd_undo()
            else:
                print(f"  ! unknown command {cmd!r}. valid: m [N], t, c, n <text>, s, u, q")
    except KeyboardInterrupt:
        pass

    print("\nbye.")


if __name__ == "__main__":
    main()

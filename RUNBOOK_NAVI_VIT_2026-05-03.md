# Runbook — NaVi vs Vitality, BLAST Rivals Playoffs (2026-05-03)

**Match start**: 2026-05-03 17:30 UTC (13:30 EDT)
**Format**: BO5
**Polymarket event_id**: `442528`
**Polymarket slug**: `cs2-navi-vit-2026-05-03`

**Per-map market_ids on Polymarket**:

| Map | market_id | question |
|---|---|---|
| BO5 parent | `2143221` | NaVi vs Vitality (BO5) - BLAST Rivals Playoffs |
| Map 1 | `2143222` | NaVi vs Vitality - Map 1 Winner |
| Map 2 | `2143223` | NaVi vs Vitality - Map 2 Winner |
| Map 3 | `2143224` | NaVi vs Vitality - Map 3 Winner |
| Map 4 | `2143225` | NaVi vs Vitality - Map 4 Winner |
| Map 5 | not yet listed | (Polymarket will add if it goes 2-2) |

The collector + database run on the droplet (`root@159.203.33.91`,
`/opt/polymarket-collector/`). The cs2odds daemon runs on this Windows box
(Tailscale `100.110.66.95:8765`); the droplet collector hits it over Tailscale.
Both must be up.

> Run all `ssh` commands from a **PowerShell** prompt (the bash shell on this
> Windows host doesn't have `ssh` in `PATH`).

---

## T-1h — deploy code, prewarm Pinnacle

**1. Commit + push the new tooling.** From this machine:

```powershell
cd "C:\Users\hocke\Documents\Software Projects\PolyMarket_COD"
git add src/database.py src/config.py src/realtime_collector.py api/migrate.py `
        tools/prewarm_pinnacle.py tools/cs2_live_marker.py `
        analysis_cs2_live_validation.py RUNBOOK_NAVI_VIT_2026-05-03.md
git commit -m "Add live-event marker + per-match polling override for CS2"
git push origin fix/api-db-concurrency
```

(Push is to the existing branch `fix/api-db-concurrency`, which is what the
droplet tracks.)

**2. Pull on the droplet, run migrate to create `cs2_live_events`:**

```powershell
ssh -o BatchMode=yes root@159.203.33.91 "sudo -u polymarket git -C /opt/polymarket-collector pull"
ssh -o BatchMode=yes root@159.203.33.91 "sudo -u polymarket /opt/polymarket-collector/api/venv/bin/python -m api.migrate"
```

Confirm the table exists:

```powershell
"SELECT name FROM sqlite_master WHERE name='cs2_live_events';" | `
  ssh -o BatchMode=yes root@159.203.33.91 "sqlite3 /opt/polymarket-collector/data/polymarket_esports.db"
```

Should print `cs2_live_events`.

**3. Confirm cs2odds is reachable from the droplet** (Tailscale link to your
Windows box):

```powershell
ssh -o BatchMode=yes root@159.203.33.91 "curl -sS --max-time 3 http://100.110.66.95:8765/health"
```

Should print something like `{"status":"ok",...}`. If it fails, check that
cs2odds is running on this Windows machine and Tailscale is up.

**4. Prewarm `pinnacle_match_links`** for all per-map sub-markets (the collector
will auto-link on its own when it polls, but doing it now confirms the home/away
mapping looks right *before* the match starts):

```powershell
ssh -o BatchMode=yes root@159.203.33.91 "cd /opt/polymarket-collector && sudo -u polymarket -E PINNACLE_API_URL=http://100.110.66.95:8765 ./api/venv/bin/python tools/prewarm_pinnacle.py --slug cs2-navi-vit-2026-05-03 --show-candidates"
```

Look for one `[ok] LINKED` line per per-map market with `home=NaVi` / `away=Vitality`
(or vice versa — note the order, the home/away convention on cs2odds may differ
from your expectation but it'll be consistent across snapshots).

If any market shows `UNMATCHED`, the script prints fuzzy candidates. Pin manually
with:

```powershell
ssh -o BatchMode=yes root@159.203.33.91 "cd /opt/polymarket-collector && sudo -u polymarket -E PINNACLE_API_URL=http://100.110.66.95:8765 ./api/venv/bin/python tools/prewarm_pinnacle.py --force-pin 2143222=<PIN_MATCH_ID>:ps3838"
```

---

## T-15min — enable 2s polling override and restart collector

```powershell
ssh -o BatchMode=yes root@159.203.33.91 @'
mkdir -p /etc/systemd/system/polymarket-collector.service.d
cat > /etc/systemd/system/polymarket-collector.service.d/fast-override.conf <<EOF
[Service]
Environment="FAST_OVERRIDE_MARKET_IDS=2143222,2143223,2143224,2143225"
Environment="FAST_OVERRIDE_INTERVAL=2"
EOF
systemctl daemon-reload
systemctl restart polymarket-collector
sleep 3
journalctl -u polymarket-collector -n 30 --no-pager
'@
```

In the journal output, confirm a line like:
`Starting fast orderbook polling (default=10s, override=2.0s for 4 markets, ...)`.

**Verify 2s cadence is hitting the DB** (run this ~30s after the restart):

```powershell
"SELECT market_id, COUNT(*) AS n, MAX(timestamp)-MIN(timestamp) AS span_ms FROM orderbook_snapshots WHERE market_id IN ('2143222','2143223','2143224','2143225') AND timestamp > (strftime('%s','now')*1000 - 60000) GROUP BY market_id;" | `
  ssh -o BatchMode=yes root@159.203.33.91 "sqlite3 -cmd '.headers on' -cmd '.mode column' /opt/polymarket-collector/data/polymarket_esports.db"
```

Expect ~25–30 rows per market in 60s (one every 2s). If you see ~6 rows, the
override env var didn't apply — re-check the drop-in.

---

## During the match — run the live marker

In a **second PowerShell window** (keep the first open for ad-hoc DB checks),
SSH into the droplet and start the marker. The marker is interactive, so
allocate a TTY with `-t`:

```powershell
ssh -t -o BatchMode=yes root@159.203.33.91 "cd /opt/polymarket-collector && sudo -u polymarket ./api/venv/bin/python tools/cs2_live_marker.py --market-id 2143222,2143223,2143224,2143225"
```

It will print:

```
Tracking markets:
  map=1  2143222  'Counter-Strike: Natus Vincere vs Vitality - Map 1 Winner' pin=...
  map=2  2143223  ...
  map=3  2143224  ...
  map=4  2143225  ...

Ready. Commands: m [N], t, c, n <text>, s, u, q
```

**Hotkeys** (each followed by Enter — timestamp is captured the moment you press
Enter):

| Key | Meaning |
|---|---|
| `m` | Map start. First press anchors map 1; subsequent presses advance to next map. Resets score to 0-0. |
| `m 3` | Force map start to a specific map (useful if BO5 skips map 4 etc., or if you missed pressing `m`). |
| `t` | Round end won by **T side**. Auto-increments t_score. |
| `c` | Round end won by **CT side**. Auto-increments ct_score. |
| `n <text>` | Free-form note (e.g. `n vit clutch 1v3`, `n big swing right now`, `n bomb defuse`). |
| `s` | Show current state + last 10 events from DB. |
| `u` | Undo last event from this session (also rolls back the score). |
| `q` | Quit. |

**Workflow each round**: when the round ends on the stream, press the matching
side key (`t` or `c`) and Enter. That's it — one keystroke per round end.

**At map start**: press `m` and Enter once you see the first round actually go
live (CT spawn, weapons drawn). Don't press it during the warmup or pre-game.

**Note any anomalies**: if you *see* the line move 1-2 seconds before something
on the stream, hit `n line moved before clutch` immediately. The note carries a
timestamp you can compare to demo events later.

---

## After the match — restore polling, run analysis

**1. Stop the marker** with `q` (or Ctrl-C).

**2. Remove the override drop-in and restart collector:**

```powershell
ssh -o BatchMode=yes root@159.203.33.91 @'
rm -f /etc/systemd/system/polymarket-collector.service.d/fast-override.conf
systemctl daemon-reload
systemctl restart polymarket-collector
'@
```

**3. Wait 10–60min for HLTV to publish the GOTV demo.** Then on this Windows
machine (which has `demoparser2` and the demo download stack):

```powershell
cd "C:\Users\hocke\Documents\Software Projects\PolyMarket_COD"
# hltv_demos.py downloads + extracts; check its --help for the exact flow.
# When done, you'll have data\demos\<match_id>\<map1>.dem etc.
```

**4. Pull the day's slice of `cs2_live_events`, `orderbook_snapshots`, and
`pinnacle_snapshots` from the droplet to local** (the analysis script reads from
the local DB):

```powershell
ssh -o BatchMode=yes root@159.203.33.91 "sqlite3 /opt/polymarket-collector/data/polymarket_esports.db .dump cs2_live_events" | `
  Out-File -Encoding utf8 .\data\cs2_live_events_dump.sql
# repeat for orderbook_snapshots / pinnacle_snapshots filtered to today's market_ids
# (a small Python script using sqlite3 .iterdump or a SELECT/INSERT pair is cleaner;
# this is the moment to write it once and reuse).
```

(Cleanest: just SCP the whole DB to a local copy and analyze against that — a
2.3 GB file but this only happens post-match.)

```powershell
scp root@159.203.33.91:/opt/polymarket-collector/data/polymarket_esports.db `
    .\data\polymarket_esports_postmatch_2026-05-03.db
```

Then point the analysis at it (set `DATABASE_PATH` env var or temporarily move
the file into `data/polymarket_esports.db`).

**5. Run the validation script per map:**

```powershell
python analysis_cs2_live_validation.py `
  --market-id 2143222 `
  --demo-path data\demos\<match>\map1.dem `
  --stream-delay-ms 18000

python analysis_cs2_live_validation.py `
  --market-id 2143223 `
  --demo-path data\demos\<match>\map2.dem `
  --stream-delay-ms 18000
# ... etc per played map
```

`--stream-delay-ms 18000` is a starting estimate (~18s typical Twitch latency
on low-latency mode). The script also prints the *empirical* stream delay
derived from your `t`/`c` taps vs demo round_ends — if those disagree
substantially, re-run with the empirical value as `--stream-delay-ms` for a
sharper anchor.

**Output** — for each map: `data/<market_id>_map<N>_round_alignment.csv` and a
console verdict line:

- `lag - stream_delay < 5s` → **odds reflect game state on the stream**
- `lag - stream_delay between 5–30s` → **reactive but not delayed by a full round**
- `lag - stream_delay > 30s` → **suspicious for a baked-in ~1-round delay**

The CSV gives you per-round granularity: round_num, side won, demo_ts, your tap,
biggest Polymarket move + magnitude, biggest Pinnacle move + magnitude, all the
lag deltas. Use it to slice swings by round type (pistol, gun-round, eco, force)
and side, and to spot-check any round where Polymarket *led* Pinnacle (suggests
Polymarket had a sharper view than the sportsbook on that round).

---

## Quick reference — IDs in one place

```
event_id:        442528
slug:            cs2-navi-vit-2026-05-03
market_ids:      2143222 (Map 1), 2143223 (Map 2), 2143224 (Map 3), 2143225 (Map 4)
parent BO5:      2143221
override env:    FAST_OVERRIDE_MARKET_IDS=2143222,2143223,2143224,2143225
                 FAST_OVERRIDE_INTERVAL=2
collector unit:  systemctl restart polymarket-collector  (on droplet)
drop-in path:    /etc/systemd/system/polymarket-collector.service.d/fast-override.conf
```

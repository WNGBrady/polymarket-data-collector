"""Configuration for Polymarket API endpoints and rate limits."""

import os

# API Base URLs
GAMMA_API_URL = "https://gamma-api.polymarket.com"
CLOB_API_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"
OI_API_URL = "https://data-api.polymarket.com/oi"

# Rate limits (requests per 10 seconds)
RATE_LIMITS = {
    "gamma_markets": 300,
    "gamma_events": 500,
    "gamma_search": 350,
    "clob_prices": 1500,
    "clob_book": 1500,
    "data_trades": 200,
    "data_oi": 200,
    "gamma_tags": 200,
}

# Rate limit window in seconds
RATE_LIMIT_WINDOW = 10

# Per-game configuration
GAME_CONFIGS = {
    "cod": {
        "display_name": "Call of Duty",
        "league_abbreviations": ["cdl", "cod"],
        "search_terms": [
            # General terms
            "call of duty",
            "call of duty league",
            "cdl",
            # CDL event types
            "cdl major",
            "cdl stage",
            "cdl qualifier",
            "cdl championship",
            # All 12 CDL teams (2024-2025 season)
            "optic texas",
            "faze atlanta",
            "atlanta faze",
            "boston breach",
            "los angeles thieves",
            "la thieves",
            "miami heretics",
            "carolina royal ravens",
            "toronto ultra",
            "new york subliners",
            "las vegas legion",
            "seattle surge",
            "los angeles guerrillas",
            "la guerrillas",
            "minnesota rokkr",
        ],
        "validation_terms": [
            "call of duty",
            "cdl regular season",
            "cdl stage",
            "cdl major",
            "cdl qualifier",
            "cdl championship",
            # All CDL teams (current and recent)
            "optic texas",
            "atlanta faze",
            "faze atlanta",
            "boston breach",
            "los angeles thieves",
            "la thieves",
            "miami heretics",
            "carolina royal ravens",
            "toronto ultra",
            "toronto koi",
            "new york subliners",
            "cloud9 new york",
            "las vegas legion",
            "seattle surge",
            "vancouver surge",
            "los angeles guerrillas",
            "la guerrillas",
            "minnesota rokkr",
            "g2 minnesota",
            "paris gentle mates",
            "riyadh falcons",
        ],
        "tag_labels": [
            "esports",
            "gaming",
            "call of duty",
            "cdl",
            "video games",
        ],
    },
    "cs2": {
        "display_name": "Counter-Strike 2",
        "league_abbreviations": ["cs2", "csgo"],
        "search_terms": [
            # General terms
            "counter-strike",
            "counter strike",
            "cs2",
            "csgo",
            # Major tournaments / leagues
            "esl pro league",
            "blast premier",
            "iem",
            "iem katowice",
            "iem cologne",
            "pgl major",
            "pgl cs2",
            "blast world final",
            "blast spring",
            "blast fall",
            # Top CS2 teams
            "natus vincere",
            "navi cs",
            "g2 esports",
            "faze clan",
            "team vitality",
            "team spirit",
            "mouz",
            "mousesports",
            "heroic",
            "team liquid",
            "fnatic",
            "astralis",
            "cloud9 cs",
            "complexity",
            "virtus.pro",
            "eternal fire",
            "pain gaming",
            "imperial esports",
            "9z team",
            "monte",
        ],
        # Game-identifying terms: any one of these is sufficient to confirm CS2
        "game_terms": [
            "counter-strike",
            "counter strike",
            "cs2",
            "csgo",
            "cs:go",
        ],
        # Validation terms: game_terms + tournament/league terms that uniquely identify CS2
        "validation_terms": [
            "counter-strike",
            "counter strike",
            "cs2",
            "csgo",
            "cs:go",
            "esl pro league",
            "blast premier",
            "blast spring",
            "blast fall",
            "blast world final",
            "iem katowice",
            "iem cologne",
            "pgl major",
            "pgl cs2",
        ],
        # Team terms: only match if a game_term is also present in the text
        "team_terms": [
            "natus vincere",
            "navi",
            "g2 esports",
            "faze clan",
            "team vitality",
            "team spirit",
            "mouz",
            "mousesports",
            "heroic",
            "team liquid",
            "fnatic",
            "astralis",
            "cloud9 cs",
            "complexity",
            "virtus.pro",
            "eternal fire",
        ],
        "tag_labels": [
            "esports",
            "gaming",
            "counter-strike",
            "cs2",
            "csgo",
            "video games",
        ],
    },
}

# Valid game keys
VALID_GAMES = list(GAME_CONFIGS.keys())
ALL_GAMES = VALID_GAMES


def get_game_config(game: str) -> dict:
    """Get configuration for a specific game."""
    if game not in GAME_CONFIGS:
        raise ValueError(f"Unknown game: {game}. Valid games: {VALID_GAMES}")
    return GAME_CONFIGS[game]


# Backward-compatible aliases (used by generate_report.py)
COD_SEARCH_TERMS = GAME_CONFIGS["cod"]["search_terms"]
COD_VALIDATION_TERMS = GAME_CONFIGS["cod"]["validation_terms"]
ESPORTS_TAG_LABELS = GAME_CONFIGS["cod"]["tag_labels"]

# Database path (resolve relative to project root, not cwd)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABASE_PATH = os.path.join(_PROJECT_ROOT, "data", "polymarket_esports.db")

# Retry settings
MAX_RETRIES = 5
INITIAL_BACKOFF = 1  # seconds
MAX_BACKOFF = 60  # seconds

# Orderbook settings
ORDERBOOK_POLL_INTERVAL = 60  # seconds between orderbook snapshots (default tier)
ORDERBOOK_DEPTH = 5  # Number of price levels to store

# Fast-tier orderbook polling for tier-1 CS2 events (IEM, ESL Pro League, BLAST, PGL).
# Markets matching TIER1_CS2_KEYWORDS in their question are polled on this faster cadence
# and skipped by the default-tier loop to avoid double work.
FAST_ORDERBOOK_POLL_INTERVAL = 10  # seconds
FAST_ORDERBOOK_CONCURRENCY = 8  # parallel HTTP fetches per fast-tier round
TIER1_CS2_KEYWORDS = ["iem", "esl pro league", "blast", "pgl"]

# Per-match override: polls the listed market_ids on a tighter cadence (e.g. 2s)
# while leaving every other fast-tier market on the default 10s. Intended as a
# one-off env var set during a live match to capture lead/lag detail and
# reverted after — not a permanent setting. Empty set = no override.
FAST_OVERRIDE_MARKET_IDS = {
    s.strip() for s in os.environ.get("FAST_OVERRIDE_MARKET_IDS", "").split(",") if s.strip()
}
FAST_OVERRIDE_INTERVAL = float(os.environ.get("FAST_OVERRIDE_INTERVAL", "2"))

# Periodic trades polling in continuous mode. The historical backfill at startup
# only pulls trades once per market; without this loop, trades for existing
# markets stop flowing until the next service restart (the system was previously
# relying on OOM-restarts as a hidden trades cron).
TRADES_POLL_ENABLED = os.environ.get("TRADES_POLL_ENABLED", "1").lower() in {"1", "true", "yes"}
TRADES_FAST_POLL_INTERVAL = int(os.environ.get("TRADES_FAST_POLL_INTERVAL", "60"))
TRADES_SLOW_POLL_INTERVAL = int(os.environ.get("TRADES_SLOW_POLL_INTERVAL", "600"))

# Continuous mode settings
DISCOVERY_INTERVAL = 1800  # 30 minutes between market discovery runs

# Sports WebSocket settings
MATCH_SNAPSHOT_DELAY = 2  # seconds to wait after match end before snapshotting

# Pinnacle (cs2odds) integration. The cs2odds daemon runs as a separate systemd
# service and exposes its in-memory state on this localhost port. If the API is
# unreachable, the realtime collector logs and skips — no orderbook collection
# is blocked on it.
PINNACLE_API_URL = os.environ.get("PINNACLE_API_URL", "http://127.0.0.1:8765")
PINNACLE_HTTP_TIMEOUT = float(os.environ.get("PINNACLE_HTTP_TIMEOUT", "0.5"))
PINNACLE_FUZZY_THRESHOLD = float(os.environ.get("PINNACLE_FUZZY_THRESHOLD", "0.85"))

# Kalshi tracking. Off by default; enabled by setting KALSHI_TICKERS to a
# comma-separated list of market tickers (e.g. KXCS2GAME-26MAY031330VITNAVI-VIT,...).
# Both polling tasks no-op when the set is empty so the rest of the collector is
# unaffected. Used per-match: set the env var on the systemd drop-in, restart,
# revert after the match.
KALSHI_API_BASE = os.environ.get("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_TICKERS = {
    s.strip() for s in os.environ.get("KALSHI_TICKERS", "").split(",") if s.strip()
}
KALSHI_POLL_INTERVAL = float(os.environ.get("KALSHI_POLL_INTERVAL", "2.0"))
KALSHI_TRADES_POLL_INTERVAL = float(os.environ.get("KALSHI_TRADES_POLL_INTERVAL", "60.0"))
KALSHI_HTTP_TIMEOUT = float(os.environ.get("KALSHI_HTTP_TIMEOUT", "5.0"))
KALSHI_CONCURRENCY = int(os.environ.get("KALSHI_CONCURRENCY", "4"))

# Trading + sportsbook flags. main.py imports these unconditionally; the
# corresponding code paths are gated behind these flags and only run when
# explicitly enabled via env vars. Defaults disable both so the collector can
# boot without the optional src/trading and src/sportsbook modules present.
TRADING_ENABLED = os.environ.get("TRADING_ENABLED", "").lower() in {"1", "true", "yes"}
SPORTSBOOK_ENABLED = os.environ.get("SPORTSBOOK_ENABLED", "").lower() in {"1", "true", "yes"}
SIGNAL_SCAN_INTERVAL = int(os.environ.get("SIGNAL_SCAN_INTERVAL", "30"))
ORDER_CHECK_INTERVAL = int(os.environ.get("ORDER_CHECK_INTERVAL", "60"))

# ---------------------------------------------------------------------------
# Bot classification heuristics (Phase 3)
# ---------------------------------------------------------------------------
# Thresholds drive both the per-feature flag and the final bot_label assignment
# in src/bot_classifier.py. Tune from here without touching code.
BOT_HEURISTICS = {
    "min_trades_for_classification": 10,
    "inter_trade_cv_max": 0.30,        # CV below this → cadence is bot-like
    "round_size_share_min": 0.60,      # >60% sizes are round → bot-like
    "round_sizes": (5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000),
    "round_size_tolerance": 0.5,       # accept e.g. 99.50–100.50 as "100"
    "night_share_min": 0.25,           # >25% trades at 02:00–06:00 UTC → bot-like
    "night_hours_utc": (2, 3, 4, 5),
    "two_sided_ratio_min": 0.40,       # min(buy,sell)/max ≥ this → market maker
    "two_sided_min_trades": 20,
    "cross_market_burst_window_s": 5,
    "cross_market_burst_distinct": 3,  # ≥3 distinct markets traded in 5s → bot-like
    "markets_per_active_day_min": 20,  # touched ≥20 markets per active day → bot-like
    "score_bot_threshold": 0.50,       # bot_score ≥ → label=bot
    "score_likely_bot_threshold": 0.30,
    # Per-feature contribution to bot_score (sum is normalised to [0, 1])
    "weights": {
        "inter_trade_cv": 0.25,
        "round_size_share": 0.10,
        "night_share": 0.10,
        "cross_market_burst": 0.20,
        "markets_per_day": 0.20,
        "two_sided_ratio": 0.15,
    },
}

# Phase 4: CS2 signal correlation
CS2_SIGNAL_HEURISTICS = {
    "pinnacle_lookback_window_s": 60,   # match a trade to the most recent Pinnacle snapshot within X s
    "pinnacle_min_move_implied": 0.01,  # only count Pinnacle moves of |Δprob| ≥ 1%
    "score_event_window_s": 10,         # trades landing within 10s of a final_prices event count as score reaction
    "min_cs2_trades_for_signals": 5,
}

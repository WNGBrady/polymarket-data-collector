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
ORDERBOOK_POLL_INTERVAL = 60  # seconds between orderbook snapshots
ORDERBOOK_DEPTH = 5  # Number of price levels to store

# Continuous mode settings
DISCOVERY_INTERVAL = 1800  # 30 minutes between market discovery runs

# Sports WebSocket settings
MATCH_SNAPSHOT_DELAY = 2  # seconds to wait after match end before snapshotting

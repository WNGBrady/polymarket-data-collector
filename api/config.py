"""Configuration for the Polymarket Esports API."""

import os

# Database path — server vs local dev
_SERVER_DB = "/opt/polymarket-collector/data/polymarket_esports.db"
_LOCAL_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "polymarket_esports.db")

DB_PATH = _SERVER_DB if os.path.exists(_SERVER_DB) else _LOCAL_DB

# CORS — allow Vercel frontend and local dev
CORS_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    # Add Vercel deployment URLs here
]

# Also allow local network (10.0.0.X) and Vercel deployments
CORS_ORIGIN_REGEX = r"(https://.*\.vercel\.app|http://10\.0\.0\.\d{1,3}(:\d+)?)"

# Cache TTLs (seconds)
CACHE_TTL = {
    "overview": 120,
    "markets": 120,
    "charts": 90,
    "closing_lines": 300,
    "pre_match": 60,
    "health": 60,
}

# Whale trade threshold in dollars
WHALE_THRESHOLD = 1000

# Game display names
GAME_DISPLAY = {"cod": "Call of Duty", "cs2": "Counter-Strike 2"}
VALID_GAMES = ["cod", "cs2"]

# Default pagination
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200

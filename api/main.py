"""FastAPI application for the Polymarket Esports Dashboard API."""

import os
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import CORS_ORIGINS, CORS_ORIGIN_REGEX
from .db import db_path
from .routes import (
    overview,
    markets,
    daily_volume,
    top_markets,
    trade_distribution,
    whale_analysis,
    buy_sell,
    whale_timeline,
    spread_analysis,
    closing_lines,
    pre_match,
    collection_health,
)

_start_time = time.monotonic()

app = FastAPI(
    title="Polymarket Esports API",
    description="Live analytics for Polymarket esports prediction markets",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_origin_regex=CORS_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(overview.router, prefix="/api")
app.include_router(markets.router, prefix="/api")
app.include_router(daily_volume.router, prefix="/api")
app.include_router(top_markets.router, prefix="/api")
app.include_router(trade_distribution.router, prefix="/api")
app.include_router(whale_analysis.router, prefix="/api")
app.include_router(buy_sell.router, prefix="/api")
app.include_router(whale_timeline.router, prefix="/api")
app.include_router(spread_analysis.router, prefix="/api")
app.include_router(closing_lines.router, prefix="/api")
app.include_router(pre_match.router, prefix="/api")
app.include_router(collection_health.router, prefix="/api")


@app.get("/api/health")
def health():
    """Lightweight health check."""
    path = db_path()
    try:
        size_mb = os.path.getsize(path) / (1024 * 1024)
    except OSError:
        size_mb = 0
    return {
        "status": "ok",
        "db_path": path,
        "db_size_mb": round(size_mb, 1),
        "uptime_seconds": round(time.monotonic() - _start_time),
    }

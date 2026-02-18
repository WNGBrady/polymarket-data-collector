#!/usr/bin/env python3
"""Generate a comprehensive PDF report of Polymarket esports data (COD + CS2).

Usage:
    python generate_tournament_report.py                    # Generate from local DB
    python generate_tournament_report.py --fetch-db         # Copy DB from server first
    python generate_tournament_report.py --db path/to/db    # Custom DB path
    python generate_tournament_report.py --output my.pdf    # Custom output path
"""

import argparse
import json
import os
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, HRFlowable, KeepTogether,
)

# ═══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS & STYLE
# ═══════════════════════════════════════════════════════════════════════════════

PROJECT_ROOT = Path(__file__).parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "polymarket_esports.db"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "Polymarket_Esports_Tournament_Report.pdf"
CHART_DIR = PROJECT_ROOT / "data" / "charts_tournament"

REMOTE_HOST = "159.203.33.91"
REMOTE_USER = "root"
REMOTE_DB_PATH = "/opt/polymarket-collector/data/polymarket_esports.db"

GAME_DISPLAY = {"cod": "Call of Duty", "cs2": "Counter-Strike 2"}
WHALE_THRESHOLD = 1000  # dollars

# Matplotlib dark theme (reused from generate_report.py)
plt.rcParams.update({
    "figure.facecolor": "#0e1117",
    "axes.facecolor": "#0e1117",
    "axes.edgecolor": "#333",
    "axes.labelcolor": "#ccc",
    "xtick.color": "#aaa",
    "ytick.color": "#aaa",
    "text.color": "#eee",
    "grid.color": "#222",
    "grid.alpha": 0.6,
    "font.size": 10,
    "axes.titlesize": 13,
    "figure.dpi": 150,
})

ACCENT = "#00d4aa"
ACCENT2 = "#ff6b6b"
ACCENT3 = "#4ecdc4"
ACCENT4 = "#ffe66d"
ACCENT5 = "#a29bfe"
ACCENT6 = "#fd79a8"
PALETTE = [ACCENT, ACCENT2, ACCENT3, ACCENT4, ACCENT5, ACCENT6]

# ReportLab table style presets
HEADER_BG = colors.HexColor("#1a1a2e")
HEADER_FG = colors.white
ROW_ALT = [colors.white, colors.HexColor("#f8f9fa")]
GRID_COLOR = colors.HexColor("#ddd")


def _table_style(header_bg=HEADER_BG):
    """Return a reusable TableStyle list."""
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_bg),
        ("TEXTCOLOR", (0, 0), (-1, 0), HEADER_FG),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.5, GRID_COLOR),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), ROW_ALT),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ])


# ═══════════════════════════════════════════════════════════════════════════════
#  SERVER DB FETCH
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_db_from_server(local_path: Path, password: Optional[str] = None) -> None:
    """Download the database from the remote server via SFTP."""
    try:
        import paramiko
    except ImportError:
        print("ERROR: paramiko is required for --fetch-db. Install with: pip install paramiko")
        sys.exit(1)

    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Resolve password: CLI arg > env var > interactive prompt
    if not password:
        password = os.environ.get("POLYMARKET_SERVER_PASSWORD")

    print(f"Connecting to {REMOTE_HOST}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if password:
        ssh.connect(REMOTE_HOST, username=REMOTE_USER, password=password,
                    look_for_keys=False, allow_agent=False)
    else:
        # Try key-based auth first, fall back to interactive password
        try:
            ssh.connect(REMOTE_HOST, username=REMOTE_USER, look_for_keys=True, allow_agent=True)
        except (paramiko.ssh_exception.AuthenticationException,
                paramiko.ssh_exception.SSHException):
            import getpass
            password = getpass.getpass(f"Password for {REMOTE_USER}@{REMOTE_HOST}: ")
            ssh.connect(REMOTE_HOST, username=REMOTE_USER, password=password,
                        look_for_keys=False, allow_agent=False)

    sftp = ssh.open_sftp()
    remote_stat = sftp.stat(REMOTE_DB_PATH)
    remote_size_mb = remote_stat.st_size / (1024 * 1024)
    print(f"Remote DB size: {remote_size_mb:.1f} MB")
    print(f"Downloading {REMOTE_DB_PATH} -> {local_path} ...")

    # Progress callback
    downloaded = [0]
    def progress(transferred, total):
        downloaded[0] = transferred
        pct = transferred / total * 100
        print(f"\r  {transferred / 1024 / 1024:.1f} / {total / 1024 / 1024:.1f} MB ({pct:.0f}%)", end="", flush=True)

    sftp.get(REMOTE_DB_PATH, str(local_path), callback=progress)
    print()  # newline after progress

    sftp.close()
    ssh.close()

    local_size_mb = local_path.stat().st_size / (1024 * 1024)
    print(f"Download complete: {local_size_mb:.1f} MB")


# ═══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_dataframes(db_path: Path) -> Dict[str, pd.DataFrame]:
    """Load all relevant tables into pandas DataFrames with game-aware JOINs."""
    conn = sqlite3.connect(str(db_path))

    # trades with game + question from markets
    trades_df = pd.read_sql_query("""
        SELECT t.*, m.game, m.question
        FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        ORDER BY t.timestamp
    """, conn)

    # markets
    markets_df = pd.read_sql_query("SELECT * FROM markets", conn)

    # orderbook snapshots with game from markets
    orderbook_df = pd.read_sql_query("""
        SELECT o.*, m.game, m.question
        FROM orderbook_snapshots o
        JOIN markets m ON o.market_id = m.market_id
    """, conn)

    # final prices
    final_prices_df = pd.read_sql_query("SELECT * FROM final_prices", conn)

    # realtime prices
    realtime_df = pd.read_sql_query("SELECT * FROM realtime_prices", conn)

    # table record counts for health section
    counts = {}
    for table in ["markets", "trades", "price_history", "realtime_prices",
                   "orderbook_snapshots", "final_prices", "open_interest"]:
        row = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table}", conn)
        counts[table] = int(row["cnt"].iloc[0])

    conn.close()

    return {
        "trades": trades_df,
        "markets": markets_df,
        "orderbook": orderbook_df,
        "final_prices": final_prices_df,
        "realtime": realtime_df,
        "counts": counts,
    }


def compute_meta(data: Dict, closing_lines: Optional[pd.DataFrame] = None) -> Dict:
    """Compute summary statistics from loaded DataFrames."""
    trades = data["trades"]
    markets = data["markets"]
    orderbook = data["orderbook"]
    final_prices = data["final_prices"]
    counts = data["counts"]

    meta = {"counts": counts}

    # Overall
    meta["total_markets"] = len(markets)
    meta["total_trades"] = len(trades)
    meta["total_volume"] = trades["size"].sum() if len(trades) > 0 else 0

    # Date range from trades timestamps (unix seconds)
    if len(trades) > 0:
        meta["min_date"] = datetime.fromtimestamp(trades["timestamp"].min(), tz=timezone.utc)
        meta["max_date"] = datetime.fromtimestamp(trades["timestamp"].max(), tz=timezone.utc)
    else:
        meta["min_date"] = meta["max_date"] = None

    # Per-game breakdowns
    meta["games"] = {}
    for game in ["cod", "cs2"]:
        g_markets = markets[markets["game"] == game]
        g_trades = trades[trades["game"] == game]
        g_orderbook = orderbook[orderbook["game"] == game]
        g_final = final_prices[final_prices["game"] == game] if "game" in final_prices.columns else pd.DataFrame()

        gm = {
            "display_name": GAME_DISPLAY.get(game, game),
            "markets": len(g_markets),
            "trades": len(g_trades),
            "volume": g_trades["size"].sum() if len(g_trades) > 0 else 0,
            "orderbook_snapshots": len(g_orderbook),
            "final_prices": len(g_final),
        }

        if len(g_trades) > 0:
            whale_mask = g_trades["size"] >= WHALE_THRESHOLD
            gm["whale_volume"] = g_trades.loc[whale_mask, "size"].sum()
            gm["whale_pct"] = gm["whale_volume"] / gm["volume"] * 100 if gm["volume"] > 0 else 0
            gm["whale_trades"] = whale_mask.sum()
            gm["median_trade"] = g_trades["size"].median()
            gm["max_trade"] = g_trades["size"].max()
            gm["avg_trade"] = g_trades["size"].mean()
        else:
            gm["whale_volume"] = gm["whale_pct"] = gm["whale_trades"] = 0
            gm["median_trade"] = gm["max_trade"] = gm["avg_trade"] = 0

        meta["games"][game] = gm

    # Total whale stats
    if len(trades) > 0:
        whale_mask = trades["size"] >= WHALE_THRESHOLD
        meta["whale_volume"] = trades.loc[whale_mask, "size"].sum()
        meta["whale_pct"] = meta["whale_volume"] / meta["total_volume"] * 100 if meta["total_volume"] > 0 else 0
    else:
        meta["whale_volume"] = meta["whale_pct"] = 0

    # Final prices match count
    if len(final_prices) > 0 and "game_id" in final_prices.columns:
        meta["cs2_matches"] = final_prices[final_prices["game"] == "cs2"]["game_id"].nunique() if "game" in final_prices.columns else 0
    else:
        meta["cs2_matches"] = 0

    # Closing line stats (two rows per match — use home rows to derive favorites)
    if closing_lines is not None and len(closing_lines) > 0:
        meta["closing_line_matches"] = closing_lines["game_id"].nunique()
        home_rows = closing_lines[closing_lines["is_home"] == True]
        fav_wins = 0
        fav_total = 0
        fav_cls = []
        for _, hr in home_rows.iterrows():
            gid = hr["game_id"]
            ar = closing_lines[(closing_lines["game_id"] == gid) & (closing_lines["is_home"] == False)]
            if len(ar) == 0:
                continue
            ar = ar.iloc[0]
            fav_is_home = hr["closing_price"] > 0.5
            fav_won = hr["team_won"] if fav_is_home else ar["team_won"]
            fav_cl = hr["closing_price"] if fav_is_home else ar["closing_price"]
            if fav_won is not None:
                fav_total += 1
                fav_wins += int(fav_won)
                fav_cls.append(fav_cl)
        meta["favorite_win_rate"] = fav_wins / fav_total * 100 if fav_total > 0 else 0
        meta["avg_closing_confidence"] = (sum(fav_cls) / len(fav_cls) * 100) if fav_cls else 0
    else:
        meta["closing_line_matches"] = 0
        meta["favorite_win_rate"] = 0
        meta["avg_closing_confidence"] = 0

    return meta


# ═══════════════════════════════════════════════════════════════════════════════
#  API: GAME START TIMES
# ═══════════════════════════════════════════════════════════════════════════════

GAMMA_API_BASE = "https://gamma-api.polymarket.com/markets"


def fetch_game_start_times(market_ids: List[str]) -> Dict[str, str]:
    """Fetch gameStartTime from Polymarket Gamma API for given market IDs.

    Returns {market_id: ISO timestamp string} for markets that have a gameStartTime.
    """
    start_times = {}
    total = len(market_ids)
    for i, mid in enumerate(market_ids):
        try:
            resp = requests.get(GAMMA_API_BASE, params={"id": mid}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data and len(data) > 0:
                gst = data[0].get("gameStartTime")
                if gst:
                    start_times[mid] = gst
        except Exception as e:
            print(f"  Warning: failed to fetch start time for market {mid}: {e}")

        if (i + 1) % 20 == 0 or (i + 1) == total:
            print(f"  Fetched {i + 1}/{total} market start times...")
        time.sleep(0.1)  # rate limit

    return start_times


# ═══════════════════════════════════════════════════════════════════════════════
#  CLOSING LINE COMPUTATION
# ═══════════════════════════════════════════════════════════════════════════════


def compute_closing_lines(final_prices_df: pd.DataFrame, trades_df: pd.DataFrame,
                          markets_df: pd.DataFrame, start_times: Dict[str, str]) -> pd.DataFrame:
    """Compute pre-match closing line for each team in each match-winner market.

    For each BO3/BO5 market in final_prices, finds per-outcome closing line
    (last trade price before gameStartTime), plus min/max pre-match prices.
    Returns two rows per match (one per team).
    """
    if len(final_prices_df) == 0 or not start_times:
        return pd.DataFrame()

    # Filter to CS2 match-winner markets (BO3/BO5)
    cs2_fp = final_prices_df[final_prices_df["game"] == "cs2"].copy() if "game" in final_prices_df.columns else final_prices_df.copy()
    if len(cs2_fp) == 0:
        return pd.DataFrame()

    # Join with markets to get question text
    cs2_fp = cs2_fp.merge(
        markets_df[["market_id", "question"]],
        on="market_id", how="left"
    )

    # Filter to BO3/BO5 match-winner markets
    bo_mask = cs2_fp["question"].str.contains(r"\(BO[35]\)", case=False, na=False)
    match_markets = cs2_fp[bo_mask].copy()
    if len(match_markets) == 0:
        return pd.DataFrame()

    rows = []
    for _, row in match_markets.iterrows():
        mid = row["market_id"]
        gst_str = start_times.get(mid)
        if not gst_str:
            continue

        # Parse gameStartTime — format: "2026-02-14 08:30:00+00"
        try:
            gst_dt = pd.to_datetime(gst_str, utc=True)
            gst_ts = gst_dt.timestamp()
        except Exception:
            continue

        # Get all pre-match trades for this market
        market_trades = trades_df[trades_df["market_id"] == mid]
        pre_match = market_trades[market_trades["timestamp"] < gst_ts]
        if len(pre_match) == 0:
            continue

        # Determine outcome from post-match price
        post_price = row.get("last_trade_price", None)
        if pd.notna(post_price):
            home_won = post_price > 0.5
        else:
            home_won = None

        home_team = row.get("home_team")
        away_team = row.get("away_team")

        # Compute per-outcome stats from actual trade data
        outcomes = pre_match["outcome"].unique()
        for team_name in [home_team, away_team]:
            is_home = (team_name == home_team)
            team_trades = pre_match[pre_match["outcome"] == team_name]

            if len(team_trades) > 0:
                last_trade = team_trades.loc[team_trades["timestamp"].idxmax()]
                closing_price = last_trade["price"]
                min_price = team_trades["price"].min()
                max_price = team_trades["price"].max()
                n_trades = len(team_trades)
            else:
                # No direct trades for this outcome — infer from other side
                other_team = away_team if is_home else home_team
                other_trades = pre_match[pre_match["outcome"] == other_team]
                if len(other_trades) == 0:
                    continue
                last_other = other_trades.loc[other_trades["timestamp"].idxmax()]
                closing_price = 1 - last_other["price"]
                min_price = 1 - other_trades["price"].max()
                max_price = 1 - other_trades["price"].min()
                n_trades = 0

            team_won = (is_home and home_won) or (not is_home and not home_won) if home_won is not None else None

            rows.append({
                "game_id": row.get("game_id"),
                "market_id": mid,
                "home_team": home_team,
                "away_team": away_team,
                "team": team_name,
                "is_home": is_home,
                "question": row.get("question"),
                "game_start_time": gst_dt,
                "closing_price": closing_price,
                "min_price": min_price,
                "max_price": max_price,
                "final_score": row.get("final_score"),
                "team_won": team_won,
                "n_trades": n_trades,
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    # Sort by game start time, then home team first
    result = result.sort_values(["game_start_time", "is_home"],
                                ascending=[False, False]).reset_index(drop=True)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def format_volume(v: float) -> str:
    """Format volume as $1.04M, $123K, $5.2K, $42 etc."""
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    elif v >= 10_000:
        return f"${v / 1_000:.0f}K"
    elif v >= 1_000:
        return f"${v / 1_000:.1f}K"
    else:
        return f"${v:,.0f}"


def format_number(n) -> str:
    """Format integer with commas."""
    return f"{int(n):,}"


def _save_chart(fig, name: str) -> str:
    """Save a matplotlib figure to CHART_DIR and return the path."""
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    path = CHART_DIR / f"{name}.png"
    fig.savefig(str(path), bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


# ═══════════════════════════════════════════════════════════════════════════════
#  CHART GENERATORS
# ═══════════════════════════════════════════════════════════════════════════════

def chart_daily_volume(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Bar chart of daily trade volume for a specific game."""
    df = trades_df[trades_df["game"] == game].copy()
    if len(df) == 0:
        return None

    df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.date
    daily = df.groupby("date").agg(volume=("size", "sum"), count=("size", "count")).reset_index()

    fig, ax1 = plt.subplots(figsize=(9, 4))
    x = np.arange(len(daily))
    labels = [d.strftime("%m/%d") for d in daily["date"]]

    ax1.bar(x, daily["volume"], color=ACCENT, alpha=0.85, label="Volume ($)")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Volume ($)", color=ACCENT)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax1.grid(True, axis="y", alpha=0.3)

    display = GAME_DISPLAY.get(game, game)
    ax1.set_title(f"{display} — Daily Trading Volume", fontweight="bold")

    ax2 = ax1.twinx()
    ax2.plot(x, daily["count"], color=ACCENT2, marker="o", markersize=5, linewidth=2, label="Trade count")
    ax2.set_ylabel("Trade Count", color=ACCENT2)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8,
               facecolor="#1a1a2e", edgecolor="#333")

    # Add volume labels on bars
    for i, v in enumerate(daily["volume"]):
        if v > 0:
            ax1.text(i, v + daily["volume"].max() * 0.02, format_volume(v),
                     ha="center", va="bottom", fontsize=7, color="#ccc")

    return _save_chart(fig, f"daily_volume_{game}")


def chart_top_markets(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Horizontal bar chart of top 10 markets by volume for a game."""
    df = trades_df[trades_df["game"] == game]
    if len(df) == 0:
        return None

    vol_by_market = df.groupby("question")["size"].sum().sort_values(ascending=False).head(10)
    if len(vol_by_market) == 0:
        return None

    labels = [q[:55] for q in vol_by_market.index][::-1]
    values = vol_by_market.values[::-1]

    fig, ax = plt.subplots(figsize=(9, 5))
    bar_colors = [PALETTE[i % len(PALETTE)] for i in range(len(labels))]
    ax.barh(range(len(labels)), values, color=bar_colors, alpha=0.85)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Volume ($)")

    display = GAME_DISPLAY.get(game, game)
    ax.set_title(f"{display} — Top Markets by Volume", fontweight="bold")
    ax.grid(True, axis="x", alpha=0.3)

    for i, v in enumerate(values):
        ax.text(v + max(values) * 0.01, i, format_volume(v), va="center", fontsize=7, color="#ccc")

    return _save_chart(fig, f"top_markets_{game}")


def chart_trade_size_distribution(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Dual histogram (linear + log) of trade sizes for a game."""
    df = trades_df[trades_df["game"] == game]
    if len(df) == 0:
        return None

    sizes = df["size"].values

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

    # Linear scale
    ax1.hist(sizes, bins=50, color=ACCENT, alpha=0.8, edgecolor="#0e1117")
    ax1.set_xlabel("Trade Size ($)")
    ax1.set_ylabel("Count")
    median_val = np.median(sizes)
    ax1.axvline(median_val, color=ACCENT2, linestyle="--", linewidth=1.5,
                label=f"Median: ${median_val:.0f}")
    ax1.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax1.set_title("Trade Size Distribution", fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # Log scale
    log_sizes = sizes[sizes > 0]
    if len(log_sizes) > 0:
        ax2.hist(log_sizes, bins=np.logspace(np.log10(0.1), np.log10(max(log_sizes)), 40),
                 color=ACCENT3, alpha=0.8, edgecolor="#0e1117")
        ax2.set_xscale("log")
        ax2.axvline(WHALE_THRESHOLD, color=ACCENT2, linestyle="--", linewidth=1.5,
                    label=f"Whale threshold (${WHALE_THRESHOLD})")
        ax2.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax2.set_xlabel("Trade Size ($) — Log Scale")
    ax2.set_ylabel("Count")
    ax2.set_title("Trade Size (Log)", fontweight="bold")
    ax2.grid(True, alpha=0.3)

    display = GAME_DISPLAY.get(game, game)
    fig.suptitle(f"{display}", fontsize=9, color="#888", y=0.02)

    return _save_chart(fig, f"trade_size_dist_{game}")


def chart_whale_vs_retail(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Pie chart: whale vs retail by volume and by count."""
    df = trades_df[trades_df["game"] == game]
    if len(df) == 0:
        return None

    whale_vol = df.loc[df["size"] >= WHALE_THRESHOLD, "size"].sum()
    retail_vol = df.loc[df["size"] < WHALE_THRESHOLD, "size"].sum()
    whale_cnt = (df["size"] >= WHALE_THRESHOLD).sum()
    retail_cnt = (df["size"] < WHALE_THRESHOLD).sum()

    if whale_vol == 0 and retail_vol == 0:
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))

    ax1.pie([whale_vol, retail_vol],
            labels=[f"Whale (>=${WHALE_THRESHOLD})", f"Retail (<${WHALE_THRESHOLD})"],
            colors=[ACCENT2, ACCENT], autopct="%1.1f%%", startangle=90,
            textprops={"color": "#eee", "fontsize": 9})
    ax1.set_title("Share of Volume", fontweight="bold")

    ax2.pie([whale_cnt, retail_cnt],
            labels=[f"Whale (>=${WHALE_THRESHOLD})", f"Retail (<${WHALE_THRESHOLD})"],
            colors=[ACCENT2, ACCENT], autopct="%1.1f%%", startangle=90,
            textprops={"color": "#eee", "fontsize": 9})
    ax2.set_title("Share of Trade Count", fontweight="bold")

    return _save_chart(fig, f"whale_vs_retail_{game}")


def chart_buy_sell_imbalance(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Butterfly bar chart of buy vs sell volume by market."""
    df = trades_df[trades_df["game"] == game]
    if len(df) == 0:
        return None

    buy_vol = df[df["side"] == "BUY"].groupby("question")["size"].sum()
    sell_vol = df[df["side"] == "SELL"].groupby("question")["size"].sum()

    all_mkts = (buy_vol.add(sell_vol, fill_value=0)).sort_values(ascending=False).head(10)
    if len(all_mkts) == 0:
        return None

    labels = [q[:45] for q in all_mkts.index][::-1]
    buys = [buy_vol.get(q, 0) for q in all_mkts.index][::-1]
    sells = [-sell_vol.get(q, 0) for q in all_mkts.index][::-1]

    fig, ax = plt.subplots(figsize=(9, 5))
    y = np.arange(len(labels))
    ax.barh(y, buys, color=ACCENT, alpha=0.85, label="BUY")
    ax.barh(y, sells, color=ACCENT2, alpha=0.85, label="SELL")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Volume ($)")
    ax.axvline(0, color="#555", linewidth=0.8)

    display = GAME_DISPLAY.get(game, game)
    ax.set_title(f"{display} — Buy vs Sell Volume", fontweight="bold")
    ax.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax.grid(True, axis="x", alpha=0.3)

    return _save_chart(fig, f"buy_sell_{game}")


def chart_whale_timeline(trades_df: pd.DataFrame, game: str) -> Optional[str]:
    """Scatter plot of whale trades over time, sized by value, colored by outcome."""
    df = trades_df[(trades_df["game"] == game) & (trades_df["size"] >= WHALE_THRESHOLD)].copy()
    if len(df) == 0:
        return None

    df["dt"] = pd.to_datetime(df["timestamp"], unit="s")
    sizes = df["size"].values
    bubble_sizes = np.clip(sizes / 10, 20, 500)

    # Color by outcome — use a hash of the outcome string for consistent colors
    outcomes = df["outcome"].fillna("Unknown").values
    unique_outcomes = list(set(outcomes))
    outcome_colors = {o: PALETTE[i % len(PALETTE)] for i, o in enumerate(unique_outcomes)}
    clrs = [outcome_colors[o] for o in outcomes]

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.scatter(df["dt"], sizes, c=clrs, s=bubble_sizes, alpha=0.7, edgecolors="#333", linewidth=0.5)
    ax.set_ylabel("Trade Size ($)")
    ax.set_xlabel("Time")

    display = GAME_DISPLAY.get(game, game)
    ax.set_title(f"{display} — Whale Trades (>=${WHALE_THRESHOLD}) Timeline", fontweight="bold")
    ax.grid(True, alpha=0.3)

    # Legend for top outcomes
    legend_outcomes = sorted(unique_outcomes, key=lambda o: -sum(s for s, oc in zip(sizes, outcomes) if oc == o))[:6]
    legend_els = [Line2D([0], [0], marker="o", color="w", markerfacecolor=outcome_colors[o],
                         markersize=7, label=o[:25]) for o in legend_outcomes]
    if legend_els:
        ax.legend(handles=legend_els, loc="upper left", fontsize=7, facecolor="#1a1a2e", edgecolor="#333")

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    fig.autofmt_xdate(rotation=30)

    return _save_chart(fig, f"whale_timeline_{game}")


def chart_spread_analysis(orderbook_df: pd.DataFrame, game: str) -> Optional[str]:
    """Horizontal bar of avg orderbook spread by market."""
    df = orderbook_df[orderbook_df["game"] == game].copy()
    if len(df) == 0:
        return None

    # Filter to rows with valid spread
    df = df[df["spread"].notna() & (df["spread"] > 0) & (df["spread"] < 1)]
    if len(df) == 0:
        return None

    avg_spread = df.groupby("question")["spread"].mean().sort_values()
    if len(avg_spread) == 0:
        return None

    # Show top 15 tightest spreads
    avg_spread = avg_spread.head(15)

    labels = [q[:45] for q in avg_spread.index]
    values = avg_spread.values

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(range(len(labels)), values, color=ACCENT4, alpha=0.85)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Average Bid-Ask Spread")

    display = GAME_DISPLAY.get(game, game)
    ax.set_title(f"{display} — Tightest Orderbook Spreads", fontweight="bold")
    ax.grid(True, axis="x", alpha=0.3)

    for i, v in enumerate(values):
        ax.text(v + 0.005, i, f"{v:.3f}", va="center", fontsize=7, color="#ccc")

    return _save_chart(fig, f"spread_{game}")


def chart_comparison(meta: Dict) -> Optional[str]:
    """Grouped bar chart comparing COD vs CS2 key metrics."""
    cod = meta["games"].get("cod", {})
    cs2 = meta["games"].get("cs2", {})

    if not cod.get("trades") and not cs2.get("trades"):
        return None

    metrics = ["Volume ($)", "Trades", "Markets", "Avg Trade ($)", "Whale %"]
    cod_vals = [
        cod.get("volume", 0),
        cod.get("trades", 0),
        cod.get("markets", 0),
        cod.get("avg_trade", 0),
        cod.get("whale_pct", 0),
    ]
    cs2_vals = [
        cs2.get("volume", 0),
        cs2.get("trades", 0),
        cs2.get("markets", 0),
        cs2.get("avg_trade", 0),
        cs2.get("whale_pct", 0),
    ]

    fig, axes = plt.subplots(1, 5, figsize=(12, 4))
    fig.suptitle("Call of Duty vs Counter-Strike 2 — Key Metrics", fontweight="bold", fontsize=13)

    for i, (metric, cv, sv) in enumerate(zip(metrics, cod_vals, cs2_vals)):
        ax = axes[i]
        bars = ax.bar([0, 1], [cv, sv], color=[ACCENT, ACCENT2], alpha=0.85, width=0.6)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["COD", "CS2"], fontsize=8)
        ax.set_title(metric, fontsize=9, fontweight="bold")
        ax.grid(True, axis="y", alpha=0.3)

        # Value labels
        for bar, val in zip(bars, [cv, sv]):
            if metric == "Volume ($)":
                label = format_volume(val)
            elif metric == "Whale %":
                label = f"{val:.1f}%"
            elif metric == "Avg Trade ($)":
                label = f"${val:.0f}"
            else:
                label = format_number(val)
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    label, ha="center", va="bottom", fontsize=7, color="#ccc")

    fig.tight_layout(rect=[0, 0, 1, 0.92])
    return _save_chart(fig, "comparison")


def chart_collection_health(data: Dict, meta: Dict) -> Optional[str]:
    """Daily record counts (trades + orderbook) for collection health monitoring."""
    trades = data["trades"]
    orderbook = data["orderbook"]

    if len(trades) == 0 and len(orderbook) == 0:
        return None

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
    fig.subplots_adjust(hspace=0.15)

    # Trades per day per game
    if len(trades) > 0:
        trades_copy = trades.copy()
        trades_copy["date"] = pd.to_datetime(trades_copy["timestamp"], unit="s").dt.date
        daily_trades = trades_copy.groupby(["date", "game"]).size().unstack(fill_value=0)

        x_dates = daily_trades.index
        x = np.arange(len(x_dates))
        width = 0.35

        if "cod" in daily_trades.columns:
            ax1.bar(x - width / 2, daily_trades["cod"], width, color=ACCENT, alpha=0.85, label="COD")
        if "cs2" in daily_trades.columns:
            ax1.bar(x + width / 2, daily_trades["cs2"], width, color=ACCENT2, alpha=0.85, label="CS2")

        ax1.set_ylabel("Trade Count")
        ax1.set_title("Daily Data Collection — Trades", fontweight="bold")
        ax1.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
        ax1.grid(True, axis="y", alpha=0.3)

    # Orderbook snapshots per day per game
    if len(orderbook) > 0:
        ob_copy = orderbook.copy()
        # orderbook timestamps are in milliseconds
        ob_copy["date"] = pd.to_datetime(ob_copy["timestamp"], unit="ms").dt.date
        daily_ob = ob_copy.groupby(["date", "game"]).size().unstack(fill_value=0)

        x_dates_ob = daily_ob.index
        x_ob = np.arange(len(x_dates_ob))

        if "cod" in daily_ob.columns:
            ax2.bar(x_ob - width / 2, daily_ob["cod"], width, color=ACCENT, alpha=0.85, label="COD")
        if "cs2" in daily_ob.columns:
            ax2.bar(x_ob + width / 2, daily_ob["cs2"], width, color=ACCENT2, alpha=0.85, label="CS2")

        ax2.set_ylabel("Snapshot Count")
        ax2.set_title("Daily Data Collection — Orderbook Snapshots", fontweight="bold")
        ax2.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
        ax2.grid(True, axis="y", alpha=0.3)

        date_labels = [d.strftime("%m/%d") for d in x_dates_ob]
        ax2.set_xticks(x_ob)
        ax2.set_xticklabels(date_labels, rotation=45, ha="right", fontsize=8)
    elif len(trades) > 0:
        date_labels = [d.strftime("%m/%d") for d in x_dates]
        ax1.set_xticks(x)
        ax1.set_xticklabels(date_labels, rotation=45, ha="right", fontsize=8)

    return _save_chart(fig, "collection_health")


def chart_closing_lines(closing_lines_df: pd.DataFrame) -> Optional[str]:
    """Grouped horizontal bar chart of pre-match closing lines for both teams."""
    if closing_lines_df is None or len(closing_lines_df) == 0:
        return None

    # Get unique matches (by game_id), most recent first
    match_ids = closing_lines_df.drop_duplicates("game_id")["game_id"].tolist()[:20]
    matches = closing_lines_df[closing_lines_df["game_id"].isin(match_ids)]

    labels = []
    home_prices = []
    away_prices = []
    scores = []

    for gid in reversed(match_ids):
        m = matches[matches["game_id"] == gid]
        home_row = m[m["is_home"] == True]
        away_row = m[m["is_home"] == False]
        if len(home_row) == 0 or len(away_row) == 0:
            continue
        hr = home_row.iloc[0]
        ar = away_row.iloc[0]
        labels.append(f"{hr['home_team']} vs {hr['away_team']}")
        home_prices.append(hr["closing_price"])
        away_prices.append(ar["closing_price"])
        score_str = str(hr.get("final_score", "")).replace('"', '')
        scores.append(score_str)

    if not labels:
        return None

    fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.45)))
    y = np.arange(len(labels))
    bar_h = 0.35

    ax.barh(y + bar_h / 2, home_prices, bar_h, color=ACCENT, alpha=0.85, label="Home team")
    ax.barh(y - bar_h / 2, away_prices, bar_h, color=ACCENT2, alpha=0.85, label="Away team")
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Closing Line Price")
    ax.axvline(0.5, color="#aaa", linestyle="--", linewidth=1.2)
    ax.set_title("CS2 Pre-Match Closing Lines — Both Teams", fontweight="bold")
    ax.grid(True, axis="x", alpha=0.3)
    ax.set_xlim(0, 1.05)

    for i, (hp, ap, s) in enumerate(zip(home_prices, away_prices, scores)):
        ax.text(hp + 0.01, i + bar_h / 2, f"{hp:.2f}", va="center", fontsize=6.5, color=ACCENT)
        ax.text(ap + 0.01, i - bar_h / 2, f"{ap:.2f}", va="center", fontsize=6.5, color=ACCENT2)
        ax.text(0.98, i, s, va="center", ha="right", fontsize=6.5, color="#999")

    ax.legend(loc="lower right", fontsize=8, facecolor="#1a1a2e", edgecolor="#333")

    return _save_chart(fig, "closing_lines_cs2")


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_pdf(charts: Dict[str, Optional[str]], data: Dict, meta: Dict,
              output_path: Path, db_path: Path,
              closing_lines: Optional[pd.DataFrame] = None) -> None:
    """Build the full PDF report."""
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        topMargin=0.6 * inch,
        bottomMargin=0.5 * inch,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("Title2", parent=styles["Title"], fontSize=22, spaceAfter=6,
                              textColor=colors.HexColor("#1a1a2e")))
    styles.add(ParagraphStyle("Sub", parent=styles["Normal"], fontSize=11,
                              textColor=colors.grey, spaceAfter=12))
    styles.add(ParagraphStyle("H1", parent=styles["Heading1"], fontSize=16, spaceBefore=16,
                              spaceAfter=8, textColor=colors.HexColor("#1a1a2e")))
    styles.add(ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, spaceBefore=12,
                              spaceAfter=6, textColor=colors.HexColor("#2d3436")))
    styles.add(ParagraphStyle("Body", parent=styles["Normal"], fontSize=9.5, leading=13,
                              spaceAfter=6))
    styles.add(ParagraphStyle("SmallBody", parent=styles["Normal"], fontSize=8.5, leading=11,
                              spaceAfter=4))
    styles.add(ParagraphStyle("Caption", parent=styles["Normal"], fontSize=8,
                              textColor=colors.grey, alignment=TA_CENTER, spaceAfter=10))

    story = []

    # ── Title Page ─────────────────────────────────────────────────────
    story.append(Spacer(1, 1.2 * inch))
    story.append(Paragraph("Polymarket Esports Data Report", styles["Title2"]))

    date_range = ""
    if meta.get("min_date") and meta.get("max_date"):
        d1 = meta["min_date"].strftime("%b %d, %Y")
        d2 = meta["max_date"].strftime("%b %d, %Y")
        date_range = f"{d1} — {d2}"
    story.append(Paragraph(f"Call of Duty League + Counter-Strike 2 — {date_range}", styles["Sub"]))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor(ACCENT)))
    story.append(Spacer(1, 0.3 * inch))

    # Summary metrics table
    cod_m = meta["games"].get("cod", {})
    cs2_m = meta["games"].get("cs2", {})

    summary_data = [
        ["Metric", "Call of Duty", "Counter-Strike 2", "Total"],
        ["Markets", format_number(cod_m.get("markets", 0)),
         format_number(cs2_m.get("markets", 0)), format_number(meta["total_markets"])],
        ["Trades", format_number(cod_m.get("trades", 0)),
         format_number(cs2_m.get("trades", 0)), format_number(meta["total_trades"])],
        ["Volume", format_volume(cod_m.get("volume", 0)),
         format_volume(cs2_m.get("volume", 0)), format_volume(meta["total_volume"])],
        ["Whale Volume", format_volume(cod_m.get("whale_volume", 0)),
         format_volume(cs2_m.get("whale_volume", 0)), format_volume(meta["whale_volume"])],
        ["Whale %", f"{cod_m.get('whale_pct', 0):.1f}%",
         f"{cs2_m.get('whale_pct', 0):.1f}%", f"{meta['whale_pct']:.1f}%"],
        ["Orderbook Snapshots", format_number(cod_m.get("orderbook_snapshots", 0)),
         format_number(cs2_m.get("orderbook_snapshots", 0)),
         format_number(meta["counts"].get("orderbook_snapshots", 0))],
        ["Final Price Snapshots", format_number(cod_m.get("final_prices", 0)),
         format_number(cs2_m.get("final_prices", 0)),
         format_number(meta["counts"].get("final_prices", 0))],
        ["Realtime Price Records", "—", "—",
         format_number(meta["counts"].get("realtime_prices", 0))],
    ]

    t_summary = Table(summary_data, colWidths=[1.6 * inch, 1.5 * inch, 1.5 * inch, 1.2 * inch])
    t_summary.setStyle(_table_style())
    story.append(t_summary)
    story.append(Spacer(1, 0.2 * inch))

    # Intro paragraph
    story.append(Paragraph(
        f"This report analyzes prediction market data collected from Polymarket for "
        f"esports events across two games: <b>Call of Duty League (CDL)</b> and "
        f"<b>Counter-Strike 2 (CS2)</b>. Data was collected continuously from "
        f"{'a remote server' if date_range else 'the local database'}, capturing "
        f"markets, trades, orderbook snapshots, and match results.",
        styles["Body"]
    ))
    story.append(PageBreak())

    # ── Data Collection Health ─────────────────────────────────────────
    story.append(Paragraph("1. Data Collection Health", styles["H1"]))

    counts = meta["counts"]
    health_data = [
        ["Table", "Records", "Notes"],
        ["markets", format_number(counts.get("markets", 0)),
         f"COD: {cod_m.get('markets', 0)}, CS2: {cs2_m.get('markets', 0)}"],
        ["trades", format_number(counts.get("trades", 0)),
         f"${format_volume(meta['total_volume'])} total volume"],
        ["orderbook_snapshots", format_number(counts.get("orderbook_snapshots", 0)),
         "60-second polling intervals"],
        ["final_prices", format_number(counts.get("final_prices", 0)),
         f"{meta.get('cs2_matches', 0)} unique CS2 matches"],
        ["realtime_prices", format_number(counts.get("realtime_prices", 0)),
         "WebSocket price feeds"],
        ["price_history", format_number(counts.get("price_history", 0)),
         "API auth required — not collected" if counts.get("price_history", 0) == 0 else ""],
        ["open_interest", format_number(counts.get("open_interest", 0)),
         "Not available for esports" if counts.get("open_interest", 0) == 0 else ""],
    ]
    t_health = Table(health_data, colWidths=[1.6 * inch, 1.2 * inch, 3.0 * inch])
    t_health.setStyle(_table_style())
    story.append(t_health)
    story.append(Spacer(1, 0.15 * inch))

    if charts.get("collection_health"):
        story.append(Image(charts["collection_health"], width=6.5 * inch, height=3.8 * inch))
        story.append(Paragraph(
            "Daily data collection rates for trades and orderbook snapshots, broken down by game.",
            styles["Caption"]))

    story.append(PageBreak())

    # ── Call of Duty Section ───────────────────────────────────────────
    story.append(Paragraph("2. Call of Duty", styles["H1"]))

    cod_trades = len(data["trades"][data["trades"]["game"] == "cod"])
    if cod_trades > 0:
        story.append(Paragraph(
            f"The CDL dataset contains <b>{format_number(cod_m.get('markets', 0))} markets</b> with "
            f"<b>{format_number(cod_m.get('trades', 0))} trades</b> totaling "
            f"<b>{format_volume(cod_m.get('volume', 0))}</b> in volume. "
            f"Median trade size is <b>${cod_m.get('median_trade', 0):.2f}</b>, with the largest "
            f"single trade at <b>${cod_m.get('max_trade', 0):,.2f}</b>. "
            f"Whale trades (>=${WHALE_THRESHOLD}) account for "
            f"<b>{cod_m.get('whale_pct', 0):.1f}%</b> of volume.",
            styles["Body"]
        ))

        # Top markets table
        cod_vol = data["trades"][data["trades"]["game"] == "cod"].groupby("question")["size"].sum()
        cod_top = cod_vol.sort_values(ascending=False).head(5)
        if len(cod_top) > 0:
            story.append(Paragraph("2.1 Top Markets by Volume", styles["H2"]))
            top_data = [["Market", "Volume", "Share"]]
            total_cod_vol = cod_m.get("volume", 1)
            for q, v in cod_top.items():
                top_data.append([q[:60], format_volume(v), f"{v / total_cod_vol * 100:.1f}%"])
            t_top = Table(top_data, colWidths=[3.5 * inch, 1.2 * inch, 1.0 * inch])
            t_top.setStyle(_table_style())
            story.append(t_top)
            story.append(Spacer(1, 0.1 * inch))

        if charts.get("top_markets_cod"):
            story.append(Image(charts["top_markets_cod"], width=6.5 * inch, height=3.5 * inch))
            story.append(Paragraph("COD — Top markets by trading volume.", styles["Caption"]))

        if charts.get("daily_volume_cod"):
            story.append(Image(charts["daily_volume_cod"], width=6.5 * inch, height=2.8 * inch))
            story.append(Paragraph("COD — Daily trading volume with trade count overlay.", styles["Caption"]))

        story.append(PageBreak())

        # Whale analysis
        story.append(Paragraph("2.2 COD Whale Analysis", styles["H2"]))
        story.append(Paragraph(
            f"<b>{format_number(cod_m.get('whale_trades', 0))} whale trades</b> "
            f"(>=${WHALE_THRESHOLD}) generated <b>{format_volume(cod_m.get('whale_volume', 0))}</b> "
            f"({cod_m.get('whale_pct', 0):.1f}% of COD volume).",
            styles["Body"]
        ))

        if charts.get("whale_vs_retail_cod"):
            story.append(Image(charts["whale_vs_retail_cod"], width=6 * inch, height=2.7 * inch))
            story.append(Paragraph("COD whale vs retail breakdown by volume and trade count.", styles["Caption"]))

        if charts.get("whale_timeline_cod"):
            story.append(Image(charts["whale_timeline_cod"], width=6.5 * inch, height=3 * inch))
            story.append(Paragraph("COD whale trade timeline. Bubble size proportional to trade value.", styles["Caption"]))

        if charts.get("trade_size_dist_cod"):
            story.append(Image(charts["trade_size_dist_cod"], width=6.5 * inch, height=2.7 * inch))
            story.append(Paragraph("COD trade size distribution (linear and log scale).", styles["Caption"]))

        if charts.get("buy_sell_cod"):
            story.append(Spacer(1, 0.1 * inch))
            story.append(Paragraph("2.3 COD Buy/Sell Imbalance", styles["H2"]))
            cod_buys = data["trades"][(data["trades"]["game"] == "cod") & (data["trades"]["side"] == "BUY")]["size"].sum()
            cod_sells = data["trades"][(data["trades"]["game"] == "cod") & (data["trades"]["side"] == "SELL")]["size"].sum()
            cod_total = cod_buys + cod_sells
            buy_pct = cod_buys / cod_total * 100 if cod_total > 0 else 0
            story.append(Paragraph(
                f"Buy volume: <b>{format_volume(cod_buys)} ({buy_pct:.1f}%)</b> vs "
                f"Sell volume: <b>{format_volume(cod_sells)} ({100 - buy_pct:.1f}%)</b>.",
                styles["Body"]
            ))
            story.append(Image(charts["buy_sell_cod"], width=6.5 * inch, height=3.5 * inch))
            story.append(Paragraph("COD buy (green) vs sell (red) volume by market.", styles["Caption"]))
    else:
        story.append(Paragraph("No COD trade data available in this dataset.", styles["Body"]))

    story.append(PageBreak())

    # ── Counter-Strike 2 Section ───────────────────────────────────────
    story.append(Paragraph("3. Counter-Strike 2", styles["H1"]))

    cs2_trades = len(data["trades"][data["trades"]["game"] == "cs2"])
    if cs2_trades > 0:
        story.append(Paragraph(
            f"The CS2 dataset contains <b>{format_number(cs2_m.get('markets', 0))} markets</b> with "
            f"<b>{format_number(cs2_m.get('trades', 0))} trades</b> totaling "
            f"<b>{format_volume(cs2_m.get('volume', 0))}</b> in volume. "
            f"Median trade size is <b>${cs2_m.get('median_trade', 0):.2f}</b>, with the largest "
            f"single trade at <b>${cs2_m.get('max_trade', 0):,.2f}</b>. "
            f"Whale trades (>=${WHALE_THRESHOLD}) account for "
            f"<b>{cs2_m.get('whale_pct', 0):.1f}%</b> of volume.",
            styles["Body"]
        ))

        # Top markets table
        cs2_vol = data["trades"][data["trades"]["game"] == "cs2"].groupby("question")["size"].sum()
        cs2_top = cs2_vol.sort_values(ascending=False).head(5)
        if len(cs2_top) > 0:
            story.append(Paragraph("3.1 Top Markets by Volume", styles["H2"]))
            top_data = [["Market", "Volume", "Share"]]
            total_cs2_vol = cs2_m.get("volume", 1)
            for q, v in cs2_top.items():
                top_data.append([q[:60], format_volume(v), f"{v / total_cs2_vol * 100:.1f}%"])
            t_top = Table(top_data, colWidths=[3.5 * inch, 1.2 * inch, 1.0 * inch])
            t_top.setStyle(_table_style())
            story.append(t_top)
            story.append(Spacer(1, 0.1 * inch))

        if charts.get("top_markets_cs2"):
            story.append(Image(charts["top_markets_cs2"], width=6.5 * inch, height=3.5 * inch))
            story.append(Paragraph("CS2 — Top markets by trading volume.", styles["Caption"]))

        if charts.get("daily_volume_cs2"):
            story.append(Image(charts["daily_volume_cs2"], width=6.5 * inch, height=2.8 * inch))
            story.append(Paragraph("CS2 — Daily trading volume with trade count overlay.", styles["Caption"]))

        story.append(PageBreak())

        # Whale analysis
        story.append(Paragraph("3.2 CS2 Whale Analysis", styles["H2"]))
        story.append(Paragraph(
            f"<b>{format_number(cs2_m.get('whale_trades', 0))} whale trades</b> "
            f"(>=${WHALE_THRESHOLD}) generated <b>{format_volume(cs2_m.get('whale_volume', 0))}</b> "
            f"({cs2_m.get('whale_pct', 0):.1f}% of CS2 volume).",
            styles["Body"]
        ))

        if charts.get("whale_vs_retail_cs2"):
            story.append(Image(charts["whale_vs_retail_cs2"], width=6 * inch, height=2.7 * inch))
            story.append(Paragraph("CS2 whale vs retail breakdown by volume and trade count.", styles["Caption"]))

        if charts.get("whale_timeline_cs2"):
            story.append(Image(charts["whale_timeline_cs2"], width=6.5 * inch, height=3 * inch))
            story.append(Paragraph("CS2 whale trade timeline. Bubble size proportional to trade value.", styles["Caption"]))

        if charts.get("trade_size_dist_cs2"):
            story.append(Image(charts["trade_size_dist_cs2"], width=6.5 * inch, height=2.7 * inch))
            story.append(Paragraph("CS2 trade size distribution (linear and log scale).", styles["Caption"]))

        # Buy/Sell
        if charts.get("buy_sell_cs2"):
            story.append(Spacer(1, 0.1 * inch))
            story.append(Paragraph("3.3 CS2 Buy/Sell Imbalance", styles["H2"]))
            cs2_buys = data["trades"][(data["trades"]["game"] == "cs2") & (data["trades"]["side"] == "BUY")]["size"].sum()
            cs2_sells = data["trades"][(data["trades"]["game"] == "cs2") & (data["trades"]["side"] == "SELL")]["size"].sum()
            cs2_total_bs = cs2_buys + cs2_sells
            buy_pct = cs2_buys / cs2_total_bs * 100 if cs2_total_bs > 0 else 0
            story.append(Paragraph(
                f"Buy volume: <b>{format_volume(cs2_buys)} ({buy_pct:.1f}%)</b> vs "
                f"Sell volume: <b>{format_volume(cs2_sells)} ({100 - buy_pct:.1f}%)</b>.",
                styles["Body"]
            ))
            story.append(Image(charts["buy_sell_cs2"], width=6.5 * inch, height=3.5 * inch))
            story.append(Paragraph("CS2 buy (green) vs sell (red) volume by market.", styles["Caption"]))

        story.append(PageBreak())

        # Closing Lines & Match Results
        has_closing = closing_lines is not None and len(closing_lines) > 0
        if has_closing or charts.get("closing_lines_cs2") or meta.get("cs2_matches", 0) > 0:
            story.append(Paragraph("3.4 CS2 Closing Lines &amp; Match Results", styles["H2"]))

            if has_closing:
                n_matches = closing_lines["game_id"].nunique()
                # Compute favorite win stats from home-team rows
                home_rows = closing_lines[closing_lines["is_home"] == True].copy()
                fav_data = []
                for _, hr in home_rows.iterrows():
                    gid = hr["game_id"]
                    away_r = closing_lines[(closing_lines["game_id"] == gid) & (closing_lines["is_home"] == False)]
                    if len(away_r) == 0:
                        continue
                    ar = away_r.iloc[0]
                    home_cl = hr["closing_price"]
                    fav_is_home = home_cl > 0.5
                    fav_cl = home_cl if fav_is_home else ar["closing_price"]
                    fav_won = hr["team_won"] if fav_is_home else ar["team_won"]
                    if fav_won is not None:
                        fav_data.append({"fav_cl": fav_cl, "fav_won": fav_won})
                fav_df = pd.DataFrame(fav_data) if fav_data else pd.DataFrame()
                fav_wins = int(fav_df["fav_won"].sum()) if len(fav_df) > 0 else 0
                fav_total = len(fav_df)
                fav_pct = fav_wins / fav_total * 100 if fav_total > 0 else 0
                avg_conf = fav_df["fav_cl"].mean() * 100 if len(fav_df) > 0 else 0

                story.append(Paragraph(
                    f"Pre-match closing lines were computed for <b>{n_matches} CS2 matches</b> "
                    f"({len(closing_lines)} team lines) by finding the last traded price per team "
                    f"before each match's scheduled start time (fetched from the Polymarket API).",
                    styles["Body"]
                ))

                # Closing lines table — one row per team
                cl_data = [["Team", "Match", "Start", "Close", "Min", "Max", "Won?"]]
                game_ids_seen = []
                for gid in closing_lines["game_id"].unique():
                    match_rows = closing_lines[closing_lines["game_id"] == gid].sort_values("is_home", ascending=False)
                    for _, r in match_rows.iterrows():
                        matchup = f"vs {r['away_team']}" if r["is_home"] else f"vs {r['home_team']}"
                        start_t = r["game_start_time"].strftime("%m/%d %H:%M") if pd.notna(r.get("game_start_time")) else ""
                        cl_price = f"{r['closing_price']:.2f}"
                        mn = f"{r['min_price']:.2f}"
                        mx = f"{r['max_price']:.2f}"
                        won = "Yes" if r.get("team_won") is True else ("No" if r.get("team_won") is False else "")
                        cl_data.append([str(r["team"])[:18], matchup[:22], start_t, cl_price, mn, mx, won])

                t_cl = Table(cl_data, colWidths=[1.2 * inch, 1.4 * inch, 0.8 * inch, 0.6 * inch, 0.55 * inch, 0.55 * inch, 0.5 * inch])
                cl_style = _table_style()
                # Add alternating shading per match (every 2 rows)
                t_cl.setStyle(cl_style)
                story.append(t_cl)
                story.append(Spacer(1, 0.1 * inch))

                # Summary stats
                story.append(Paragraph(
                    f"Pre-match favorite won <b>{fav_wins} of {fav_total} matches "
                    f"({fav_pct:.0f}%)</b>. Average favorite closing line: <b>{avg_conf:.0f}%</b>.",
                    styles["Body"]
                ))

                # Narrative
                if fav_pct >= 70:
                    accuracy_desc = "strong predictive accuracy"
                elif fav_pct >= 55:
                    accuracy_desc = "moderate predictive accuracy"
                else:
                    accuracy_desc = "limited predictive accuracy for favorites"
                story.append(Paragraph(
                    f"The Polymarket closing lines showed {accuracy_desc}, with pre-match favorites "
                    f"winning {fav_pct:.0f}% of the time. The average closing line for the "
                    f"favored team was {avg_conf:.0f}%, suggesting the market was "
                    f"{'confident' if avg_conf >= 65 else 'relatively uncertain'} in its pre-match "
                    f"assessments.",
                    styles["Body"]
                ))
            else:
                story.append(Paragraph(
                    "Closing line data was not available. Run without --no-api to fetch "
                    "pre-match start times from the Polymarket API.",
                    styles["Body"]
                ))

            if charts.get("closing_lines_cs2"):
                story.append(Image(charts["closing_lines_cs2"], width=6.5 * inch, height=4 * inch))
                story.append(Paragraph(
                    "CS2 pre-match closing lines — both teams shown. "
                    "Green = home team, red = away team.",
                    styles["Caption"]))

            story.append(PageBreak())
    else:
        story.append(Paragraph("No CS2 trade data available in this dataset.", styles["Body"]))
        story.append(PageBreak())

    # ── Liquidity Analysis ─────────────────────────────────────────────
    story.append(Paragraph("4. Liquidity Analysis", styles["H1"]))
    story.append(Paragraph(
        "Orderbook spread analysis reveals the liquidity characteristics of each game's markets. "
        "Tighter spreads indicate more active market making and better execution for traders.",
        styles["Body"]
    ))

    for game_key, game_label in [("cod", "Call of Duty"), ("cs2", "Counter-Strike 2")]:
        chart_key = f"spread_{game_key}"
        if charts.get(chart_key):
            story.append(Paragraph(f"4.{1 if game_key == 'cod' else 2} {game_label} Spreads", styles["H2"]))

            # Compute avg spread stats
            ob_game = data["orderbook"][data["orderbook"]["game"] == game_key]
            valid_ob = ob_game[ob_game["spread"].notna() & (ob_game["spread"] > 0) & (ob_game["spread"] < 1)]
            if len(valid_ob) > 0:
                avg_spread = valid_ob["spread"].mean()
                median_spread = valid_ob["spread"].median()
                story.append(Paragraph(
                    f"Average spread: <b>{avg_spread:.3f}</b> | "
                    f"Median spread: <b>{median_spread:.3f}</b> | "
                    f"Snapshots analyzed: <b>{format_number(len(valid_ob))}</b>",
                    styles["Body"]
                ))

            story.append(Image(charts[chart_key], width=6.5 * inch, height=3.3 * inch))
            story.append(Paragraph(
                f"{game_label} — Markets with tightest average bid-ask spreads.",
                styles["Caption"]))

    story.append(PageBreak())

    # ── COD vs CS2 Comparison ──────────────────────────────────────────
    story.append(Paragraph("5. COD vs CS2 Comparison", styles["H1"]))

    if charts.get("comparison"):
        story.append(Image(charts["comparison"], width=6.5 * inch, height=2.8 * inch))
        story.append(Paragraph(
            "Side-by-side comparison of key metrics between Call of Duty and Counter-Strike 2.",
            styles["Caption"]))

    # Comparison table
    vol_ratio = cs2_m.get("volume", 0) / cod_m.get("volume", 1) if cod_m.get("volume", 0) > 0 else float("inf")

    comp_data = [
        ["Metric", "Call of Duty", "Counter-Strike 2", "Ratio (CS2/COD)"],
        ["Volume", format_volume(cod_m.get("volume", 0)),
         format_volume(cs2_m.get("volume", 0)), f"{vol_ratio:.1f}x"],
        ["Trades", format_number(cod_m.get("trades", 0)),
         format_number(cs2_m.get("trades", 0)),
         f"{cs2_m.get('trades', 0) / max(cod_m.get('trades', 1), 1):.1f}x"],
        ["Markets", format_number(cod_m.get("markets", 0)),
         format_number(cs2_m.get("markets", 0)),
         f"{cs2_m.get('markets', 0) / max(cod_m.get('markets', 1), 1):.1f}x"],
        ["Avg Trade Size", f"${cod_m.get('avg_trade', 0):.2f}",
         f"${cs2_m.get('avg_trade', 0):.2f}",
         f"{cs2_m.get('avg_trade', 0) / max(cod_m.get('avg_trade', 1), 0.01):.1f}x"],
        ["Median Trade", f"${cod_m.get('median_trade', 0):.2f}",
         f"${cs2_m.get('median_trade', 0):.2f}", ""],
        ["Max Trade", f"${cod_m.get('max_trade', 0):,.2f}",
         f"${cs2_m.get('max_trade', 0):,.2f}", ""],
        ["Whale %", f"{cod_m.get('whale_pct', 0):.1f}%",
         f"{cs2_m.get('whale_pct', 0):.1f}%", ""],
        ["Orderbook Snapshots", format_number(cod_m.get("orderbook_snapshots", 0)),
         format_number(cs2_m.get("orderbook_snapshots", 0)), ""],
    ]
    t_comp = Table(comp_data, colWidths=[1.5 * inch, 1.4 * inch, 1.5 * inch, 1.3 * inch])
    t_comp.setStyle(_table_style())
    story.append(Spacer(1, 0.15 * inch))
    story.append(t_comp)
    story.append(Spacer(1, 0.15 * inch))

    # Narrative comparison
    if cod_m.get("volume", 0) > 0 and cs2_m.get("volume", 0) > 0:
        story.append(Paragraph(
            f"Counter-Strike 2 dominates the Polymarket esports landscape with "
            f"<b>{vol_ratio:.1f}x</b> the trading volume of Call of Duty. CS2's larger volume "
            f"is driven by both more markets ({cs2_m.get('markets', 0)} vs {cod_m.get('markets', 0)}) "
            f"and higher per-trade sizes (${cs2_m.get('avg_trade', 0):.0f} avg vs "
            f"${cod_m.get('avg_trade', 0):.0f} avg).",
            styles["Body"]
        ))

    # ── Footer ─────────────────────────────────────────────────────────
    story.append(Spacer(1, 0.4 * inch))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#ccc")))
    story.append(Paragraph(
        f"Report generated {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
        f"Database: {db_path.name} ({db_path.stat().st_size / 1024 / 1024:.1f} MB) | "
        f"Collection period: {date_range}",
        styles["Caption"]
    ))

    doc.build(story)
    print(f"PDF saved to: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Generate Polymarket esports tournament report")
    parser.add_argument("--fetch-db", action="store_true",
                        help="Download database from remote server before generating")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to database file (default: data/polymarket_esports.db)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output PDF path (default: data/Polymarket_Esports_Tournament_Report.pdf)")
    parser.add_argument("--password", type=str, default=None,
                        help="Server password for --fetch-db (or set POLYMARKET_SERVER_PASSWORD env var)")
    parser.add_argument("--no-api", action="store_true",
                        help="Skip API calls (closing line data won't be available)")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH
    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    # Step 1: Optionally fetch DB from server
    if args.fetch_db:
        fetch_db_from_server(db_path, password=args.password)

    # Verify DB exists
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("  Run with --fetch-db to download from server, or specify --db path")
        sys.exit(1)

    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"Using database: {db_path} ({db_size_mb:.1f} MB)")

    # Step 2: Load data
    print("Loading data...")
    data = load_dataframes(db_path)

    trades_df = data["trades"]
    markets_df = data["markets"]
    orderbook_df = data["orderbook"]
    final_prices_df = data["final_prices"]

    print(f"  Markets: {len(markets_df)}")
    print(f"  Trades: {len(trades_df)}")
    print(f"  Orderbook snapshots: {len(orderbook_df)}")
    print(f"  Final prices: {len(final_prices_df)}")

    # Step 3: Fetch game start times & compute closing lines
    closing_lines = None
    if not args.no_api:
        # Get unique market_ids from CS2 final_prices that are BO3/BO5 match-winner markets
        cs2_fp = final_prices_df[final_prices_df["game"] == "cs2"] if "game" in final_prices_df.columns else final_prices_df
        if len(cs2_fp) > 0:
            fp_with_q = cs2_fp.merge(markets_df[["market_id", "question"]], on="market_id", how="left")
            bo_mask = fp_with_q["question"].str.contains(r"\(BO[35]\)", case=False, na=False)
            bo_market_ids = fp_with_q.loc[bo_mask, "market_id"].unique().tolist()

            if bo_market_ids:
                print(f"Fetching game start times for {len(bo_market_ids)} match-winner markets...")
                start_times = fetch_game_start_times(bo_market_ids)
                print(f"  Got start times for {len(start_times)} markets")

                print("Computing closing lines...")
                closing_lines = compute_closing_lines(final_prices_df, trades_df, markets_df, start_times)
                if closing_lines is not None and len(closing_lines) > 0:
                    print(f"  Computed closing lines for {len(closing_lines)} matches")
                else:
                    print("  No closing lines could be computed")
    else:
        print("Skipping API calls (--no-api)")

    # Step 4: Compute metadata
    meta = compute_meta(data, closing_lines)
    for game, gm in meta["games"].items():
        print(f"  {gm['display_name']}: {gm['markets']} markets, "
              f"{gm['trades']} trades, {format_volume(gm['volume'])} volume")
    if meta.get("closing_line_matches", 0) > 0:
        print(f"  Closing lines: {meta['closing_line_matches']} matches, "
              f"favorite win rate: {meta['favorite_win_rate']:.0f}%, "
              f"avg confidence: {meta['avg_closing_confidence']:.0f}%")

    # Step 5: Generate charts
    print("Generating charts...")
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    charts = {}

    # Per-game charts
    for game in ["cod", "cs2"]:
        charts[f"daily_volume_{game}"] = chart_daily_volume(trades_df, game)
        charts[f"top_markets_{game}"] = chart_top_markets(trades_df, game)
        charts[f"trade_size_dist_{game}"] = chart_trade_size_distribution(trades_df, game)
        charts[f"whale_vs_retail_{game}"] = chart_whale_vs_retail(trades_df, game)
        charts[f"buy_sell_{game}"] = chart_buy_sell_imbalance(trades_df, game)
        charts[f"whale_timeline_{game}"] = chart_whale_timeline(trades_df, game)
        charts[f"spread_{game}"] = chart_spread_analysis(orderbook_df, game)

    # Combined charts
    charts["comparison"] = chart_comparison(meta)
    charts["collection_health"] = chart_collection_health(data, meta)

    # CS2 closing lines chart
    charts["closing_lines_cs2"] = chart_closing_lines(closing_lines)

    generated = [k for k, v in charts.items() if v]
    skipped = [k for k, v in charts.items() if v is None]
    print(f"  Generated {len(generated)} charts: {', '.join(generated)}")
    if skipped:
        print(f"  Skipped {len(skipped)} (no data): {', '.join(skipped)}")

    # Step 6: Build PDF
    print("Building PDF...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    build_pdf(charts, data, meta, output_path, db_path, closing_lines=closing_lines)

    print("Done!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Generate a PDF report of Polymarket CDL data analysis."""

import sqlite3
import os
import json
import statistics
from datetime import datetime
from collections import defaultdict, OrderedDict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    Image, PageBreak, HRFlowable, KeepTogether
)

# ── paths ──────────────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "data" / "polymarket_cod.db"
CHART_DIR = Path(__file__).parent / "data" / "charts"
OUTPUT_PDF = Path(__file__).parent / "data" / "CDL_Polymarket_Report.pdf"

CHART_DIR.mkdir(parents=True, exist_ok=True)

# ── global matplotlib style ────────────────────────────────────────
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
PALETTE = ["#00d4aa", "#ff6b6b", "#4ecdc4", "#ffe66d", "#a29bfe", "#fd79a8"]


# ═══════════════════════════════════════════════════════════════════
#  DATA LOADING
# ═══════════════════════════════════════════════════════════════════
def load_data():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # markets
    c.execute("SELECT * FROM markets")
    markets = [dict(r) for r in c.fetchall()]

    # trades
    c.execute("""
        SELECT t.*, m.question FROM trades t
        JOIN markets m ON t.market_id = m.market_id
        ORDER BY t.timestamp
    """)
    trades = [dict(r) for r in c.fetchall()]

    # realtime
    c.execute("SELECT * FROM realtime_prices ORDER BY timestamp")
    realtime = [dict(r) for r in c.fetchall()]

    # token map
    token_map = {}
    for m in markets:
        if m["clob_token_id_yes"]:
            token_map[m["clob_token_id_yes"]] = (m["market_id"], m["question"], "YES")
        if m["clob_token_id_no"]:
            token_map[m["clob_token_id_no"]] = (m["market_id"], m["question"], "NO")

    conn.close()
    return markets, trades, realtime, token_map


# ═══════════════════════════════════════════════════════════════════
#  CHART GENERATORS
# ═══════════════════════════════════════════════════════════════════
def chart_price_timeline(trades):
    """BO5 match price over time with volume bars."""
    bo5 = [t for t in trades if t["market_id"] == "1297885"]
    if not bo5:
        return None

    times = [datetime.fromtimestamp(t["timestamp"]) for t in bo5]
    prices = [t["price"] for t in bo5]
    sizes = [t["size"] for t in bo5]
    side_colors = [ACCENT if t["outcome"] and "Carolina" in t["outcome"] else ACCENT2 for t in bo5]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5.5), gridspec_kw={"height_ratios": [3, 1]}, sharex=True)
    fig.subplots_adjust(hspace=0.05)

    ax1.scatter(times, prices, c=side_colors, s=12, alpha=0.7, zorder=3)
    # rolling avg
    if len(prices) > 10:
        window = min(20, len(prices) // 5)
        rolling = np.convolve(prices, np.ones(window)/window, mode="valid")
        ax1.plot(times[window-1:], rolling, color="#ffffff", linewidth=1.5, alpha=0.8, label="Rolling avg")
    ax1.set_ylabel("Price (Probability)")
    ax1.set_ylim(-0.02, 1.02)
    ax1.set_title("CRR vs Vancouver Surge (BO5) — Price Timeline", fontweight="bold")
    ax1.axhline(0.5, color="#555", linestyle="--", linewidth=0.8)
    ax1.grid(True, alpha=0.3)

    # legend
    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT, markersize=7, label="CRR"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT2, markersize=7, label="Vancouver"),
    ]
    ax1.legend(handles=legend_els, loc="upper right", fontsize=8, facecolor="#1a1a2e", edgecolor="#333")

    ax2.bar(times, sizes, width=0.001, color=side_colors, alpha=0.6)
    ax2.set_ylabel("Trade Size ($)")
    ax2.set_xlabel("Time (Jan 30)")
    ax2.grid(True, alpha=0.3)

    path = CHART_DIR / "price_timeline.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_hourly_volume(trades):
    """Hourly volume and trade count for BO5."""
    bo5 = [t for t in trades if t["market_id"] == "1297885"]
    if not bo5:
        return None

    hourly_vol = defaultdict(float)
    hourly_cnt = defaultdict(int)
    for t in bo5:
        h = datetime.fromtimestamp(t["timestamp"]).strftime("%H:00")
        hourly_vol[h] += t["size"]
        hourly_cnt[h] += 1

    hours = sorted(hourly_vol.keys())
    vols = [hourly_vol[h] for h in hours]
    cnts = [hourly_cnt[h] for h in hours]

    fig, ax1 = plt.subplots(figsize=(9, 4))
    x = np.arange(len(hours))

    bars = ax1.bar(x, vols, color=ACCENT, alpha=0.8, label="Volume ($)")
    ax1.set_xlabel("Hour (UTC)")
    ax1.set_ylabel("Volume ($)", color=ACCENT)
    ax1.set_xticks(x)
    ax1.set_xticklabels(hours, rotation=45, ha="right")
    ax1.grid(True, axis="y", alpha=0.3)
    ax1.set_title("CRR vs Vancouver — Hourly Volume & Trade Count", fontweight="bold")

    ax2 = ax1.twinx()
    ax2.plot(x, cnts, color=ACCENT2, marker="o", markersize=5, linewidth=2, label="Trade count")
    ax2.set_ylabel("Trade Count", color=ACCENT2)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=8,
               facecolor="#1a1a2e", edgecolor="#333")

    path = CHART_DIR / "hourly_volume.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_volume_by_market(trades):
    """Top markets by volume (horizontal bar)."""
    vol_by_market = defaultdict(float)
    for t in trades:
        q = t["question"][:50]
        vol_by_market[q] += t["size"]

    sorted_mkts = sorted(vol_by_market.items(), key=lambda x: x[1], reverse=True)[:12]
    labels = [m[0] for m in sorted_mkts][::-1]
    values = [m[1] for m in sorted_mkts][::-1]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(range(len(labels)), values, color=PALETTE * 3, alpha=0.85)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Volume ($)")
    ax.set_title("Trading Volume by Market", fontweight="bold")
    ax.grid(True, axis="x", alpha=0.3)

    for i, v in enumerate(values):
        ax.text(v + max(values)*0.01, i, f"${v:,.0f}", va="center", fontsize=7, color="#ccc")

    path = CHART_DIR / "volume_by_market.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_trade_size_distribution(trades):
    """Trade size histogram."""
    sizes = [t["size"] for t in trades]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

    # Linear scale
    ax1.hist(sizes, bins=50, color=ACCENT, alpha=0.8, edgecolor="#0e1117")
    ax1.set_xlabel("Trade Size ($)")
    ax1.set_ylabel("Count")
    ax1.set_title("Trade Size Distribution", fontweight="bold")
    ax1.axvline(statistics.median(sizes), color=ACCENT2, linestyle="--", linewidth=1.5, label=f"Median: ${statistics.median(sizes):.0f}")
    ax1.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax1.grid(True, alpha=0.3)

    # Log scale for whale visibility
    log_sizes = [s for s in sizes if s > 0]
    ax2.hist(log_sizes, bins=np.logspace(np.log10(0.1), np.log10(max(log_sizes)), 40),
             color=ACCENT3, alpha=0.8, edgecolor="#0e1117")
    ax2.set_xscale("log")
    ax2.set_xlabel("Trade Size ($) — Log Scale")
    ax2.set_ylabel("Count")
    ax2.set_title("Trade Size Distribution (Log)", fontweight="bold")
    ax2.axvline(100, color=ACCENT2, linestyle="--", linewidth=1.5, label="Whale threshold ($100)")
    ax2.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax2.grid(True, alpha=0.3)

    path = CHART_DIR / "trade_size_dist.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_whale_vs_retail(trades):
    """Pie chart: whale vs retail volume."""
    whale_vol = sum(t["size"] for t in trades if t["size"] >= 100)
    retail_vol = sum(t["size"] for t in trades if t["size"] < 100)
    whale_cnt = sum(1 for t in trades if t["size"] >= 100)
    retail_cnt = sum(1 for t in trades if t["size"] < 100)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))

    # By volume
    wedges1, texts1, autotexts1 = ax1.pie(
        [whale_vol, retail_vol],
        labels=["Whale (>$100)", "Retail (<$100)"],
        colors=[ACCENT2, ACCENT],
        autopct="%1.1f%%", startangle=90,
        textprops={"color": "#eee", "fontsize": 9}
    )
    ax1.set_title("Share of Volume", fontweight="bold")

    # By count
    wedges2, texts2, autotexts2 = ax2.pie(
        [whale_cnt, retail_cnt],
        labels=["Whale (>$100)", "Retail (<$100)"],
        colors=[ACCENT2, ACCENT],
        autopct="%1.1f%%", startangle=90,
        textprops={"color": "#eee", "fontsize": 9}
    )
    ax2.set_title("Share of Trade Count", fontweight="bold")

    path = CHART_DIR / "whale_vs_retail.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_buy_sell_imbalance(trades):
    """Buy vs Sell volume by market."""
    buy_vol = defaultdict(float)
    sell_vol = defaultdict(float)
    for t in trades:
        q = t["question"][:45]
        if t["side"] == "BUY":
            buy_vol[q] += t["size"]
        else:
            sell_vol[q] += t["size"]

    all_mkts = sorted(set(list(buy_vol.keys()) + list(sell_vol.keys())),
                      key=lambda q: buy_vol.get(q, 0) + sell_vol.get(q, 0), reverse=True)[:10]
    all_mkts = all_mkts[::-1]

    fig, ax = plt.subplots(figsize=(9, 5))
    y = np.arange(len(all_mkts))

    buys = [buy_vol.get(m, 0) for m in all_mkts]
    sells = [-sell_vol.get(m, 0) for m in all_mkts]

    ax.barh(y, buys, color=ACCENT, alpha=0.85, label="BUY")
    ax.barh(y, sells, color=ACCENT2, alpha=0.85, label="SELL")
    ax.set_yticks(y)
    ax.set_yticklabels(all_mkts, fontsize=7)
    ax.set_xlabel("Volume ($)")
    ax.axvline(0, color="#555", linewidth=0.8)
    ax.set_title("Buy vs Sell Volume by Market", fontweight="bold")
    ax.legend(fontsize=8, facecolor="#1a1a2e", edgecolor="#333")
    ax.grid(True, axis="x", alpha=0.3)

    path = CHART_DIR / "buy_sell_imbalance.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_spread_analysis(realtime, token_map):
    """Bid-ask spread for markets with meaningful data."""
    # Group by market
    spread_data = defaultdict(list)
    for r in realtime:
        info = token_map.get(r["market_id"])
        if not info:
            continue
        _, name, side = info
        bid = r["bid"] or 0
        ask = r["ask"] or 0
        if bid > 0 and ask > 0 and ask > bid:
            spread = ask - bid
            label = f"{name[:40]}... [{side}]"
            spread_data[label].append(spread)

    if not spread_data:
        return None

    # Filter to those with data and sort
    items = [(k, v) for k, v in spread_data.items() if len(v) >= 3]
    if not items:
        return None
    items.sort(key=lambda x: statistics.mean(x[1]))
    items = items[:15]

    labels = [i[0][:45] for i in items][::-1]
    means = [statistics.mean(i[1]) for i in items][::-1]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(range(len(labels)), means, color=ACCENT4, alpha=0.85)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Average Bid-Ask Spread")
    ax.set_title("Market Liquidity — Average Bid-Ask Spread", fontweight="bold")
    ax.set_xlim(0, 1.05)
    ax.grid(True, axis="x", alpha=0.3)

    for i, v in enumerate(means):
        ax.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=7, color="#ccc")

    path = CHART_DIR / "spread_analysis.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_whale_timeline(trades):
    """Timeline of whale trades colored by outcome."""
    whales = [t for t in trades if t["size"] >= 100]
    if not whales:
        return None

    times = [datetime.fromtimestamp(t["timestamp"]) for t in whales]
    sizes = [t["size"] for t in whales]
    clrs = []
    for t in whales:
        outcome = (t.get("outcome") or "").lower()
        if "carolina" in outcome or "crr" in outcome:
            clrs.append(ACCENT)
        elif "vancouver" in outcome:
            clrs.append(ACCENT2)
        elif "over" in outcome:
            clrs.append(ACCENT3)
        elif "under" in outcome:
            clrs.append(ACCENT4)
        else:
            clrs.append("#a29bfe")

    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.scatter(times, sizes, c=clrs, s=[max(20, s/15) for s in sizes], alpha=0.8, edgecolors="#333", linewidth=0.5)
    ax.set_ylabel("Trade Size ($)")
    ax.set_xlabel("Time")
    ax.set_title("Whale Trades (>$100) — Timeline", fontweight="bold")
    ax.grid(True, alpha=0.3)

    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT, markersize=7, label="CRR / Yes"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT2, markersize=7, label="Vancouver"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT3, markersize=7, label="Over"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor=ACCENT4, markersize=7, label="Under"),
    ]
    ax.legend(handles=legend_els, loc="upper left", fontsize=8, facecolor="#1a1a2e", edgecolor="#333")

    path = CHART_DIR / "whale_timeline.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def chart_liquidity_capacity(trades):
    """Show how much volume was absorbed at each price level for the BO5."""
    bo5 = [t for t in trades if t["market_id"] == "1297885"]
    if not bo5:
        return None

    # bucket by price in 0.05 increments
    buckets = defaultdict(float)
    for t in bo5:
        bucket = round(t["price"] * 20) / 20  # round to nearest 0.05
        buckets[bucket] += t["size"]

    prices = sorted(buckets.keys())
    volumes = [buckets[p] for p in prices]

    fig, ax = plt.subplots(figsize=(9, 4))
    bar_colors = [ACCENT if p >= 0.5 else ACCENT2 for p in prices]
    ax.bar(prices, volumes, width=0.04, color=bar_colors, alpha=0.85, edgecolor="#0e1117")
    ax.set_xlabel("Price Level")
    ax.set_ylabel("Volume Absorbed ($)")
    ax.set_title("BO5 Match — Volume by Price Level (Liquidity Depth)", fontweight="bold")
    ax.set_xlim(-0.05, 1.05)
    ax.grid(True, axis="y", alpha=0.3)

    path = CHART_DIR / "liquidity_depth.png"
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


# ═══════════════════════════════════════════════════════════════════
#  PDF BUILDER
# ═══════════════════════════════════════════════════════════════════
def build_pdf(charts, trades, markets, realtime, token_map):
    doc = SimpleDocTemplate(
        str(OUTPUT_PDF),
        pagesize=letter,
        topMargin=0.6*inch,
        bottomMargin=0.5*inch,
        leftMargin=0.7*inch,
        rightMargin=0.7*inch,
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle("Title2", parent=styles["Title"], fontSize=22, spaceAfter=6,
                              textColor=colors.HexColor("#1a1a2e")))
    styles.add(ParagraphStyle("Sub", parent=styles["Normal"], fontSize=11, textColor=colors.grey,
                              spaceAfter=12))
    styles.add(ParagraphStyle("H1", parent=styles["Heading1"], fontSize=16, spaceBefore=16,
                              spaceAfter=8, textColor=colors.HexColor("#1a1a2e")))
    styles.add(ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, spaceBefore=12,
                              spaceAfter=6, textColor=colors.HexColor("#2d3436")))
    styles.add(ParagraphStyle("Body", parent=styles["Normal"], fontSize=9.5, leading=13,
                              spaceAfter=6))
    styles.add(ParagraphStyle("SmallBody", parent=styles["Normal"], fontSize=8.5, leading=11,
                              spaceAfter=4))
    styles.add(ParagraphStyle("Caption", parent=styles["Normal"], fontSize=8, textColor=colors.grey,
                              alignment=TA_CENTER, spaceAfter=10))
    styles.add(ParagraphStyle("Metric", parent=styles["Normal"], fontSize=10, leading=14,
                              textColor=colors.HexColor("#2d3436")))

    story = []

    # ── Title page ─────────────────────────────────────────────
    story.append(Spacer(1, 1.2*inch))
    story.append(Paragraph("Polymarket CDL Data Report", styles["Title2"]))
    story.append(Paragraph("Call of Duty League — Stage 1 Major Playoffs", styles["Sub"]))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor("#00d4aa")))
    story.append(Spacer(1, 0.3*inch))

    # Summary metrics
    total_vol = sum(t["size"] for t in trades)
    total_trades = len(trades)
    distinct_markets = len(set(t["market_id"] for t in trades))
    whale_vol = sum(t["size"] for t in trades if t["size"] >= 100)
    median_trade = statistics.median([t["size"] for t in trades])
    max_trade = max(t["size"] for t in trades)

    summary_data = [
        ["Metric", "Value"],
        ["Data Period", "Jan 18 – Jan 30, 2026"],
        ["Total Markets Discovered", str(len(markets))],
        ["Markets with Trades", str(distinct_markets)],
        ["Total Trades", f"{total_trades:,}"],
        ["Total Volume", f"${total_vol:,.2f}"],
        ["Median Trade Size", f"${median_trade:,.2f}"],
        ["Largest Trade", f"${max_trade:,.2f}"],
        ["Whale Volume (>$100)", f"${whale_vol:,.2f} ({whale_vol/total_vol*100:.1f}%)"],
        ["Realtime Price Records", f"{len(realtime):,}"],
    ]
    t_summary = Table(summary_data, colWidths=[2.5*inch, 3.5*inch])
    t_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(t_summary)
    story.append(Spacer(1, 0.3*inch))

    story.append(Paragraph(
        "This report analyzes prediction market data collected from Polymarket for "
        "Call of Duty League (CDL) Stage 1 Major Playoff markets. The primary event captured is the "
        "<b>Carolina Royal Ravens vs Vancouver Surge BO5 match</b> on January 30, 2026, plus "
        "season-long futures and tournament winner markets.",
        styles["Body"]
    ))
    story.append(PageBreak())

    # ── Section 1: Market Overview ─────────────────────────────
    story.append(Paragraph("1. Market Overview", styles["H1"]))
    story.append(Paragraph(
        "33 CDL markets were discovered across three categories: live match markets (BO5 + individual maps), "
        "OpTic Texas Major winner markets, and CDL Regular Season Top 4 futures. The CRR vs Vancouver "
        "match alone accounts for <b>96.3%</b> of all trading volume.",
        styles["Body"]
    ))

    if charts.get("volume_by_market"):
        story.append(Image(charts["volume_by_market"], width=6.5*inch, height=3.6*inch))
        story.append(Paragraph("Trading volume by market — dominated by the live BO5 match.", styles["Caption"]))

    # Volume table by category
    bo5_vol = sum(t["size"] for t in trades if t["market_id"] in
                  ["1297885","1297886","1297888","1297890","1297893","1297895","1297897","1297898"])
    major_vol = sum(t["size"] for t in trades if "Major" in t.get("question","") and "win" in t.get("question","").lower())
    season_vol = sum(t["size"] for t in trades if "top 4" in t.get("question","").lower())

    cat_data = [
        ["Category", "Markets", "Volume", "Share"],
        ["CRR vs Vancouver (match + maps)", "8", f"${bo5_vol:,.0f}", f"{bo5_vol/total_vol*100:.1f}%"],
        ["OpTic Texas Major Winner", "13", f"${major_vol:,.0f}", f"{major_vol/total_vol*100:.1f}%"],
        ["CDL Regular Season Top 4", "12", f"${season_vol:,.0f}", f"{season_vol/total_vol*100:.1f}%"],
    ]
    t_cat = Table(cat_data, colWidths=[2.8*inch, 0.9*inch, 1.3*inch, 0.9*inch])
    t_cat.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(Spacer(1, 0.1*inch))
    story.append(t_cat)

    story.append(PageBreak())

    # ── Section 2: Price Timeline ──────────────────────────────
    story.append(Paragraph("2. CRR vs Vancouver Surge — Price Action", styles["H1"]))
    story.append(Paragraph(
        "The BO5 match saw dramatic price swings throughout the day. CRR opened as a heavy favorite at ~80% "
        "probability, but momentum shifted as the match progressed. Vancouver ultimately won, collapsing the CRR "
        "price to near-zero by settlement.",
        styles["Body"]
    ))

    if charts.get("price_timeline"):
        story.append(Image(charts["price_timeline"], width=6.5*inch, height=3.6*inch))
        story.append(Paragraph("Price (top) and trade size (bottom) for the BO5 match. Green = CRR trades, Red = Vancouver trades.", styles["Caption"]))

    # Hourly breakdown table
    bo5_trades = [t for t in trades if t["market_id"] == "1297885"]
    hourly = defaultdict(lambda: {"vol": 0, "cnt": 0, "prices": [], "buys": 0, "sells": 0})
    for t in bo5_trades:
        h = datetime.fromtimestamp(t["timestamp"]).strftime("%H:00")
        hourly[h]["vol"] += t["size"]
        hourly[h]["cnt"] += 1
        hourly[h]["prices"].append(t["price"])
        if t["side"] == "BUY":
            hourly[h]["buys"] += 1
        else:
            hourly[h]["sells"] += 1

    hourly_data = [["Hour", "Trades", "Volume", "Price Range", "Avg", "Buy/Sell"]]
    for h in sorted(hourly.keys()):
        d = hourly[h]
        p = d["prices"]
        hourly_data.append([
            h, str(d["cnt"]), f"${d['vol']:,.0f}",
            f"{min(p):.3f}–{max(p):.3f}", f"{statistics.mean(p):.3f}",
            f"{d['buys']}/{d['sells']}"
        ])

    t_hourly = Table(hourly_data, colWidths=[0.7*inch, 0.7*inch, 1.1*inch, 1.3*inch, 0.7*inch, 0.8*inch])
    t_hourly.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(Spacer(1, 0.15*inch))
    story.append(Paragraph("Hourly Breakdown", styles["H2"]))
    story.append(t_hourly)

    if charts.get("hourly_volume"):
        story.append(Spacer(1, 0.1*inch))
        story.append(Image(charts["hourly_volume"], width=6*inch, height=2.7*inch))
        story.append(Paragraph("Hourly volume peaks during the live match (13:00–14:00 UTC).", styles["Caption"]))

    story.append(PageBreak())

    # ── Section 3: Outlier Analysis ────────────────────────────
    story.append(Paragraph("3. Outlier Analysis", styles["H1"]))

    # Price outliers
    story.append(Paragraph("3.1 Extreme Price Trades", styles["H2"]))
    extreme = [t for t in trades if t["price"] < 0.05 or t["price"] > 0.95]
    story.append(Paragraph(
        f"<b>{len(extreme)} trades ({len(extreme)/len(trades)*100:.0f}%)</b> executed at extreme prices "
        f"(&lt;0.05 or &gt;0.95). These are primarily settlement trades — positions closing as game outcomes became "
        f"certain (0.999 buys) or distressed exits on the losing side (0.001-0.002).",
        styles["Body"]
    ))

    # Top extreme trades table
    extreme_sorted = sorted(extreme, key=lambda t: t["size"], reverse=True)[:10]
    ext_data = [["Time", "Price", "Size", "Side", "Outcome", "Market"]]
    for t in extreme_sorted:
        dt = datetime.fromtimestamp(t["timestamp"]).strftime("%m/%d %H:%M")
        ext_data.append([
            dt, f"{t['price']:.3f}", f"${t['size']:,.2f}",
            t["side"], (t["outcome"] or "")[:18], t["question"][:30]
        ])
    t_ext = Table(ext_data, colWidths=[0.8*inch, 0.6*inch, 0.9*inch, 0.5*inch, 1.3*inch, 2.0*inch])
    t_ext.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#c0392b")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fdf2f2")]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t_ext)

    # Volatility outliers
    story.append(Spacer(1, 0.15*inch))
    story.append(Paragraph("3.2 Volatility Outliers", styles["H2"]))
    story.append(Paragraph(
        "The BO5 match exhibited the widest price swing (0.998 — from 0.001 to 0.999). "
        "Standard deviation peaked at <b>0.499</b> during the 15:00 hour (settlement) and <b>0.320</b> at 09:00 "
        "(pre-match uncertainty). During the most active trading hour (14:00), the std was 0.222 with 163 trades.",
        styles["Body"]
    ))

    # Volume whale detection
    story.append(Paragraph("3.3 The $9,250 Vancouver Whale", styles["H2"]))
    story.append(Paragraph(
        "Between 10:51 and 10:57, four back-to-back trades totaling <b>$9,250</b> bought Vancouver Surge at "
        "$0.23 — a massive contrarian bet against the CRR favorite. At this price, a Vancouver win would "
        "return approximately <b>$40,217</b> (4.3x). Vancouver did win, making this the single most profitable "
        "sequence in the dataset.",
        styles["Body"]
    ))

    whale_data = [["Time", "Size", "Price", "Outcome"],
                  ["10:51", "$2,550", "0.230", "Vancouver Surge"],
                  ["10:53", "$1,800", "0.230", "Vancouver Surge"],
                  ["10:56", "$2,550", "0.230", "Vancouver Surge"],
                  ["10:57", "$2,350", "0.230", "Vancouver Surge"],
                  ["", "$9,250", "", "~$40,217 payout (4.3x)"]]
    t_whale = Table(whale_data, colWidths=[0.8*inch, 1*inch, 0.8*inch, 2.5*inch])
    t_whale.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8f5e9")),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#f8f9fa")]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(t_whale)

    story.append(PageBreak())

    # ── Section 4: Whale Analysis ──────────────────────────────
    story.append(Paragraph("4. Whale Activity", styles["H1"]))

    whale_trades = [t for t in trades if t["size"] >= 100]
    story.append(Paragraph(
        f"<b>{len(whale_trades)} trades</b> exceeded $100, accounting for <b>${whale_vol:,.0f} "
        f"({whale_vol/total_vol*100:.1f}%)</b> of total volume. The market is dominated by a small "
        f"number of large participants.",
        styles["Body"]
    ))

    if charts.get("whale_vs_retail"):
        story.append(Image(charts["whale_vs_retail"], width=6*inch, height=2.7*inch))
        story.append(Paragraph("Whales are 10.8% of trades but 80.8% of volume.", styles["Caption"]))

    if charts.get("whale_timeline"):
        story.append(Image(charts["whale_timeline"], width=6.5*inch, height=3*inch))
        story.append(Paragraph("Timeline of whale trades (>$100). Bubble size proportional to trade value.", styles["Caption"]))

    if charts.get("trade_size_dist"):
        story.append(Image(charts["trade_size_dist"], width=6.5*inch, height=2.7*inch))
        story.append(Paragraph("Trade size distribution: most trades are small, but volume is concentrated in the tail.", styles["Caption"]))

    story.append(PageBreak())

    # ── Section 5: Buy/Sell Imbalance ──────────────────────────
    story.append(Paragraph("5. Buy/Sell Imbalance", styles["H1"]))

    total_buy = sum(t["size"] for t in trades if t["side"] == "BUY")
    total_sell = sum(t["size"] for t in trades if t["side"] == "SELL")
    story.append(Paragraph(
        f"Across all markets, buying dominates overwhelmingly: <b>${total_buy:,.0f} (96.4%)</b> in buys vs "
        f"<b>${total_sell:,.0f} (3.6%)</b> in sells. This extreme buy-side skew means there are very few "
        f"sellers providing resting liquidity. Most selling happens at extreme prices during market resolution.",
        styles["Body"]
    ))

    if charts.get("buy_sell"):
        story.append(Image(charts["buy_sell"], width=6.5*inch, height=3.5*inch))
        story.append(Paragraph("Buy (green, right) vs Sell (red, left) volume by market.", styles["Caption"]))

    # ── Section 6: Liquidity ───────────────────────────────────
    story.append(Paragraph("6. Liquidity Analysis", styles["H1"]))
    story.append(Paragraph(
        "Real-time WebSocket data (906 records over ~4 hours on Jan 30) reveals <b>extremely thin orderbooks</b> "
        "across all CDL markets.",
        styles["Body"]
    ))

    story.append(Paragraph("6.1 Bid-Ask Spreads", styles["H2"]))
    story.append(Paragraph(
        "All markets show bid-ask spreads of <b>0.98 or wider</b>. The match sub-markets (games, handicaps, "
        "totals) show near-maximum spreads of 0.998 — bids at 0.001 and asks at 0.999. "
        "Season-long futures have consistent bid=0.01, ask=0.99 with zero variation. "
        "This means <b>there are no market makers</b> providing continuous liquidity.",
        styles["Body"]
    ))

    if charts.get("spread"):
        story.append(Image(charts["spread"], width=6.5*inch, height=3.3*inch))
        story.append(Paragraph("Average bid-ask spread — lower is better. All markets show extreme illiquidity.", styles["Caption"]))

    story.append(Paragraph("6.2 Volume by Price Level", styles["H2"]))
    story.append(Paragraph(
        "The chart below shows how much volume was absorbed at each price level for the BO5 match. "
        "Liquidity is concentrated at two extremes: ~0.80 (CRR favored) and ~0.23 (Vancouver upset). "
        "The 0.999 bucket reflects settlement volume, not genuine price discovery.",
        styles["Body"]
    ))

    if charts.get("liquidity_depth"):
        story.append(Image(charts["liquidity_depth"], width=6*inch, height=2.7*inch))
        story.append(Paragraph("Volume absorbed at each price level — bimodal distribution around the two sides.", styles["Caption"]))

    story.append(Paragraph("6.3 Capacity for Large Bets", styles["H2"]))

    cap_data = [
        ["Bet Size", "Feasibility", "Expected Slippage"],
        ["$1–$50", "Possible", "Low during live matches"],
        ["$50–$500", "Feasible during matches", "Moderate — need existing orders"],
        ["$500–$2,500", "Difficult", "High — will move the market"],
        ["$2,500+", "Contrarian timing only", "Extreme — limited counterparties"],
    ]
    t_cap = Table(cap_data, colWidths=[1.2*inch, 2*inch, 2.8*inch])
    t_cap.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#ddd")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(t_cap)

    story.append(Spacer(1, 0.15*inch))
    story.append(Paragraph(
        "<b>Key takeaways:</b> CDL Polymarket markets are extremely illiquid. The entire dataset's volume "
        "($43.8K) is smaller than a single trade on major Polymarket political markets. Placing bets over "
        "~$500 requires significant patience, accepting poor fills, or timing entry to live match moments "
        "when counterparty flow peaks. Season-long futures markets are effectively dead with no resting liquidity.",
        styles["Body"]
    ))

    # ── Footer ─────────────────────────────────────────────────
    story.append(Spacer(1, 0.4*inch))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#ccc")))
    story.append(Paragraph(
        f"Report generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from data/polymarket_cod.db",
        styles["Caption"]
    ))

    doc.build(story)
    print(f"PDF saved to: {OUTPUT_PDF}")


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("Loading data...")
    markets, trades, realtime, token_map = load_data()

    print(f"  {len(markets)} markets, {len(trades)} trades, {len(realtime)} realtime records")

    print("Generating charts...")
    charts = {}
    charts["price_timeline"] = chart_price_timeline(trades)
    charts["hourly_volume"] = chart_hourly_volume(trades)
    charts["volume_by_market"] = chart_volume_by_market(trades)
    charts["trade_size_dist"] = chart_trade_size_distribution(trades)
    charts["whale_vs_retail"] = chart_whale_vs_retail(trades)
    charts["buy_sell"] = chart_buy_sell_imbalance(trades)
    charts["spread"] = chart_spread_analysis(realtime, token_map)
    charts["whale_timeline"] = chart_whale_timeline(trades)
    charts["liquidity_depth"] = chart_liquidity_capacity(trades)

    generated = [k for k, v in charts.items() if v]
    print(f"  Generated {len(generated)} charts: {', '.join(generated)}")

    print("Building PDF...")
    build_pdf(charts, trades, markets, realtime, token_map)
    print("Done!")


if __name__ == "__main__":
    main()

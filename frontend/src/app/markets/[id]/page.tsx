"use client";

import { use } from "react";
import Link from "next/link";
import useSWR from "swr";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";
import { buildUrl, fetcher } from "@/lib/api";
import { PALETTE, CHART_THEME } from "@/lib/colors";
import { formatVolume, formatTimestamp } from "@/lib/format";
import LoadingSkeleton from "@/components/LoadingSkeleton";
import ChartCard from "@/components/ChartCard";
import StatCard from "@/components/StatCard";
import GameBadge from "@/components/GameBadge";

interface Trade {
  trade_id: string;
  timestamp: number;
  price: number;
  size: number;
  side: string;
  outcome: string;
}

interface MarketDetail {
  market: {
    market_id: string;
    question: string;
    game: string;
    outcomes: string;
    start_date: string;
    end_date: string;
  };
  trades: Trade[];
  price_timeline: { timestamp: number; price: number; outcome: string }[];
  orderbook: {
    token_id: string;
    best_bid_price: number;
    best_bid_size: number;
    best_ask_price: number;
    best_ask_size: number;
    spread: number;
    mid_price: number;
  }[];
  stats: {
    trade_count: number;
    total_volume: number;
    avg_trade_size: number;
    max_trade_size: number;
  };
}

export default function MarketDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const url = buildUrl(`markets/${id}`, {});
  const { data, isLoading, error } = useSWR<MarketDetail>(url, fetcher);

  if (isLoading)
    return (
      <div className="space-y-4">
        <LoadingSkeleton height="h-12" />
        <LoadingSkeleton height="h-80" />
      </div>
    );

  if (error || !data)
    return (
      <div className="flex items-center justify-center h-64 text-[var(--accent-red)]">
        Failed to load market details
      </div>
    );

  const { market, trades, price_timeline, orderbook, stats } = data;

  // Group price timeline by outcome
  const outcomes: Record<string, { timestamp: number; price: number }[]> = {};
  for (const pt of price_timeline) {
    const key = pt.outcome || "Unknown";
    if (!outcomes[key]) outcomes[key] = [];
    outcomes[key].push(pt);
  }
  const outcomeNames = Object.keys(outcomes);

  // Merge timelines for chart
  const timelineMap: Record<number, Record<string, number>> = {};
  for (const [outcome, points] of Object.entries(outcomes)) {
    for (const pt of points) {
      if (!timelineMap[pt.timestamp]) timelineMap[pt.timestamp] = {};
      timelineMap[pt.timestamp][outcome] = pt.price;
    }
  }
  const chartData = Object.entries(timelineMap)
    .map(([ts, prices]) => ({ timestamp: Number(ts), ...prices }))
    .sort((a, b) => a.timestamp - b.timestamp);

  return (
    <div className="space-y-6">
      {/* Breadcrumb + Header */}
      <div>
        <Link
          href="/markets"
          className="inline-flex items-center gap-1 text-[var(--text-muted)] text-sm transition-colors duration-150 hover:text-[var(--foreground)]"
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <path d="M10 12L6 8l4-4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Back to Markets
        </Link>
        <h1 className="text-3xl font-extrabold mt-2 tracking-tight">{market.question}</h1>
        <div className="mt-2">
          <GameBadge game={market.game} size="md" />
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Trades"
          value={stats.trade_count.toLocaleString()}
        />
        <StatCard
          label="Volume"
          value={formatVolume(stats.total_volume)}
          color="var(--accent-green)"
        />
        <StatCard
          label="Avg Trade"
          value={`$${stats.avg_trade_size?.toFixed(2) || "0"}`}
          color="var(--accent-teal)"
        />
        <StatCard
          label="Max Trade"
          value={`$${stats.max_trade_size?.toFixed(2) || "0"}`}
          color="var(--accent-yellow)"
        />
      </div>

      {/* Orderbook */}
      {orderbook.length > 0 && (
        <div
          className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden inner-glow"
          style={{ boxShadow: "var(--shadow-md)" }}
        >
          <div className="px-4 pt-4 pb-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
            <h3 className="text-base font-bold tracking-tight">Latest Orderbook</h3>
          </div>
          <div className="p-4">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              {orderbook.map((ob, idx) => (
                <div
                  key={idx}
                  className="bg-[var(--background)]/50 rounded-lg p-3 space-y-1.5 border border-[var(--border-subtle)]"
                >
                  <p className="text-[11px] text-[var(--text-muted)] font-medium uppercase tracking-wider">
                    Token {idx + 1}
                  </p>
                  <p className="text-sm">
                    Bid:{" "}
                    <span className="text-[var(--accent-green)] font-mono font-medium">
                      {ob.best_bid_price?.toFixed(3) || "\u2014"}
                    </span>
                  </p>
                  <p className="text-sm">
                    Ask:{" "}
                    <span className="text-[var(--accent-red)] font-mono font-medium">
                      {ob.best_ask_price?.toFixed(3) || "\u2014"}
                    </span>
                  </p>
                  <p className="text-sm">
                    Spread:{" "}
                    <span className="font-mono text-[var(--text-muted)]">
                      {ob.spread?.toFixed(4) || "\u2014"}
                    </span>
                  </p>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Price Timeline */}
      {chartData.length > 0 && (
        <ChartCard title="Price Timeline" height="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={chartData}
              margin={{ top: 5, right: 5, bottom: 5, left: 5 }}
            >
              <CartesianGrid {...CHART_THEME.grid} />
              <XAxis
                dataKey="timestamp"
                tick={CHART_THEME.axis.tickSmall}
                tickFormatter={(ts: number) => formatTimestamp(ts)}
              />
              <YAxis
                tick={CHART_THEME.axis.tickSmall}
                domain={[0, 1]}
                tickFormatter={(v: number) => Number(v).toFixed(2)}
              />
              <Tooltip
                {...CHART_THEME.tooltip}
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                labelFormatter={(ts: any) => formatTimestamp(ts)}
              />
              <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
              {outcomeNames.map((name, idx) => (
                <Line
                  key={name}
                  type="monotone"
                  dataKey={name}
                  stroke={PALETTE[idx % PALETTE.length]}
                  dot={CHART_THEME.line.dot}
                  strokeWidth={CHART_THEME.line.strokeWidth}
                  activeDot={CHART_THEME.line.activeDot}
                  animationDuration={CHART_THEME.animationDuration}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Recent Trades Table */}
      <div
        className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden"
        style={{ boxShadow: "var(--shadow-md)" }}
      >
        <div className="px-4 pt-4 pb-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
          <h3 className="text-base font-bold tracking-tight">
            Recent Trades ({trades.length})
          </h3>
        </div>
        <div className="overflow-x-auto max-h-96 overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-[var(--surface-elevated)]/50 border-b-2 border-[var(--border)]">
              <tr>
                <th className="text-left px-4 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Time</th>
                <th className="text-left px-3 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Outcome</th>
                <th className="text-right px-3 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Price</th>
                <th className="text-right px-3 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Size</th>
                <th className="text-right px-4 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Side</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t) => (
                <tr
                  key={t.trade_id}
                  className="border-t border-[var(--border-subtle)] table-row-hover"
                >
                  <td className="px-4 py-2.5 text-xs text-[var(--text-muted)]">
                    {formatTimestamp(t.timestamp)}
                  </td>
                  <td className="px-3 py-2.5 text-xs">{t.outcome}</td>
                  <td className="px-3 py-2.5 text-right font-mono">
                    {t.price.toFixed(3)}
                  </td>
                  <td className="px-3 py-2.5 text-right font-mono">
                    ${t.size.toFixed(2)}
                  </td>
                  <td
                    className={`px-4 py-2.5 text-right text-xs font-semibold ${
                      t.side === "BUY"
                        ? "text-[var(--accent-green)]"
                        : "text-[var(--accent-red)]"
                    }`}
                  >
                    {t.side}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

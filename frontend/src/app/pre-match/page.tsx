"use client";

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
import { useApiData } from "@/lib/hooks";
import { PALETTE, CHART_THEME } from "@/lib/colors";
import { formatTimestamp } from "@/lib/format";
import LoadingSkeleton from "@/components/LoadingSkeleton";
import GameBadge from "@/components/GameBadge";

interface PreMatchMarket {
  market_id: string;
  question: string;
  game: string;
  game_start_time: string;
  current_prices: Record<string, number>;
  favored: string;
  trade_count: number;
  timeline: Record<string, { timestamp: number; price: number }[]>;
}

interface PreMatchResponse {
  markets: PreMatchMarket[];
  note?: string;
}

export default function PreMatchPage() {
  const { data, isLoading, error } = useApiData<PreMatchResponse>("pre-match-movement");

  if (isLoading)
    return (
      <div className="space-y-4">
        <LoadingSkeleton height="h-12" />
        <LoadingSkeleton height="h-64" />
        <LoadingSkeleton height="h-64" />
      </div>
    );

  const markets = data?.markets || [];

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">Pre-Match Movement</h1>
        <p className="text-base text-[var(--text-muted)] mt-1">
          Active markets with upcoming matches. Price timelines derived from trade data.
        </p>
      </div>

      {data?.note && (
        <div
          className="text-[var(--accent-yellow)] text-sm bg-[var(--surface)] rounded-xl p-4 border border-[var(--accent-yellow)]/20"
          style={{ boxShadow: "var(--shadow-sm)" }}
        >
          {data.note}
        </div>
      )}

      {error && (
        <p className="text-[var(--accent-red)]">Failed to load pre-match data.</p>
      )}

      {markets.length === 0 && !error && !data?.note && (
        <p className="text-[var(--text-muted)]">No upcoming matches found.</p>
      )}

      <div className="space-y-5">
        {markets.map((market) => {
          const outcomeNames = Object.keys(market.timeline);

          // Merge timelines
          const timelineMap: Record<number, Record<string, number>> = {};
          for (const [outcome, points] of Object.entries(market.timeline)) {
            for (const pt of points) {
              if (!timelineMap[pt.timestamp]) timelineMap[pt.timestamp] = {};
              timelineMap[pt.timestamp][outcome] = pt.price;
            }
          }
          const chartData = Object.entries(timelineMap)
            .map(([ts, prices]) => ({ timestamp: Number(ts), ...prices }))
            .sort((a, b) => a.timestamp - b.timestamp);

          return (
            <div
              key={market.market_id}
              className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden card-hover inner-glow"
              style={{ boxShadow: "var(--shadow-md)" }}
            >
              <div className="px-4 pt-4 pb-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <h3 className="text-base font-bold tracking-tight">{market.question}</h3>
                    <p className="text-xs text-[var(--text-muted)] mt-1.5">
                      Start: {market.game_start_time} &middot;{" "}
                      {market.trade_count} trades
                    </p>
                  </div>
                  <GameBadge game={market.game} size="md" />
                </div>
              </div>

              <div className="p-4">
                {/* Current prices */}
                <div className="flex gap-3 mb-4">
                  {Object.entries(market.current_prices).map(([team, price]) => (
                    <div
                      key={team}
                      className={`px-3 py-2 rounded-lg text-sm font-mono transition-colors duration-150 ${
                        team === market.favored
                          ? "bg-[var(--accent-green)]/10 text-[var(--accent-green)] font-bold border border-[var(--accent-green)]/20"
                          : "bg-[var(--background)] text-[var(--text-muted)] border border-[var(--border-subtle)]"
                      }`}
                    >
                      {team}: {(price * 100).toFixed(1)}%
                    </div>
                  ))}
                </div>

                {/* Chart */}
                {chartData.length > 1 && (
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={chartData}>
                        <CartesianGrid {...CHART_THEME.grid} />
                        <XAxis
                          dataKey="timestamp"
                          tick={CHART_THEME.axis.tickSmall}
                          tickFormatter={(ts: number) => formatTimestamp(ts)}
                        />
                        <YAxis
                          tick={CHART_THEME.axis.tickSmall}
                          domain={[0, 1]}
                          tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`}
                        />
                        <Tooltip
                          {...CHART_THEME.tooltip}
                          // eslint-disable-next-line @typescript-eslint/no-explicit-any
                          labelFormatter={(ts: any) => formatTimestamp(ts)}
                          // eslint-disable-next-line @typescript-eslint/no-explicit-any
                          formatter={(value: any) => [
                            `${(value * 100).toFixed(1)}%`,
                            "",
                          ]}
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
                            connectNulls
                            animationDuration={CHART_THEME.animationDuration}
                          />
                        ))}
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

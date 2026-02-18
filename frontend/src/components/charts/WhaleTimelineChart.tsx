"use client";

import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ZAxis,
  Legend,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { PALETTE, CHART_THEME } from "@/lib/colors";
import { formatVolume, formatTimestamp } from "@/lib/format";
import ChartCard from "../ChartCard";

interface Trade {
  timestamp: number;
  price: number;
  size: number;
  side: string;
  outcome: string;
  question: string;
  game: string;
}

export default function WhaleTimelineChart() {
  const { data, error, isLoading } = useApiData<{ data: Trade[]; threshold: number }>("whale-timeline");

  // Group by outcome, take top 6
  const byOutcome: Record<string, Trade[]> = {};
  for (const t of data?.data || []) {
    const key = t.outcome || "Unknown";
    if (!byOutcome[key]) byOutcome[key] = [];
    byOutcome[key].push(t);
  }

  const sortedOutcomes = Object.entries(byOutcome)
    .sort((a, b) => {
      const aVol = a[1].reduce((s, t) => s + t.size, 0);
      const bVol = b[1].reduce((s, t) => s + t.size, 0);
      return bVol - aVol;
    })
    .slice(0, 6);

  return (
    <ChartCard title={`Whale Trades (>=$${data?.threshold || 1000}) Timeline`} isLoading={isLoading} error={error}>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
          <CartesianGrid {...CHART_THEME.grid} />
          <XAxis
            dataKey="timestamp"
            type="number"
            tick={CHART_THEME.axis.tickSmall}
            tickFormatter={(ts: number) => formatTimestamp(ts)}
            domain={["dataMin", "dataMax"]}
          />
          <YAxis
            dataKey="size"
            tick={CHART_THEME.axis.tickSmall}
            tickFormatter={(v: number) => formatVolume(v)}
          />
          <ZAxis dataKey="size" range={[30, 300]} />
          <Tooltip
            {...CHART_THEME.tooltip}
            formatter={(value: any, name: any) => {
              if (name === "size") return [formatVolume(value), "Size"];
              if (name === "timestamp") return [formatTimestamp(value), "Time"];
              return [value, name];
            }}
          />
          <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
          {sortedOutcomes.map(([outcome, trades], idx) => (
            <Scatter
              key={outcome}
              name={outcome.length > 20 ? outcome.slice(0, 20) + "..." : outcome}
              data={trades}
              fill={PALETTE[idx % PALETTE.length]}
              opacity={0.75}
              animationDuration={CHART_THEME.animationDuration}
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

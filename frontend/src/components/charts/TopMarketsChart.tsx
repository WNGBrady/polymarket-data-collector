"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Cell,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { PALETTE, CHART_THEME } from "@/lib/colors";
import { formatVolume } from "@/lib/format";
import ChartCard from "../ChartCard";

interface MarketRow {
  market_id: string;
  question: string;
  game: string;
  volume: number;
  trade_count: number;
}

export default function TopMarketsChart() {
  const { data, error, isLoading } = useApiData<{ data: MarketRow[] }>("top-markets", { limit: 10 });

  const chartData = (data?.data || []).map((r) => ({
    ...r,
    short: r.question.length > 45 ? r.question.slice(0, 45) + "..." : r.question,
  })).reverse();

  return (
    <ChartCard title="Top Markets by Volume" isLoading={isLoading} error={error} height="h-96">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 60, bottom: 5, left: 5 }}>
          <CartesianGrid {...CHART_THEME.grid} horizontal={false} />
          <XAxis
            type="number"
            tick={CHART_THEME.axis.tickSmall}
            tickFormatter={(v: number) => formatVolume(v)}
          />
          <YAxis
            type="category"
            dataKey="short"
            width={200}
            tick={CHART_THEME.axis.tickSmall}
          />
          <Tooltip
            {...CHART_THEME.tooltip}
            formatter={(value: any) => [formatVolume(value), "Volume"]}
            labelFormatter={(label: any) => label}
          />
          <Bar
            dataKey="volume"
            name="Volume"
            radius={CHART_THEME.bar.radiusHorizontal}
            animationDuration={CHART_THEME.animationDuration}
          >
            {chartData.map((_, idx) => (
              <Cell key={idx} fill={PALETTE[idx % PALETTE.length]} opacity={CHART_THEME.bar.opacity} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

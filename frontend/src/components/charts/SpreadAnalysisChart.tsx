"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { COLORS, CHART_THEME } from "@/lib/colors";
import ChartCard from "../ChartCard";

interface Row {
  market_id: string;
  question: string;
  game: string;
  avg_spread: number;
  min_spread: number;
  max_spread: number;
  snapshot_count: number;
}

export default function SpreadAnalysisChart() {
  const { data, error, isLoading } = useApiData<{ data: Row[] }>("spread-analysis", { limit: 15 });

  const chartData = (data?.data || []).map((r) => ({
    name: r.question.length > 40 ? r.question.slice(0, 40) + "..." : r.question,
    avg_spread: r.avg_spread,
  }));

  return (
    <ChartCard title="Tightest Orderbook Spreads" isLoading={isLoading} error={error} height="h-96">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 40, bottom: 5, left: 5 }}>
          <CartesianGrid {...CHART_THEME.grid} horizontal={false} />
          <XAxis
            type="number"
            tick={CHART_THEME.axis.tickSmall}
            tickFormatter={(v: number) => v.toFixed(3)}
          />
          <YAxis
            type="category"
            dataKey="name"
            width={200}
            tick={{ ...CHART_THEME.axis.tickSmall, fontSize: 8 }}
          />
          <Tooltip
            {...CHART_THEME.tooltip}
            formatter={(value: any) => [value.toFixed(4), "Avg Spread"]}
          />
          <Bar
            dataKey="avg_spread"
            name="Avg Spread"
            fill={COLORS.yellow}
            opacity={CHART_THEME.bar.opacity}
            radius={CHART_THEME.bar.radiusHorizontal}
            animationDuration={CHART_THEME.animationDuration}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
  ReferenceLine,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { COLORS, CHART_THEME } from "@/lib/colors";
import { formatVolume } from "@/lib/format";
import ChartCard from "../ChartCard";

interface Row {
  market_id: string;
  question: string;
  game: string;
  buy_volume: number;
  sell_volume: number;
  total_volume: number;
}

export default function BuySellChart() {
  const { data, error, isLoading } = useApiData<{ data: Row[] }>("buy-sell-imbalance", { limit: 10 });

  const chartData = (data?.data || [])
    .map((r) => ({
      name: r.question.length > 40 ? r.question.slice(0, 40) + "..." : r.question,
      buy: r.buy_volume,
      sell: -r.sell_volume,
    }))
    .reverse();

  return (
    <ChartCard title="Buy vs Sell Imbalance (Top Markets)" isLoading={isLoading} error={error} height="h-96">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 30, bottom: 5, left: 5 }}>
          <CartesianGrid {...CHART_THEME.grid} horizontal={false} />
          <XAxis
            type="number"
            tick={CHART_THEME.axis.tickSmall}
            tickFormatter={(v: number) => formatVolume(Math.abs(v))}
          />
          <YAxis
            type="category"
            dataKey="name"
            width={190}
            tick={{ ...CHART_THEME.axis.tickSmall, fontSize: 8 }}
          />
          <Tooltip
            {...CHART_THEME.tooltip}
            formatter={(value: any) => [formatVolume(Math.abs(value)), ""]}
          />
          <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
          <ReferenceLine x={0} stroke="#2e2e5a" />
          <Bar
            dataKey="buy"
            name="Buy"
            fill={COLORS.green}
            opacity={CHART_THEME.bar.opacity}
            radius={CHART_THEME.bar.radiusHorizontal}
            animationDuration={CHART_THEME.animationDuration}
          />
          <Bar
            dataKey="sell"
            name="Sell"
            fill={COLORS.red}
            opacity={CHART_THEME.bar.opacity}
            radius={[3, 0, 0, 3]}
            animationDuration={CHART_THEME.animationDuration}
          />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { GAME_COLORS, CHART_THEME } from "@/lib/colors";
import { formatVolume } from "@/lib/format";
import ChartCard from "../ChartCard";
import { useFilters } from "@/context/FilterContext";

interface DailyRow {
  date: string;
  game: string;
  volume: number;
  trade_count: number;
}

export default function DailyVolumeChart() {
  const { game } = useFilters();
  const { data, error, isLoading } = useApiData<{ data: DailyRow[] }>("daily-volume");

  // Pivot: group by date, columns = game
  const pivoted: Record<string, Record<string, number>> = {};
  if (data?.data) {
    for (const row of data.data) {
      if (!pivoted[row.date]) pivoted[row.date] = { date: row.date as unknown as number };
      (pivoted[row.date] as Record<string, unknown>)[`${row.game}_volume`] = row.volume;
      (pivoted[row.date] as Record<string, unknown>)[`${row.game}_count`] = row.trade_count;
    }
  }
  const chartData = Object.values(pivoted);

  const showCod = game === "all" || game === "cod";
  const showCs2 = game === "all" || game === "cs2";

  return (
    <ChartCard title="Daily Trading Volume" isLoading={isLoading} error={error}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
          <CartesianGrid {...CHART_THEME.grid} />
          <XAxis
            dataKey="date"
            tick={CHART_THEME.axis.tick}
            tickFormatter={(d: string) => d.slice(5)}
          />
          <YAxis
            tick={CHART_THEME.axis.tick}
            tickFormatter={(v: number) => formatVolume(v)}
          />
          <Tooltip
            {...CHART_THEME.tooltip}
            formatter={(value: any) => [formatVolume(value), ""]}
          />
          <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
          {showCod && (
            <Bar
              dataKey="cod_volume"
              name="COD Volume"
              fill={GAME_COLORS.cod}
              opacity={CHART_THEME.bar.opacity}
              stackId="vol"
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
          )}
          {showCs2 && (
            <Bar
              dataKey="cs2_volume"
              name="CS2 Volume"
              fill={GAME_COLORS.cs2}
              opacity={CHART_THEME.bar.opacity}
              stackId="vol"
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
          )}
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
}

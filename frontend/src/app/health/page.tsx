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
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { GAME_COLORS, CHART_THEME } from "@/lib/colors";
import { formatNumber } from "@/lib/format";
import ChartCard from "@/components/ChartCard";
import LoadingSkeleton from "@/components/LoadingSkeleton";

interface DailyRow {
  date: string;
  game: string;
  count: number;
}

interface HealthResponse {
  daily_trades: DailyRow[];
  daily_orderbook: DailyRow[];
  table_totals: Record<string, number>;
}

function pivotByDate(rows: DailyRow[]): Record<string, Record<string, number>>[] {
  const map: Record<string, Record<string, number>> = {};
  for (const r of rows) {
    if (!map[r.date]) map[r.date] = {};
    map[r.date][`${r.game}`] = r.count;
  }
  return Object.entries(map)
    .map(([date, counts]) => ({ date, ...counts }))
    .sort((a, b) => (a.date as string).localeCompare(b.date as string)) as unknown as Record<string, Record<string, number>>[];
}

export default function HealthPage() {
  const { data, isLoading, error } = useApiData<HealthResponse>("collection-health");

  if (isLoading)
    return (
      <div className="space-y-4">
        <LoadingSkeleton height="h-64" />
        <LoadingSkeleton height="h-64" />
      </div>
    );

  const tradeData = pivotByDate(data?.daily_trades || []);
  const orderbookData = pivotByDate(data?.daily_orderbook || []);
  const totals = data?.table_totals || {};

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">Collection Health</h1>
        <p className="text-base text-[var(--text-muted)] mt-1">
          Data collection monitoring and database statistics
        </p>
      </div>

      {/* Table Totals */}
      <div
        className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden"
        style={{ boxShadow: "var(--shadow-md)" }}
      >
        <div className="px-4 pt-4 pb-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
          <h3 className="text-base font-bold tracking-tight">Database Table Counts</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-[var(--surface-elevated)]/50 border-b-2 border-[var(--border)]">
                <th className="text-left px-4 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Table</th>
                <th className="text-right px-4 py-2.5 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">Records</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(totals).map(([table, count]) => (
                <tr
                  key={table}
                  className="border-t border-[var(--border-subtle)] table-row-hover"
                >
                  <td className="px-4 py-2.5 font-mono text-[var(--accent-teal)]">
                    {table}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono">
                    {formatNumber(count)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Daily Trades Chart */}
      <ChartCard title="Daily Trades Collected" isLoading={false} error={error}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={tradeData as Record<string, unknown>[]} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
            <CartesianGrid {...CHART_THEME.grid} />
            <XAxis
              dataKey="date"
              tick={CHART_THEME.axis.tickSmall}
              tickFormatter={(d: string) => d.slice(5)}
            />
            <YAxis tick={CHART_THEME.axis.tick} />
            <Tooltip {...CHART_THEME.tooltip} />
            <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
            <Bar
              dataKey="cod"
              name="COD"
              fill={GAME_COLORS.cod}
              opacity={CHART_THEME.bar.opacity}
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
            <Bar
              dataKey="cs2"
              name="CS2"
              fill={GAME_COLORS.cs2}
              opacity={CHART_THEME.bar.opacity}
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Daily Orderbook Chart */}
      <ChartCard title="Daily Orderbook Snapshots" isLoading={false} error={error}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={orderbookData as Record<string, unknown>[]} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
            <CartesianGrid {...CHART_THEME.grid} />
            <XAxis
              dataKey="date"
              tick={CHART_THEME.axis.tickSmall}
              tickFormatter={(d: string) => d.slice(5)}
            />
            <YAxis tick={CHART_THEME.axis.tick} />
            <Tooltip {...CHART_THEME.tooltip} />
            <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
            <Bar
              dataKey="cod"
              name="COD"
              fill={GAME_COLORS.cod}
              opacity={CHART_THEME.bar.opacity}
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
            <Bar
              dataKey="cs2"
              name="CS2"
              fill={GAME_COLORS.cs2}
              opacity={CHART_THEME.bar.opacity}
              radius={CHART_THEME.bar.radius}
              animationDuration={CHART_THEME.animationDuration}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartCard>
    </div>
  );
}

"use client";

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from "recharts";
import { useApiData } from "@/lib/hooks";
import { COLORS, CHART_THEME } from "@/lib/colors";
import { formatVolume } from "@/lib/format";
import ChartCard from "../ChartCard";

interface WhaleData {
  whale_volume: number;
  retail_volume: number;
  whale_count: number;
  retail_count: number;
  threshold: number;
}

export default function WhaleRetailPieChart() {
  const { data, error, isLoading } = useApiData<WhaleData>("whale-analysis");

  const volumeData = data
    ? [
        { name: `Whale (>=$${data.threshold})`, value: data.whale_volume },
        { name: `Retail (<$${data.threshold})`, value: data.retail_volume },
      ]
    : [];

  const countData = data
    ? [
        { name: `Whale (>=$${data.threshold})`, value: data.whale_count },
        { name: `Retail (<$${data.threshold})`, value: data.retail_count },
      ]
    : [];

  const PIE_COLORS = [COLORS.red, COLORS.green];

  return (
    <ChartCard title="Whale vs Retail Breakdown" isLoading={isLoading} error={error}>
      <div className="flex h-full">
        <div className="flex-1">
          <p className="text-center text-[11px] text-[var(--text-muted)] font-medium mb-1">By Volume</p>
          <ResponsiveContainer width="100%" height="90%">
            <PieChart>
              <Pie
                data={volumeData}
                cx="50%"
                cy="50%"
                innerRadius={40}
                outerRadius={70}
                dataKey="value"
                label={({ percent }: { percent?: number }) => `${((percent ?? 0) * 100).toFixed(1)}%`}
                labelLine={false}
                animationDuration={CHART_THEME.animationDuration}
                stroke="var(--surface)"
                strokeWidth={2}
              >
                {volumeData.map((_, idx) => (
                  <Cell key={idx} fill={PIE_COLORS[idx]} />
                ))}
              </Pie>
              <Tooltip
                {...CHART_THEME.tooltip}
                formatter={(value: any) => formatVolume(value)}
              />
              <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div className="flex-1">
          <p className="text-center text-[11px] text-[var(--text-muted)] font-medium mb-1">By Count</p>
          <ResponsiveContainer width="100%" height="90%">
            <PieChart>
              <Pie
                data={countData}
                cx="50%"
                cy="50%"
                innerRadius={40}
                outerRadius={70}
                dataKey="value"
                label={({ percent }: { percent?: number }) => `${((percent ?? 0) * 100).toFixed(1)}%`}
                labelLine={false}
                animationDuration={CHART_THEME.animationDuration}
                stroke="var(--surface)"
                strokeWidth={2}
              >
                {countData.map((_, idx) => (
                  <Cell key={idx} fill={PIE_COLORS[idx]} />
                ))}
              </Pie>
              <Tooltip
                {...CHART_THEME.tooltip}
                formatter={(value: any) => value.toLocaleString()}
              />
              <Legend wrapperStyle={CHART_THEME.legend.wrapperStyle} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
    </ChartCard>
  );
}

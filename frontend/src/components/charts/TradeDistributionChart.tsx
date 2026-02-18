"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceLine,
} from "recharts";
import { useApiData } from "@/lib/hooks";
import { COLORS, CHART_THEME } from "@/lib/colors";
import ChartCard from "../ChartCard";

interface Bucket {
  bucket_min: number;
  bucket_max: number;
  count: number;
}

interface DistData {
  linear_buckets: Bucket[];
  log_buckets: Bucket[];
  stats: { count: number; mean: number; median: number; min: number; max: number };
}

export default function TradeDistributionChart() {
  const { data, error, isLoading } = useApiData<DistData>("trade-distribution");

  const linear = (data?.linear_buckets || []).map((b) => ({
    range: `$${b.bucket_min.toFixed(0)}`,
    count: b.count,
    mid: (b.bucket_min + b.bucket_max) / 2,
  }));

  return (
    <ChartCard title="Trade Size Distribution" isLoading={isLoading} error={error}>
      <div className="h-full flex flex-col">
        {data?.stats && (
          <div className="flex gap-4 text-[11px] text-[var(--text-muted)] mb-2 font-medium">
            <span>Mean: ${data.stats.mean.toFixed(0)}</span>
            <span>Median: ${data.stats.median.toFixed(0)}</span>
            <span>Max: ${data.stats.max.toFixed(0)}</span>
          </div>
        )}
        <div className="flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={linear} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
              <CartesianGrid {...CHART_THEME.grid} />
              <XAxis
                dataKey="range"
                tick={CHART_THEME.axis.tickSmall}
                interval="preserveStartEnd"
              />
              <YAxis tick={CHART_THEME.axis.tick} />
              <Tooltip {...CHART_THEME.tooltip} />
              {data?.stats?.median && (
                <ReferenceLine
                  x={`$${data.stats.median.toFixed(0)}`}
                  stroke={COLORS.red}
                  strokeDasharray="5 5"
                  label={{ value: "Median", fill: COLORS.red, fontSize: 10 }}
                />
              )}
              <Bar
                dataKey="count"
                fill={COLORS.green}
                opacity={CHART_THEME.bar.opacity}
                radius={CHART_THEME.bar.radius}
                animationDuration={CHART_THEME.animationDuration}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </ChartCard>
  );
}

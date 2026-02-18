"use client";

import { useApiData } from "@/lib/hooks";
import { formatVolume, formatNumber, formatPercent } from "@/lib/format";
import StatCard from "@/components/StatCard";
import LoadingSkeleton from "@/components/LoadingSkeleton";
import DailyVolumeChart from "@/components/charts/DailyVolumeChart";
import TopMarketsChart from "@/components/charts/TopMarketsChart";
import WhaleRetailPieChart from "@/components/charts/WhaleRetailPieChart";
import TradeDistributionChart from "@/components/charts/TradeDistributionChart";
import BuySellChart from "@/components/charts/BuySellChart";
import WhaleTimelineChart from "@/components/charts/WhaleTimelineChart";
import SpreadAnalysisChart from "@/components/charts/SpreadAnalysisChart";

interface Overview {
  total_markets: number;
  total_trades: number;
  total_volume: number;
  whale_volume: number;
  whale_pct: number;
  whale_trades: number;
  games: Record<
    string,
    {
      markets: number;
      trades: number;
      volume: number;
      whale_volume: number;
      whale_pct: number;
    }
  >;
}

export default function DashboardPage() {
  const { data, isLoading } = useApiData<Overview>("overview");

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">Dashboard</h1>
        <p className="text-base text-[var(--text-muted)] mt-1">
          Polymarket esports analytics overview
        </p>
      </div>

      {/* Stat Cards */}
      {isLoading ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <LoadingSkeleton key={i} height="h-24" />
          ))}
        </div>
      ) : data ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Markets"
            value={formatNumber(data.total_markets)}
          />
          <StatCard
            label="Total Trades"
            value={formatNumber(data.total_trades)}
          />
          <StatCard
            label="Total Volume"
            value={formatVolume(data.total_volume)}
            color="var(--accent-green)"
          />
          <StatCard
            label="Whale Volume %"
            value={formatPercent(data.whale_pct)}
            subValue={`${formatVolume(data.whale_volume)} from ${formatNumber(data.whale_trades)} trades`}
            color="var(--accent-red)"
          />
        </div>
      ) : null}

      {/* Charts Grid */}
      <div className="space-y-8">
        <section>
          <h2 className="text-sm font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4">
            Volume & Activity
          </h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="lg:col-span-2">
              <DailyVolumeChart />
            </div>
            <TopMarketsChart />
            <WhaleRetailPieChart />
          </div>
        </section>

        <section>
          <h2 className="text-sm font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4">
            Trade Analysis
          </h2>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <TradeDistributionChart />
            <BuySellChart />
          </div>
        </section>

        <section>
          <h2 className="text-sm font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-4">
            Whale Activity & Spreads
          </h2>
          <div className="grid grid-cols-1 gap-6">
            <WhaleTimelineChart />
            <SpreadAnalysisChart />
          </div>
        </section>
      </div>
    </div>
  );
}

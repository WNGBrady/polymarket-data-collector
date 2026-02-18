"use client";

import { useState } from "react";
import Link from "next/link";
import { useApiData } from "@/lib/hooks";
import { formatVolume, formatNumber } from "@/lib/format";
import LoadingSkeleton from "@/components/LoadingSkeleton";
import GameBadge from "@/components/GameBadge";

interface Market {
  market_id: string;
  question: string;
  game: string;
  outcomes: string;
  volume: number;
  trade_count: number;
  latest_price: number | null;
  avg_spread: number | null;
}

interface MarketsResponse {
  markets: Market[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export default function MarketsPage() {
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState("volume");
  const [order, setOrder] = useState("desc");
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");

  const { data, isLoading } = useApiData<MarketsResponse>("markets", {
    page,
    page_size: 25,
    sort,
    order,
    search,
  });

  const handleSort = (col: string) => {
    if (sort === col) {
      setOrder(order === "desc" ? "asc" : "desc");
    } else {
      setSort(col);
      setOrder("desc");
    }
    setPage(1);
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setSearch(searchInput);
    setPage(1);
  };

  const sortIcon = (col: string) => {
    if (sort !== col) return "";
    return order === "desc" ? " \u25BC" : " \u25B2";
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-extrabold tracking-tight">Markets</h1>
          <p className="text-base text-[var(--text-muted)] mt-1">
            Browse and search all tracked markets
          </p>
        </div>
        <form onSubmit={handleSearch} className="flex gap-2">
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Search markets..."
            className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm text-[var(--foreground)] w-64 transition-colors duration-200 focus:outline-none focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20 placeholder:text-[var(--text-muted)]/60"
          />
          <button
            type="submit"
            className="bg-[var(--accent-green)] text-[var(--background)] px-4 py-1.5 rounded-lg text-sm font-semibold transition-all duration-200 hover:brightness-110 active:scale-[0.97]"
          >
            Search
          </button>
        </form>
      </div>

      {isLoading ? (
        <LoadingSkeleton height="h-96" />
      ) : (
        <>
          <div
            className="overflow-x-auto rounded-xl border border-[var(--border)]"
            style={{ boxShadow: "var(--shadow-md)" }}
          >
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-[var(--surface-elevated)]/50 border-b-2 border-[var(--border)]">
                  <th className="text-left px-4 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">
                    Market
                  </th>
                  <th className="text-left px-3 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">
                    Game
                  </th>
                  <th
                    className="text-right px-3 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider cursor-pointer transition-colors duration-150 hover:text-[var(--foreground)]"
                    onClick={() => handleSort("volume")}
                  >
                    Volume{sortIcon("volume")}
                  </th>
                  <th
                    className="text-right px-3 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider cursor-pointer transition-colors duration-150 hover:text-[var(--foreground)]"
                    onClick={() => handleSort("trades")}
                  >
                    Trades{sortIcon("trades")}
                  </th>
                  <th className="text-right px-3 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider">
                    Price
                  </th>
                  <th
                    className="text-right px-4 py-3 text-[var(--text-muted)] font-medium text-xs uppercase tracking-wider cursor-pointer transition-colors duration-150 hover:text-[var(--foreground)]"
                    onClick={() => handleSort("spread")}
                  >
                    Spread{sortIcon("spread")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {(data?.markets || []).map((m) => (
                  <tr
                    key={m.market_id}
                    className="border-t border-[var(--border-subtle)] table-row-hover"
                  >
                    <td className="px-4 py-3">
                      <Link
                        href={`/markets/${m.market_id}`}
                        className="text-[var(--accent-green)] transition-colors duration-150 hover:text-[var(--accent-teal)] hover:underline underline-offset-2"
                      >
                        {m.question.length > 65
                          ? m.question.slice(0, 65) + "..."
                          : m.question}
                      </Link>
                    </td>
                    <td className="px-3 py-3">
                      <GameBadge game={m.game} />
                    </td>
                    <td className="px-3 py-3 text-right font-mono text-[var(--foreground)]">
                      {formatVolume(m.volume)}
                    </td>
                    <td className="px-3 py-3 text-right font-mono text-[var(--text-muted)]">
                      {formatNumber(m.trade_count)}
                    </td>
                    <td className="px-3 py-3 text-right font-mono text-[var(--text-muted)]">
                      {m.latest_price != null ? m.latest_price.toFixed(2) : "\u2014"}
                    </td>
                    <td className="px-4 py-3 text-right font-mono text-[var(--text-muted)]">
                      {m.avg_spread != null ? m.avg_spread.toFixed(4) : "\u2014"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {data && data.total_pages > 1 && (
            <div className="flex items-center justify-between text-sm">
              <span className="text-[var(--text-muted)]">
                {formatNumber(data.total)} markets total
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage(Math.max(1, page - 1))}
                  disabled={page <= 1}
                  className="px-3 py-1.5 rounded-lg bg-[var(--surface)] border border-[var(--border)] text-sm font-medium transition-all duration-200 hover:bg-[var(--surface-hover)] hover:border-[var(--border)]/80 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Prev
                </button>
                <span className="px-3 py-1.5 text-[var(--text-muted)] font-mono text-xs">
                  {page} / {data.total_pages}
                </span>
                <button
                  onClick={() => setPage(Math.min(data.total_pages, page + 1))}
                  disabled={page >= data.total_pages}
                  className="px-3 py-1.5 rounded-lg bg-[var(--surface)] border border-[var(--border)] text-sm font-medium transition-all duration-200 hover:bg-[var(--surface-hover)] hover:border-[var(--border)]/80 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

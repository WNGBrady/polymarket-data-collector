"use client";

import { useState, useMemo } from "react";
import { useApiData } from "@/lib/hooks";
import LoadingSkeleton from "@/components/LoadingSkeleton";

interface ClosingLine {
  game_id: string;
  market_id: string;
  home_team: string;
  away_team: string;
  team: string;
  is_home: number;
  question: string;
  game_start_time: string;
  closing_price: number;
  min_price: number;
  max_price: number;
  final_score: string;
  team_won: number | null;
  n_trades: number;
}

interface ClosingLinesResponse {
  data: ClosingLine[];
  stats: {
    total_matches: number;
    favorite_wins: number;
    favorite_win_rate: number;
    avg_confidence: number;
  };
}

function extractTournament(question: string): string {
  // Questions follow: "Counter-Strike: Team A vs Team B (BO3) - Tournament Stage"
  const dashIdx = question.lastIndexOf(" - ");
  if (dashIdx !== -1) return question.slice(dashIdx + 3).trim();
  return "Other";
}

export default function ClosingLinesPage() {
  const [localDateStart, setLocalDateStart] = useState("");
  const [localDateEnd, setLocalDateEnd] = useState("");
  const [selectedTournament, setSelectedTournament] = useState("");

  const { data, isLoading, error } = useApiData<ClosingLinesResponse>("closing-lines", {
    date_start: localDateStart,
    date_end: localDateEnd,
  });

  const rows = data?.data || [];

  // Group by game_id into matches
  const matchMap: Record<string, { home?: ClosingLine; away?: ClosingLine }> = {};
  for (const r of rows) {
    if (!matchMap[r.game_id]) matchMap[r.game_id] = {};
    if (r.is_home) matchMap[r.game_id].home = r;
    else matchMap[r.game_id].away = r;
  }

  const allMatches = Object.entries(matchMap)
    .filter(([, m]) => m.home && m.away)
    .reverse();

  // Extract unique tournaments from all matches
  const tournaments = useMemo(() => {
    const set = new Set<string>();
    for (const [, m] of allMatches) {
      const q = m.home?.question || m.away?.question || "";
      if (q) set.add(extractTournament(q));
    }
    return Array.from(set).sort();
  }, [allMatches.length, rows.length]);

  // Apply tournament filter client-side
  const matches = selectedTournament
    ? allMatches.filter(([, m]) => {
        const q = m.home?.question || m.away?.question || "";
        return extractTournament(q) === selectedTournament;
      })
    : allMatches;

  // Compute stats from visible matches
  const stats = useMemo(() => {
    if (matches.length === 0) return null;
    let favDevTotal = 0;
    let dogDevTotal = 0;
    for (const [, m] of matches) {
      const home = m.home!;
      const away = m.away!;
      const homeFavored = home.closing_price >= away.closing_price;
      const fav = homeFavored ? home : away;
      const dog = homeFavored ? away : home;
      favDevTotal += fav.closing_price - fav.min_price;
      dogDevTotal += dog.closing_price - dog.min_price;
    }
    return {
      total_matches: matches.length,
      avg_fav_deviation: favDevTotal / matches.length,
      avg_dog_deviation: dogDevTotal / matches.length,
    };
  }, [matches.length, selectedTournament, rows.length]);

  if (isLoading)
    return (
      <div className="space-y-4">
        <LoadingSkeleton height="h-12" />
        <LoadingSkeleton height="h-96" />
      </div>
    );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">Closing Lines</h1>
        <p className="text-base text-[var(--text-muted)] mt-1">
          Pre-match closing prices and win/loss analysis
        </p>
      </div>

      {/* Filter bar */}
      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1">
          <label className="text-[10px] uppercase tracking-wider font-medium text-[var(--text-muted)]">
            Start Date
          </label>
          <input
            type="date"
            value={localDateStart}
            onChange={(e) => setLocalDateStart(e.target.value)}
            className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20 outline-none"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] uppercase tracking-wider font-medium text-[var(--text-muted)]">
            End Date
          </label>
          <input
            type="date"
            value={localDateEnd}
            onChange={(e) => setLocalDateEnd(e.target.value)}
            className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20 outline-none"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[10px] uppercase tracking-wider font-medium text-[var(--text-muted)]">
            Tournament
          </label>
          <select
            value={selectedTournament}
            onChange={(e) => setSelectedTournament(e.target.value)}
            className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-3 py-1.5 text-sm focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20 outline-none"
          >
            <option value="">All Tournaments</option>
            {tournaments.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
        {(localDateStart || localDateEnd || selectedTournament) && (
          <button
            onClick={() => {
              setLocalDateStart("");
              setLocalDateEnd("");
              setSelectedTournament("");
            }}
            className="text-xs text-[var(--text-muted)] hover:text-[var(--foreground)] px-2 py-1.5 transition-colors"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Compact summary strip */}
      {stats && (
        <div
          className="flex items-center gap-0 rounded-xl border border-[var(--border)] bg-[var(--surface)] overflow-hidden"
          style={{ boxShadow: "var(--shadow-md)" }}
        >
          <SummaryStat label="Matches" value={String(stats.total_matches)} />
          <Divider />
          <SummaryStat
            label="Avg Fav Deviation"
            value={stats.avg_fav_deviation.toFixed(3)}
            color="var(--accent-green)"
          />
          <Divider />
          <SummaryStat
            label="Avg Dog Deviation"
            value={stats.avg_dog_deviation.toFixed(3)}
            color="var(--accent-teal)"
          />
        </div>
      )}

      {/* Match cards grid */}
      {matches.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-3">
          {matches.map(([gameId, m]) => {
            const home = m.home!;
            const away = m.away!;
            const homeFavored = home.closing_price >= away.closing_price;
            const score = (home.final_score?.replace(/"/g, "").replace(/^0+-0+\|/, "") || "").trim();
            const dateStr = home.game_start_time?.slice(0, 16) || "";

            return (
              <div
                key={gameId}
                className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden card-hover inner-glow"
                style={{ boxShadow: "var(--shadow-md)" }}
              >
                {/* Header bar */}
                <div className="flex items-center justify-between px-3 py-2 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
                  <span className="text-sm font-bold tracking-tight truncate">
                    {home.home_team} vs {away.away_team}
                  </span>
                  <div className="flex items-center gap-3 shrink-0">
                    {score && (
                      <span className="text-sm font-extrabold font-mono text-[var(--accent-yellow)]">
                        {score}
                      </span>
                    )}
                    <span className="text-[11px] text-[var(--text-muted)]">
                      {dateStr}
                    </span>
                  </div>
                </div>

                {/* Two-column split */}
                <div className="grid grid-cols-2 divide-x divide-[var(--border-subtle)]">
                  <TeamColumn label="Home" team={home} isFavored={homeFavored} />
                  <TeamColumn label="Away" team={away} isFavored={!homeFavored} />
                </div>
              </div>
            );
          })}
        </div>
      )}

      {error && (
        <p className="text-[var(--accent-red)] text-sm">Failed to load closing lines data.</p>
      )}
      {!isLoading && rows.length === 0 && !error && (
        <p className="text-[var(--text-muted)]">
          No closing line data available. Run the migration to import from CSV.
        </p>
      )}
    </div>
  );
}

/* ---- Sub-components ---- */

function SummaryStat({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div className="flex-1 px-5 py-3 text-center min-w-0">
      <p className="text-[10px] uppercase tracking-wider font-medium text-[var(--text-muted)] mb-0.5 truncate">
        {label}
      </p>
      <p
        className="text-lg font-extrabold tracking-tight truncate"
        style={color ? { color } : undefined}
      >
        {value}
      </p>
    </div>
  );
}

function Divider() {
  return <div className="w-px self-stretch bg-[var(--border-subtle)]" />;
}

function TeamColumn({
  label,
  team,
  isFavored,
}: {
  label: string;
  team: ClosingLine;
  isFavored: boolean;
}) {
  const won = team.team_won === 1;
  const lost = team.team_won === 0;

  return (
    <div
      className="px-3 py-2 space-y-1"
      style={
        isFavored
          ? { backgroundColor: "rgba(0, 229, 176, 0.04)" }
          : undefined
      }
    >
      {/* Label + badge */}
      <div className="flex items-center gap-2">
        <span className="text-[10px] uppercase tracking-wider font-medium text-[var(--text-muted)]">
          {label}
        </span>
        {isFavored && (
          <span className="text-[9px] uppercase tracking-wide font-bold px-1.5 py-0.5 rounded bg-[var(--accent-green)]/15 text-[var(--accent-green)] border border-[var(--accent-green)]/20">
            Favored
          </span>
        )}
      </div>

      {/* Team name */}
      <p className="text-sm font-semibold truncate">{team.team}</p>

      {/* Closing price + range on one line */}
      <div className="flex items-baseline gap-2">
        <span
          className="text-xl font-bold font-mono tracking-tight"
          style={{
            color: isFavored ? "var(--accent-green)" : "var(--foreground)",
          }}
        >
          {team.closing_price.toFixed(2)}
        </span>
        <span className="text-[11px] text-[var(--text-muted)] font-mono">
          {team.min_price.toFixed(2)} &rarr; {team.max_price.toFixed(2)}
        </span>
      </div>

      {/* Trade count + win/loss on one line */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-[var(--text-muted)]">
          {team.n_trades} trades
        </span>
        {(won || lost) && (
          <span
            className={`text-xs font-bold ${
              won ? "text-[var(--accent-green)]" : "text-[var(--accent-red)]"
            }`}
          >
            &middot; {won ? "Win" : "Loss"}
          </span>
        )}
      </div>
    </div>
  );
}

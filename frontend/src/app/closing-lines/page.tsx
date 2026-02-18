"use client";

import { useApiData } from "@/lib/hooks";
import { formatPercent } from "@/lib/format";
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

export default function ClosingLinesPage() {
  const { data, isLoading, error } = useApiData<ClosingLinesResponse>("closing-lines");

  if (isLoading)
    return (
      <div className="space-y-4">
        <LoadingSkeleton height="h-12" />
        <LoadingSkeleton height="h-96" />
      </div>
    );

  const stats = data?.stats;
  const rows = data?.data || [];

  // Group by game_id into matches
  const matchMap: Record<string, { home?: ClosingLine; away?: ClosingLine }> = {};
  for (const r of rows) {
    if (!matchMap[r.game_id]) matchMap[r.game_id] = {};
    if (r.is_home) matchMap[r.game_id].home = r;
    else matchMap[r.game_id].away = r;
  }

  const matches = Object.entries(matchMap)
    .filter(([, m]) => m.home && m.away)
    .reverse();

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-3xl font-extrabold tracking-tight">Closing Lines</h1>
        <p className="text-base text-[var(--text-muted)] mt-1">
          Pre-match closing prices and win/loss analysis
        </p>
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
            label="Fav Win Rate"
            value={formatPercent(stats.favorite_win_rate)}
            color="var(--accent-green)"
          />
          <Divider />
          <SummaryStat
            label="Fav Record"
            value={`${stats.favorite_wins}W \u2013 ${stats.total_matches - stats.favorite_wins}L`}
          />
          <Divider />
          <SummaryStat
            label="Avg Confidence"
            value={formatPercent(stats.avg_confidence)}
            color="var(--accent-teal)"
          />
        </div>
      )}

      {/* Match cards grid */}
      {matches.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          {matches.map(([gameId, m]) => {
            const home = m.home!;
            const away = m.away!;
            const homeFavored = home.closing_price >= away.closing_price;
            const score = (home.final_score?.replace(/"/g, "") || "").trim();
            const dateStr = home.game_start_time?.slice(0, 16) || "";

            return (
              <div
                key={gameId}
                className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden card-hover inner-glow"
                style={{ boxShadow: "var(--shadow-md)" }}
              >
                {/* Header bar */}
                <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
                  <span className="text-sm font-bold tracking-tight">
                    {home.home_team} vs {away.away_team}
                  </span>
                  <div className="flex items-center gap-3">
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
                  {/* Home side */}
                  <TeamColumn
                    label="Home"
                    team={home}
                    isFavored={homeFavored}
                  />
                  {/* Away side */}
                  <TeamColumn
                    label="Away"
                    team={away}
                    isFavored={!homeFavored}
                  />
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
      className="p-4 space-y-2"
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

      {/* Hero closing price */}
      <p
        className="text-3xl font-extrabold font-mono tracking-tight"
        style={{
          color: isFavored ? "var(--accent-green)" : "var(--foreground)",
        }}
      >
        {team.closing_price.toFixed(2)}
      </p>

      {/* Price range */}
      <p className="text-xs text-[var(--text-muted)] font-mono">
        {team.min_price.toFixed(2)} &rarr; {team.max_price.toFixed(2)}
      </p>

      {/* Trade count */}
      <p className="text-xs text-[var(--text-muted)]">
        {team.n_trades} trades
      </p>

      {/* Win / Loss */}
      {(won || lost) && (
        <p
          className={`text-sm font-bold ${
            won ? "text-[var(--accent-green)]" : "text-[var(--accent-red)]"
          }`}
        >
          {won ? "Win" : "Loss"}
        </p>
      )}
    </div>
  );
}

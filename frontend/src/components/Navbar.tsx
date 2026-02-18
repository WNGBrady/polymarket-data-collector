"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useFilters } from "@/context/FilterContext";

const NAV_LINKS = [
  { href: "/", label: "Dashboard" },
  { href: "/markets", label: "Markets" },
  { href: "/closing-lines", label: "Closing Lines" },
  { href: "/pre-match", label: "Pre-Match" },
  { href: "/health", label: "Health" },
];

const GAMES = [
  { value: "all", label: "All" },
  { value: "cod", label: "COD" },
  { value: "cs2", label: "CS2" },
];

export default function Navbar() {
  const pathname = usePathname();
  const { game, setGame, dateStart, setDateStart, dateEnd, setDateEnd } =
    useFilters();

  return (
    <nav className="sticky top-0 z-50 border-b-2 border-[var(--border)] bg-[var(--surface)]/80 backdrop-blur-xl"
         style={{ boxShadow: "0 4px 20px rgba(60,60,120,0.35)" }}>
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo + Nav Links */}
          <div className="flex items-center gap-6">
            <Link
              href="/"
              className="text-gradient-green font-extrabold text-xl tracking-tight"
            >
              PM Esports
            </Link>
            <div className="hidden md:flex items-center gap-0.5">
              {NAV_LINKS.map((link) => {
                const active = pathname === link.href;
                return (
                  <Link
                    key={link.href}
                    href={link.href}
                    className={`relative px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 ${
                      active
                        ? "text-[var(--foreground)] bg-[var(--surface-elevated)]"
                        : "text-[var(--text-muted)] hover:text-[var(--foreground)] hover:bg-[var(--surface-hover)]/50"
                    }`}
                  >
                    {link.label}
                    {active && (
                      <span
                        className="absolute bottom-0 left-3 right-3 h-[3px] rounded-full bg-[var(--accent-green)]"
                        style={{ boxShadow: "0 0 8px rgba(0,229,176,0.5)" }}
                      />
                    )}
                  </Link>
                );
              })}
            </div>
          </div>

          {/* Filters */}
          <div className="flex items-center gap-3">
            {/* Game Toggle */}
            <div className="flex rounded-lg p-0.5 bg-[var(--background)] border border-[var(--border)]">
              {GAMES.map((g) => (
                <button
                  key={g.value}
                  onClick={() => setGame(g.value)}
                  className={`px-3 py-1 rounded-md text-xs font-semibold transition-all duration-200 ${
                    game === g.value
                      ? "bg-[var(--accent-green)] text-[var(--background)] shadow-sm"
                      : "text-[var(--text-muted)] hover:text-[var(--foreground)]"
                  }`}
                  style={
                    game === g.value
                      ? { boxShadow: "0 0 10px rgba(0,229,176,0.35)" }
                      : undefined
                  }
                >
                  {g.label}
                </button>
              ))}
            </div>

            {/* Date Range */}
            <div className="hidden sm:flex items-center gap-2">
              <input
                type="date"
                value={dateStart}
                onChange={(e) => setDateStart(e.target.value)}
                className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-2.5 py-1 text-xs text-[var(--foreground)] transition-colors duration-200 focus:outline-none focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20"
              />
              <span className="text-[var(--text-muted)] text-xs">to</span>
              <input
                type="date"
                value={dateEnd}
                onChange={(e) => setDateEnd(e.target.value)}
                className="bg-[var(--background)] border border-[var(--border)] rounded-lg px-2.5 py-1 text-xs text-[var(--foreground)] transition-colors duration-200 focus:outline-none focus:border-[var(--accent-green)]/50 focus:ring-1 focus:ring-[var(--accent-green)]/20"
              />
            </div>
          </div>
        </div>

        {/* Mobile Nav */}
        <div className="flex md:hidden items-center gap-1 pb-2 overflow-x-auto">
          {NAV_LINKS.map((link) => {
            const active = pathname === link.href;
            return (
              <Link
                key={link.href}
                href={link.href}
                className={`px-3 py-1 rounded-lg text-xs whitespace-nowrap font-medium transition-all duration-200 ${
                  active
                    ? "text-[var(--foreground)] bg-[var(--surface-elevated)]"
                    : "text-[var(--text-muted)]"
                }`}
              >
                {link.label}
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
}

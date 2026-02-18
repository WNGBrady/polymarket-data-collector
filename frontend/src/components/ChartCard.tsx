import { ReactNode } from "react";
import LoadingSkeleton from "./LoadingSkeleton";

interface ChartCardProps {
  title: string;
  children: ReactNode;
  isLoading?: boolean;
  error?: Error | null;
  height?: string;
}

export default function ChartCard({
  title,
  children,
  isLoading,
  error,
  height = "h-80",
}: ChartCardProps) {
  if (isLoading) return <LoadingSkeleton height={height} />;

  return (
    <div
      className="bg-[var(--surface)] rounded-xl border border-[var(--border)] overflow-hidden card-hover inner-glow"
      style={{ boxShadow: "var(--shadow-md)" }}
    >
      <div className="px-4 pt-4 pb-3 border-b border-[var(--border-subtle)] bg-[var(--surface-elevated)]/30">
        <h3 className="text-base font-bold text-[var(--foreground)] tracking-tight">
          {title}
        </h3>
      </div>
      <div className="p-4">
        {error ? (
          <div className="flex items-center justify-center gap-2 text-[var(--accent-red)] text-sm h-40">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" className="opacity-70">
              <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="1.5"/>
              <path d="M8 4.5v4M8 10.5v.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
            </svg>
            Failed to load data
          </div>
        ) : (
          <div className={height}>{children}</div>
        )}
      </div>
    </div>
  );
}

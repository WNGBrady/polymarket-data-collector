interface StatCardProps {
  label: string;
  value: string;
  subValue?: string;
  color?: string;
}

export default function StatCard({
  label,
  value,
  subValue,
  color = "var(--accent-green)",
}: StatCardProps) {
  return (
    <div
      className="group bg-[var(--surface)] rounded-xl border border-[var(--border)] p-5 card-hover inner-glow transition-all duration-200 hover:bg-[var(--surface-hover)] hover:border-[var(--border)]/80"
      style={{
        boxShadow: "var(--shadow-md)",
        borderLeft: `3px solid ${color}`,
      }}
    >
      <p className="text-[var(--text-muted)] text-[11px] uppercase tracking-wider font-medium mb-1.5">
        {label}
      </p>
      <p className="text-3xl font-extrabold tracking-tight" style={{ color }}>
        {value}
      </p>
      {subValue && (
        <p className="text-[var(--text-muted)] text-xs mt-1.5">{subValue}</p>
      )}
    </div>
  );
}

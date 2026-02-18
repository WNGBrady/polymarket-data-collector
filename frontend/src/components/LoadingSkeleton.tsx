export default function LoadingSkeleton({
  height = "h-64",
}: {
  height?: string;
}) {
  return (
    <div
      className={`rounded-xl border border-[var(--border)] ${height} skeleton-shimmer`}
      style={{ boxShadow: "var(--shadow-sm)" }}
    />
  );
}

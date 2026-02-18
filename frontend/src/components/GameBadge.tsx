interface GameBadgeProps {
  game: string;
  size?: "sm" | "md";
}

export default function GameBadge({ game, size = "sm" }: GameBadgeProps) {
  const isCS2 = game === "cs2";
  return (
    <span
      className={`inline-flex items-center font-bold uppercase tracking-wide rounded-md border transition-colors duration-150 ${
        size === "md" ? "text-xs px-2.5 py-1" : "text-[10px] px-2 py-0.5"
      } ${
        isCS2
          ? "bg-[var(--accent-red)]/15 text-[var(--accent-red)] border-[var(--accent-red)]/20"
          : "bg-[var(--accent-green)]/15 text-[var(--accent-green)] border-[var(--accent-green)]/20"
      }`}
    >
      {game.toUpperCase()}
    </span>
  );
}

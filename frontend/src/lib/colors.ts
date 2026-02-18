export const COLORS = {
  green: "#00e5b0",
  red: "#ff5c5c",
  teal: "#4ecdc4",
  yellow: "#ffe66d",
  purple: "#a29bfe",
  pink: "#fd79a8",
};

export const PALETTE = [
  COLORS.green,
  COLORS.red,
  COLORS.teal,
  COLORS.yellow,
  COLORS.purple,
  COLORS.pink,
];

export const GAME_COLORS: Record<string, string> = {
  cod: COLORS.green,
  cs2: COLORS.red,
};

/** Shared chart styling — replaces all hardcoded values across chart components */
export const CHART_THEME = {
  grid: {
    stroke: "#1e1e36",
    strokeDasharray: "3 3",
  },
  axis: {
    tick: { fill: "#6b6b85", fontSize: 11 },
    tickSmall: { fill: "#6b6b85", fontSize: 9 },
  },
  tooltip: {
    contentStyle: {
      background: "#1c1c34",
      border: "1px solid #2e2e5a",
      borderRadius: 10,
      boxShadow: "0 20px 40px rgba(60,60,120,0.4), 0 8px 16px rgba(0,0,0,0.35)",
    },
    labelStyle: { color: "#eeeeee", fontWeight: 500 },
    itemStyle: { color: "#c0c0d0" },
  },
  bar: {
    radius: [4, 4, 0, 0] as [number, number, number, number],
    radiusHorizontal: [0, 4, 4, 0] as [number, number, number, number],
    opacity: 0.9,
  },
  line: {
    strokeWidth: 2.5,
    dot: false as const,
    activeDot: { r: 5, strokeWidth: 2, stroke: "#0a0a14" },
  },
  legend: {
    wrapperStyle: { fontSize: 11, color: "#8a8aa3" },
  },
  animationDuration: 800,
} as const;

// Display formatters. Pure functions, no DOM, no fetch.
//
// `fmt.dollar` returns a signed string (`+$1.23` / `-$1.23`) with `—`
// for null — matching the existing convention scattered across app.js.
// `fmt.pct` is for *signed deltas* (`+12.34%` / `-12.34%`); use
// `fmt.prob` for unsigned probabilities (`54.1%`).

export const fmt = {
  pct:   v => v == null ? "—" : (v >= 0 ? "+" : "") + (v * 100).toFixed(2) + "%",
  prob:  v => v == null ? "—" : (v * 100).toFixed(1) + "%",
  odds:  v => v == null ? "—" : (v > 0 ? "+" : "") + v,
  trueOdds: v => v == null ? "—" : (v > 0 ? "+" : "") + Number(v).toFixed(2),
  dollar:v => v == null ? "—" : (v >= 0 ? "+$" : "-$") + Math.abs(v).toFixed(2),
  time:  iso => {
    if (!iso) return "—";
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  },
};

// HTML escape — used everywhere we interpolate user/data strings into
// innerHTML. Treats null/undefined as the empty string so a missing
// field renders cleanly rather than rendering "null".
export function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

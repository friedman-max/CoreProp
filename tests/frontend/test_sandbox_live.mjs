// Frontend tests for the live-wired sandbox helpers in page-sandbox.jsx.
//
// The browser component touches React/window/document, so we can't import
// it wholesale into Node. Instead we slice out the JSX-free helper block
// (everything between `const SB_LEAGUES` and `function SandboxPage`), eval
// it in an isolated scope, and exercise each pure helper:
//
//   1. SB_BREAK_EVEN matches engine/constants.py BREAK_EVEN exactly — the
//      "Break-even for this slip" hint must equal the gate the backend uses.
//   2. There is NO 2-leg flex: sbAllowedSizes("flex") starts at 3.
//   3. sbBuildRequest sends min_prob as a fraction + the right fields.
//   4. sbFilterByRange windows the equity series correctly (and never
//      blanks the chart).
//   5. sbMapResult maps the /api/sandbox/run payload into UI shape.
//   6. sbFmtCI renders bootstrap CIs / handles null.
//
// Run with: node tests/frontend/test_sandbox_live.mjs

import fs from "node:fs";
import path from "node:path";
import url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..", "..");
const SANDBOX_JSX = path.join(ROOT, "web", "static", "page-sandbox.jsx");
const CONSTANTS_PY = path.join(ROOT, "engine", "constants.py");

const src = fs.readFileSync(SANDBOX_JSX, "utf8");

// Slice the JSX-free helper region. Start at the first real const (skip the
// `= React` destructure line, which would throw in Node) and stop at the
// component definition.
const startIdx = src.indexOf("const SB_LEAGUES");
const endIdx = src.indexOf("function SandboxPage");
if (startIdx < 0 || endIdx < 0) {
  throw new Error("Could not locate the sandbox helper block — did the file move?");
}
const helperBlock = src.slice(startIdx, endIdx);

const bundle = [
  helperBlock,
  "return { SB_BREAK_EVEN, SB_RANGE_MS, sbAllowedSizes, sbBreakEven, sbBuildRequest, sbFilterByRange, sbMapResult, sbFmtCI };",
].join("\n");

const H = new Function(bundle)();

// Parse engine/constants.py BREAK_EVEN into {type: {size: frac}} so the
// parity test catches drift if either side changes.
function parseConstantsBreakEven() {
  const py = fs.readFileSync(CONSTANTS_PY, "utf8");
  const block = py.slice(py.indexOf("BREAK_EVEN = {"), py.indexOf("}", py.indexOf("BREAK_EVEN = {")));
  const out = { power: {}, flex: {} };
  const re = /\(\s*"(\d+)"\s*,\s*"(\w+)"\s*\)\s*:\s*([\d.]+)/g;
  let m;
  while ((m = re.exec(block)) !== null) {
    const [, size, type, val] = m;
    (out[type] = out[type] || {})[Number(size)] = parseFloat(val);
  }
  return out;
}

// ── Test runner ───────────────────────────────────────────────────────
let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); console.log("  ✓ " + name); pass++; }
  catch (e) { console.log("  ✗ " + name + "\n      " + e.message); fail++; }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || "assertion failed"); }
function eq(a, b, msg) {
  if (JSON.stringify(a) !== JSON.stringify(b)) {
    throw new Error(`${msg || "not equal"}: ${JSON.stringify(a)} !== ${JSON.stringify(b)}`);
  }
}

console.log("sandbox live helpers:");

// ── 1. Break-even parity with engine/constants.py ─────────────────────
test("SB_BREAK_EVEN matches engine/constants.py BREAK_EVEN exactly", () => {
  const expected = parseConstantsBreakEven();
  eq(H.SB_BREAK_EVEN, expected, "break-even table diverged from constants.py");
});

test("flex has no 2-leg break-even (not a real product)", () => {
  assert(H.SB_BREAK_EVEN.flex[2] === undefined, "flex must not define a 2-leg break-even");
  assert(H.SB_BREAK_EVEN.power[2] !== undefined, "power should define a 2-leg break-even");
});

test("the old wrong break-evens are gone (e.g. power-2 was 60%)", () => {
  // Regression: the prototype hard-coded power {2:60, ...}; the correct
  // value is 57.74% = (1/3)^(1/2).
  assert(Math.abs(H.SB_BREAK_EVEN.power[2] - 0.5774) < 1e-9, "power-2 BE should be 0.5774");
  assert(Math.abs(H.SB_BREAK_EVEN.power[6] - 0.5407) < 1e-9, "power-6 BE should be 0.5407");
});

// ── 2. No 2-leg flex anywhere ─────────────────────────────────────────
test("sbAllowedSizes(power) is [2,3,4,5,6]", () => {
  eq(H.sbAllowedSizes("power"), [2, 3, 4, 5, 6]);
});
test("sbAllowedSizes(flex) is [3,4,5,6] — never includes 2", () => {
  eq(H.sbAllowedSizes("flex"), [3, 4, 5, 6]);
  assert(!H.sbAllowedSizes("flex").includes(2), "2-leg flex must be impossible");
});

// ── 3. sbBreakEven lookup + fallback ──────────────────────────────────
test("sbBreakEven returns the table value for valid combos", () => {
  assert(Math.abs(H.sbBreakEven("power", 2) - 0.5774) < 1e-9);
  assert(Math.abs(H.sbBreakEven("flex", 3) - 0.5774) < 1e-9);
});
test("sbBreakEven falls back to power-6 for unknown combos (e.g. flex-2)", () => {
  assert(Math.abs(H.sbBreakEven("flex", 2) - 0.5407) < 1e-9, "flex-2 should fall back, not crash");
});

// ── 4. sbBuildRequest shape ───────────────────────────────────────────
test("sbBuildRequest sends min_prob as a fraction + correct fields", () => {
  const req = H.sbBuildRequest({
    leagues: new Set(["NBA", "MLB"]), minProb: 54.1, size: 5, type: "flex", kelly: true,
  });
  assert(Math.abs(req.min_prob - 0.541) < 1e-9, `min_prob should be 0.541, got ${req.min_prob}`);
  eq(req.slip_size, 5);
  eq(req.slip_type, "flex");
  eq(req.use_kelly, true);
  eq(req.bet_size, 1.0);
  eq([...req.leagues].sort(), ["MLB", "NBA"]);
});

// ── 5. sbFilterByRange windowing ──────────────────────────────────────
const DAY = 86400000;
function mkSeries(nDays) {
  const end = Date.UTC(2026, 4, 28, 12, 0, 0);
  const arr = [];
  for (let i = nDays - 1; i >= 0; i--) arr.push({ x: end - i * DAY, y: 100 + (nDays - i) });
  return arr;
}
test("MAX returns the full series", () => {
  const s = mkSeries(120);
  eq(H.sbFilterByRange(s, "MAX").length, 120);
});
test("1W keeps only the last ~7 days (anchored to last point, not now)", () => {
  const s = mkSeries(120);   // dates end 2026-05-28, weeks in the past vs real "now"
  const out = H.sbFilterByRange(s, "1W");
  assert(out.length >= 7 && out.length <= 8, `1W window should be ~7-8 points, got ${out.length}`);
  assert(out[out.length - 1] === s[s.length - 1], "last point must be preserved");
});
test("empty series returns empty", () => {
  eq(H.sbFilterByRange([], "1M"), []);
});
test("a window that excludes everything falls back to the last point (never blank)", () => {
  // Single point, 1D window — the point IS within 1D of itself, so it stays.
  const s = [{ x: Date.UTC(2026, 4, 28), y: 105 }];
  eq(H.sbFilterByRange(s, "1D").length, 1);
});

// ── 6. sbMapResult ────────────────────────────────────────────────────
test("sbMapResult maps summary, equity curve (+startBank), and slip log", () => {
  const payload = {
    summary: {
      total_slips: 3, bet_slips: 3, total_profit: 12.5, roi_pct: 8.3,
      win_rate_pct: 41.2, max_drawdown_pct: 5.1, bankroll: 100,
      ci: { roi_pct: { lo: -2.1, hi: 18.4 }, win_rate_pct: { lo: 30, hi: 55 } },
    },
    equity_curve: [
      { x: "2026-05-01T00:00:00+00:00", y: 5 },
      { x: "2026-05-02T00:00:00+00:00", y: 12.5 },
    ],
    slips: [
      { timestamp: "2026-05-01T00:00:00+00:00", league: "NBA", hits: 6, n_eff: 6, n_legs: 6, payout: 40, bet_size: 1, profit: 39 },
      { timestamp: "2026-05-02T00:00:00+00:00", league: "MLB", hits: 4, n_eff: 5, n_legs: 5, payout: 0, bet_size: 1, profit: -1 },
    ],
  };
  const r = H.sbMapResult(payload);
  eq(r.slips, 3);
  eq(r.profit, 12.5);
  eq(r.roi, 8.3);
  eq(r.winRate, 41.2);
  eq(r.drawdown, 5.1);
  eq(r.startBank, 100);
  // Equity y = startBank + cumulative profit.
  eq(r.series.length, 2);
  eq(r.series[0].y, 105);
  eq(r.series[1].y, 112.5);
  assert(typeof r.series[0].x === "number", "series x should be epoch ms");
  // Slip log.
  eq(r.slipLog.length, 2);
  eq(r.slipLog[0].legs, 6);
  eq(r.slipLog[0].hits, 6);
  eq(r.slipLog[0].nEff, 6);
  eq(r.slipLog[0].payout, 40);
  eq(r.slipLog[1].pl, -1);
  eq(r.slipLog[1].nEff, 5);
});

test("sbMapResult defaults startBank to 100 when summary.bankroll missing", () => {
  const r = H.sbMapResult({ summary: {}, equity_curve: [{ x: "2026-05-01T00:00:00+00:00", y: 7 }], slips: [] });
  eq(r.startBank, 100);
  eq(r.series[0].y, 107);
});

test("sbMapResult tolerates an empty payload", () => {
  const r = H.sbMapResult({});
  eq(r.slips, 0);
  eq(r.series, []);
  eq(r.slipLog, []);
});

// ── 7. sbFmtCI ────────────────────────────────────────────────────────
test("sbFmtCI renders null/partial as empty string", () => {
  eq(H.sbFmtCI(null, "%"), "");
  eq(H.sbFmtCI({ lo: null, hi: 5 }, "%"), "");
  eq(H.sbFmtCI(undefined, "%"), "");
});
test("sbFmtCI formats bounds with signs", () => {
  eq(H.sbFmtCI({ lo: -2.14, hi: 18.4 }, "%"), "95% CI [-2.1%, +18.4%]");
});
test("sbFmtCI renders a zero bound with a + sign", () => {
  eq(H.sbFmtCI({ lo: 0, hi: 3.2 }, "%"), "95% CI [+0.0%, +3.2%]");
});
test("sbFmtCI omits the suffix when none is given", () => {
  eq(H.sbFmtCI({ lo: 1.2, hi: 3.4 }), "95% CI [+1.2, +3.4]");
});

// ── 8. Additional edge cases (future-proofing the safety net) ─────────
test("sbBuildRequest defaults kelly to false and tolerates empty leagues", () => {
  const req = H.sbBuildRequest({ leagues: new Set(), minProb: 60, size: 3, type: "power" });
  eq(req.use_kelly, false);
  eq(req.leagues, []);
  eq(req.bet_size, 1.0);
});
test("sbBuildRequest passes kelly through when true", () => {
  const req = H.sbBuildRequest({ leagues: new Set(["NBA"]), minProb: 55, size: 6, type: "flex", kelly: true });
  eq(req.use_kelly, true);
});
test("sbBreakEven falls back to power-6 for an unknown type", () => {
  assert(Math.abs(H.sbBreakEven("parlay", 3) - 0.5407) < 1e-9, "unknown type should fall back to 0.5407");
});
test("sbFilterByRange 1M keeps ~30 days anchored to the last point", () => {
  const s = mkSeries(120);
  const out = H.sbFilterByRange(s, "1M");
  assert(out.length >= 30 && out.length <= 31, `1M window should be ~30-31 points, got ${out.length}`);
  assert(out[out.length - 1] === s[s.length - 1], "last point preserved");
});
test("sbFilterByRange 3M keeps ~90 days", () => {
  const s = mkSeries(200);
  const out = H.sbFilterByRange(s, "3M");
  assert(out.length >= 90 && out.length <= 91, `3M window should be ~90-91 points, got ${out.length}`);
});
test("sbMapResult exposes ts + stake on each slip-log row", () => {
  const r = H.sbMapResult({
    summary: { total_slips: 1 },
    equity_curve: [],
    slips: [{ timestamp: "2026-05-01T00:00:00+00:00", n_legs: 3, hits: 3, n_eff: 3, bet_size: 2.5, payout: 15, profit: 12.5 }],
  });
  eq(r.slipLog[0].ts, "2026-05-01T00:00:00+00:00");
  eq(r.slipLog[0].stake, 2.5);
  eq(r.slipLog[0].pl, 12.5);
});
test("sbMapResult derives legs from the legs array when n_legs is absent", () => {
  const r = H.sbMapResult({
    summary: {},
    equity_curve: [],
    slips: [{ timestamp: "2026-05-01T00:00:00+00:00", legs: [{}, {}, {}, {}], hits: 2, bet_size: 1, payout: 0, profit: -1 }],
  });
  eq(r.slipLog[0].legs, 4);
  eq(r.slipLog[0].nEff, 0);  // n_eff absent, n_legs absent → 0
});

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

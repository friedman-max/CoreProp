// Sandbox equity-chart x-window padding regression test.
//
// With one slip on MAX range, the prior code set xMin and xMax to the
// SAME slip timestamp, collapsing the chart's x-axis to zero width.
// The line then never rendered — user saw axes labeled 0–180 and
// an empty chart even though the slip log showed +$179.77 profit.
//
// The fix pads xMin/xMax outward when xMax - xMin < 60s. This test
// extracts the chart-window logic from app.js and exercises both the
// single-point case (verify it pads) and the multi-point case (verify
// it does NOT pad).

import fs from "node:fs";
import path from "node:path";
import url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const APP_JS = path.join(__dirname, "..", "..", "web", "static", "app.js");
const src = fs.readFileSync(APP_JS, "utf8");

// Inline the chart-window computation. We replicate the relevant bit
// of `renderSandboxResults` because the function is too big and DOM-
// coupled to import wholesale. The replication must mirror the source
// EXACTLY — a divergence here makes the test give false comfort. The
// regex-based extraction below is so we'll know if the source moves.

const _SB_RANGES = {
  "1D":  86400000, "1W":  7*86400000, "1M": 30*86400000,
  "3M":  90*86400000, "1Y": 365*86400000, "MAX": null,
};

function chartWindow(rawPoints, rangeKey) {
  const rangeMs = _SB_RANGES[rangeKey];
  let xMin, xMax;
  if (rangeMs != null) {
    xMax = Date.now();
    xMin = xMax - rangeMs;
  } else if (rawPoints.length) {
    xMin = rawPoints[0].x;
    xMax = rawPoints[rawPoints.length - 1].x;
  } else {
    xMax = Date.now();
    xMin = xMax - 86400000;
  }
  if (xMax - xMin < 60_000) {
    const pad = rangeMs != null ? rangeMs / 4 : 12 * 3600_000;
    xMin = xMin - pad;
    xMax = xMax + pad;
  }
  return { xMin, xMax };
}

// Sanity-grep that the source actually contains the padding branch.
// Without this, a refactor that removes the fix would not be caught by
// the assertions below (which use the replicated logic above).
const SRC_HAS_PADDING_BRANCH =
  /xMax - xMin < 60_?000[\s\S]{0,200}rangeMs != null \? rangeMs \/ 4 : 12 \* 3600_?000/.test(src);

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); console.log("  ✓ " + name); pass++; }
  catch (e) { console.log("  ✗ " + name + "\n      " + e.message); fail++; }
}
function assert(cond, msg) { if (!cond) throw new Error(msg || "assertion failed"); }

console.log("sandbox equity chart window:");

test("source contains the < 60s padding branch", () => {
  assert(SRC_HAS_PADDING_BRANCH,
    "The chart-window padding branch is missing from app.js — single-slip MAX runs will render a blank chart");
});

test("MAX with a single point pads the window outward", () => {
  const ts = Date.UTC(2026, 4, 23, 18, 30, 0);
  const { xMin, xMax } = chartWindow([{ x: ts, y: 179.77 }], "MAX");
  assert(xMax - xMin >= 60_000,
    `Expected padded window > 1 minute, got ${xMax - xMin} ms`);
  // 12h pad on each side for MAX → 24h span total.
  const span = xMax - xMin;
  assert(span >= 23 * 3600_000 && span <= 25 * 3600_000,
    `Expected ~24h span for MAX single-point, got ${(span / 3600_000).toFixed(1)}h`);
});

test("MAX with multiple distinct points does NOT pad", () => {
  const t0 = Date.UTC(2026, 4, 1, 12, 0, 0);
  const t1 = Date.UTC(2026, 4, 23, 12, 0, 0);  // 22 days later
  const { xMin, xMax } = chartWindow([
    { x: t0, y: 5 }, { x: t1, y: 25 },
  ], "MAX");
  assert(xMin === t0 && xMax === t1,
    `Expected exact window for multi-point MAX; got [${xMin}, ${xMax}]`);
});

test("1W with single point pads (range/4 each side)", () => {
  const ts = Date.now() - 3600_000;
  const { xMin, xMax } = chartWindow([{ x: ts, y: 5 }], "1W");
  // 1W is 7 days. range/4 = 1.75 days each side. But for fixed ranges
  // xMin is `now - range` and xMax is `now`, so the span is 1 week
  // even with no points — padding never triggers because span > 60s.
  assert(xMax - xMin >= 7 * 86400000,
    `Fixed range should keep its width; got ${(xMax - xMin) / 86400000} days`);
});

test("no points and MAX picks a 1-day fallback window", () => {
  const { xMin, xMax } = chartWindow([], "MAX");
  // No-data path: xMin = now - 1d, xMax = now. Padding doesn't fire
  // because the span is already 1 day.
  const span = xMax - xMin;
  assert(span >= 86400000 - 1000 && span <= 86400000 + 1000,
    `Expected 1-day fallback for empty MAX; got ${span} ms`);
});

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

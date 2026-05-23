// Frontend smoke test for the sandbox client-side bootstrap.
//
// The browser code lives in web/static/app.js — too large to import
// wholesale into Node (it touches `window`, `document`, fetch, etc.).
// We grep out just the bootstrap helpers (_mulberry32, _pct,
// _bootstrapSandbox, _fmtCI), eval them in an isolated scope, and
// exercise each invariant we care about:
//
//   1. Bootstrap CI is null when there are <2 bet slips.
//   2. CI is deterministic — same input twice → identical bounds
//      (because the PRNG is seeded with a constant).
//   3. CI brackets the empirical ROI / win-rate / max-drawdown.
//   4. A clearly-positive strategy (every slip wins) has a CI lower
//      bound > 0 — the floor of "is this strategy actually profitable"
//      that the user looks at.
//   5. A clearly-noisy strategy (50% wins, even payouts) has a CI that
//      straddles zero — the "don't trust this number" signal.
//   6. _fmtCI handles null gracefully and renders ± correctly.
//
// Run with: node tests/frontend/test_sandbox_bootstrap.mjs
// Returns non-zero exit code on any failure (catches CI breakage in
// a future refactor of the inline helpers).

import fs from "node:fs";
import path from "node:path";
import url from "node:url";

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
const APP_JS = path.join(__dirname, "..", "..", "web", "static", "app.js");

const src = fs.readFileSync(APP_JS, "utf8");

// Extract each helper by name. We slice from the function header up to
// the matching close brace at column 0. The helpers are top-level
// `function NAME(...)` declarations so the brace counter terminates
// cleanly without needing a full JS parser.
function extractFunction(name) {
  const re = new RegExp(`^function\\s+${name}\\s*\\(`, "m");
  const m = src.match(re);
  if (!m) throw new Error(`Could not find helper "${name}" in app.js`);
  const start = m.index;
  let depth = 0;
  let i = src.indexOf("{", start);
  if (i < 0) throw new Error(`No opening brace for "${name}"`);
  depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return src.slice(start, i);
}

function extractConst(name) {
  const re = new RegExp(`^const\\s+${name}\\s*=\\s*([^;\\n]+);`, "m");
  const m = src.match(re);
  if (!m) throw new Error(`Could not find const "${name}" in app.js`);
  return `const ${name} = ${m[1]};`;
}

const bundle = [
  extractConst("_SB_BOOTSTRAP_RESAMPLES"),
  extractConst("_SB_BOOTSTRAP_SEED"),
  extractFunction("_mulberry32"),
  extractFunction("_pct"),
  extractFunction("_bootstrapSandbox"),
  extractFunction("_fmtCI"),
  // Export hooks so the test scope can read them.
  "return { _bootstrapSandbox, _fmtCI, _mulberry32, _pct };",
].join("\n\n");

// Run inside its own scope to avoid leaking onto the test runner.
const { _bootstrapSandbox, _fmtCI, _mulberry32, _pct } =
  new Function(bundle)();

// ── Test runner ───────────────────────────────────────────────────────
let pass = 0, fail = 0;
function test(name, fn) {
  try {
    fn();
    console.log(`  ✓ ${name}`);
    pass++;
  } catch (e) {
    console.log(`  ✗ ${name}\n      ${e.message}`);
    fail++;
  }
}
function assert(cond, msg) {
  if (!cond) throw new Error(msg || "assertion failed");
}
function assertNear(actual, expected, tol, msg) {
  if (Math.abs(actual - expected) > tol) {
    throw new Error(`${msg || "near"}: expected ~${expected}, got ${actual}`);
  }
}

console.log("sandbox bootstrap helpers:");

// ── 1. CI is null on degenerate input ─────────────────────────────────
test("returns null when zero bet slips", () => {
  assert(_bootstrapSandbox([], 100) === null);
});
test("returns null when only one bet slip (cannot bootstrap)", () => {
  assert(_bootstrapSandbox([
    { bet_size: 1, payout: 6, profit: 5 },
  ], 100) === null);
});

// ── 2. Deterministic across repeated calls ────────────────────────────
test("identical input → identical CI (seeded PRNG)", () => {
  const slips = Array.from({ length: 40 }, (_, i) => ({
    bet_size: 1,
    payout: i % 6 === 0 ? 6 : 0,
    profit:  i % 6 === 0 ? 5 : -1,
  }));
  const a = _bootstrapSandbox(slips, 100);
  const b = _bootstrapSandbox(slips, 100);
  assert(a !== null && b !== null);
  assertNear(a.roi_pct.lo, b.roi_pct.lo, 1e-9, "lo bound diverged");
  assertNear(a.roi_pct.hi, b.roi_pct.hi, 1e-9, "hi bound diverged");
  assertNear(a.win_rate_pct.lo, b.win_rate_pct.lo, 1e-9);
  assertNear(a.win_rate_pct.hi, b.win_rate_pct.hi, 1e-9);
});

// ── 3. CI brackets the point estimate ─────────────────────────────────
test("CI brackets the empirical ROI of the input slips", () => {
  // 100 slips, 20 wins paying 6x at 1u stake. ROI = (20*6 - 100) / 100 = 20%.
  const slips = Array.from({ length: 100 }, (_, i) => ({
    bet_size: 1,
    payout: i < 20 ? 6 : 0,
    profit: i < 20 ? 5 : -1,
  }));
  const ci = _bootstrapSandbox(slips, 100);
  assert(ci !== null);
  // Empirical ROI = 20%. The 95% bootstrap CI must straddle it.
  assert(ci.roi_pct.lo <= 20.0 && ci.roi_pct.hi >= 20.0,
    `ROI band [${ci.roi_pct.lo}, ${ci.roi_pct.hi}] does not include 20%`);
});

// ── 4. Clearly profitable strategy → lower bound > 0 ──────────────────
test("every-slip-wins → ROI CI lower bound is positive", () => {
  // 60 slips, every one wins. ROI = (60*6 - 60)/60 = 500%.
  const slips = Array.from({ length: 60 }, () => ({
    bet_size: 1, payout: 6, profit: 5,
  }));
  const ci = _bootstrapSandbox(slips, 100);
  assert(ci !== null);
  assert(ci.roi_pct.lo > 0,
    `Expected ROI lo>0 for a strategy that always wins; got [${ci.roi_pct.lo}, ${ci.roi_pct.hi}]`);
});

// ── 5. Noisy strategy → CI straddles zero ────────────────────────────
test("breakeven coin-flip strategy → ROI CI straddles 0", () => {
  // 30 slips, 1u stake, payout=2u when win (so net 0 in expectation
  // — half win, half lose). Empirical ROI floats near 0 with high
  // variance on n=30 — CI must include 0.
  const slips = Array.from({ length: 30 }, (_, i) => ({
    bet_size: 1,
    payout: i % 2 === 0 ? 2 : 0,
    profit: i % 2 === 0 ? 1 : -1,
  }));
  const ci = _bootstrapSandbox(slips, 100);
  assert(ci !== null);
  assert(ci.roi_pct.lo <= 0 && ci.roi_pct.hi >= 0,
    `Expected ROI CI to straddle 0 on a coin-flip strategy; got [${ci.roi_pct.lo}, ${ci.roi_pct.hi}]`);
});

// ── 6. _fmtCI ─────────────────────────────────────────────────────────
test("_fmtCI renders null as empty string", () => {
  assert(_fmtCI(null, "%") === "");
  assert(_fmtCI({ lo: null, hi: 5 }, "%") === "");
  assert(_fmtCI({ lo: 5, hi: null }, "%") === "");
});
test("_fmtCI renders positive bounds with + sign", () => {
  assert(_fmtCI({ lo: 2.34, hi: 7.89 }, "%") === "95% CI [+2.3%, +7.9%]");
});
test("_fmtCI renders negative bounds with - sign", () => {
  assert(_fmtCI({ lo: -3.45, hi: 1.23 }, "%") === "95% CI [-3.5%, +1.2%]");
});

// ── 7. _pct edge cases ────────────────────────────────────────────────
test("_pct on empty array returns null", () => {
  assert(_pct([], 0.5) === null);
});
test("_pct returns the right quantile", () => {
  assertNear(_pct([10, 20, 30, 40, 50], 0.5), 30, 0, "median wrong");
  assertNear(_pct([10, 20, 30, 40, 50], 0), 10, 0, "min wrong");
  assertNear(_pct([10, 20, 30, 40, 50], 1.0), 50, 0, "max wrong");
});

// ── 8. _mulberry32 produces values in [0, 1) ──────────────────────────
test("mulberry32 yields uniform-looking values in [0, 1)", () => {
  const r = _mulberry32(123);
  for (let i = 0; i < 100; i++) {
    const v = r();
    assert(v >= 0 && v < 1, `value out of range: ${v}`);
  }
});

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);

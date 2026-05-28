# Sandbox design report

How the Sandbox tab (`web/static/page-sandbox.jsx`) was wired to live data,
and the three correctness fixes that went with it.

## Goal

Turn the Sandbox from a client-side mock (random equity curve, fabricated
slip log) into a thin view over the real strategy simulator
(`engine/strategy_tester.StrategyTester`), reachable through
`POST /api/sandbox/run`.

## Architecture

The page is a pure presentation layer over the backend simulator. All the
math — replaying `market_observatory`, calibration, push/DNP-aware payouts,
Kelly sizing, bootstrap CIs — already lives server-side and is the same
code the live backtest uses. The frontend's only jobs are:

1. **Build the request** from the controls (`sbBuildRequest`). `min_prob` is
   sent as a fraction; `bet_size` is fixed at `1.0` (1u); `slip_strategy`,
   `bootstrap`, and date range fall back to the backend defaults
   (`live_replay`, `true`, full history).
2. **Call the endpoint** via `window.cpApi.apiFetch` (handles Supabase JWT
   auth and JSON). One call per "Run Simulation" click, plus one auto-run on
   mount.
3. **Map the response** (`sbMapResult`) into the shapes the cards, chart, and
   slip log consume. Nothing is recomputed client-side — the numbers on
   screen are exactly what the simulator produced.

This keeps a single source of truth. The server already caches per-config
results (calibration-aware invalidation), so identical re-runs are cheap and
the client needs no cache of its own.

### Response → UI mapping

- `summary.{total_slips,total_profit,roi_pct,win_rate_pct,max_drawdown_pct}`
  → the five stat cards. `summary.ci` (bootstrap 95% bands) renders as a
  sub-label under ROI and Win Rate via `sbFmtCI`.
- `equity_curve` carries **cumulative profit**; the chart shows **bankroll**,
  so each point becomes `startBank + cumProfit` (`startBank = summary.bankroll`,
  default 100).
- `slips[]` → the slip log rows (legs, hits/n_eff, stake, payout, P/L).

### Chart windowing

The range pills (1D…MAX) filter the equity series by time
(`sbFilterByRange`), anchored to the **last data point** rather than `now` —
replay history can be weeks old, so anchoring to wall-clock would blank the
chart. A window that would exclude everything falls back to the final point,
so the chart never renders empty.

### States

`idle → loading → ok | error`. The backend returns a `400` with a
human-readable `detail` when filters leave nothing to simulate (e.g. "Raise
Min True Prob to at least 54.07%"); `apiFetch` surfaces that message verbatim
in a red banner, so the error tells the user how to fix their config.

## The three fixes shipped alongside the wiring

1. **Break-even numbers were wrong.** The mock hard-coded values like
   Power-2 = 60% and a full Flex-2 row. The real per-leg break-evens live in
   `engine/constants.py::BREAK_EVEN` (e.g. Power-2 = 57.74% = (1/3)^(1/2),
   Power-6 = 54.07%). `SB_BREAK_EVEN` now mirrors that table exactly, and a
   test parses `constants.py` to assert they never drift.

2. **2-leg Flex is not a real product.** A 2-leg flex degenerates to a 2-leg
   power, and `BREAK_EVEN` has no `("2","flex")` entry. The slip-size dropdown
   is now derived from the break-even table per type (`sbAllowedSizes`):
   Power = 2–6, Flex = 3–6. Switching to Flex while on a 2-leg slip auto-bumps
   to 3 legs. This matches the backend, which already rejects 2-leg flex.

3. **League column removed** from the slip log per request.

## Testing

- `tests/frontend/test_sandbox_live.mjs` (new, 17 cases): extracts the
  JSX-free helper block from `page-sandbox.jsx` and tests break-even parity
  with `constants.py`, no-2-leg-flex, request building, equity/slip mapping,
  range windowing, and CI formatting.
- `tests/engine_tests/test_sandbox_ci_wiring.py`: kept the backend
  `_bootstrap_metrics` shape/units tests (the contract the UI consumes) and
  removed the obsolete DOM-contract half that asserted against the
  vanilla-JS `app.js` deleted in the React migration.
- Removed `tests/frontend/test_sandbox_bootstrap.mjs` and
  `test_sandbox_chart_window.mjs` — both read the deleted `app.js` and
  tested logic that no longer exists client-side (CI now comes from the
  server). They were already failing on collection.
- Full suite: `161 passed, 15 skipped` (pre-existing skips). JSX verified to
  transpile under `@babel/preset-react`.

# Frontend modules

`app.js` is now an ES module (`<script type="module">`). New code should
live here, one logical area per file, rather than in `app.js`. Existing
code stays in `app.js` for now; lift sections out as they're touched.

## What's already extracted

- `constants.js` — payout tables (Power/Flex), break-even map, league
  order, MIN observation prob. Pure values, no DOM, no fetch.
- `formatters.js` — `fmt.dollar`, `fmt.pct`, `escapeHtml`. Pure.
- `per_user_cache.js` — `PER_USER_LOCALSTORAGE_KEYS` and the purge helper.
  Imported by the auth-change reset flow in `app.js`.

## Migration plan for the rest

Each tab module should expose:

```js
export function init(root) { ... }      // wire up handlers once
export function show() { ... }          // called on tab activation
export function hide() { ... }          // optional, for cleanup
```

`app.js` keeps the tab-click router (it owns the DOM containers and the
auth guard); it just calls `import("./modules/sandbox.js").then(m => m.show())`
or imports synchronously at module top.

Suggested next extractions, in order of independence:

1. `sandbox.js` — already self-contained; just lift the existing
   `initSandbox` / `runSandbox` / `renderSandboxResults` / range toggle.
2. `observatory.js` — single API call, single render path.
3. `backtest.js` — has localStorage caching and pagination but is
   otherwise decoupled.
4. `analytics.js` — calibration plot + Brier monitor.
5. `slip_builder.js` — the +EV slip-construction UI.
6. `state.js` — the shared store (`currentSession`, `state.allBets`).
   Should be a single source of truth with subscribe semantics; the
   `_renderedUserId` reload-on-change patch becomes opt-in.

Once steps 1–6 land, `app.js` is mostly the tab router and the auth
flow. At that point a framework migration (SvelteKit / Lit) is a 1-week
project rather than a 3-week rewrite.

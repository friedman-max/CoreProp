# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the app (uvicorn; APScheduler inside web/app.py drives auto-refresh)
python main.py                     # -> http://127.0.0.1:8000

# Tests
python -m pytest tests/ -q                              # full suite (~2s, no network)
python -m pytest tests/engine_tests/test_devig.py -q     # one file
python -m pytest tests/ -q -k "consensus and not weight" # one test by name
python -m pytest tests/ -q -m "not slow"                 # markers: slow, integration

# Frontend build — REQUIRED before committing any .jsx change
./build.sh                         # esbuild web/static/*.jsx -> web/static/dist/*.js

# Deps
pip install -r requirements.txt        # prod
pip install -r requirements-dev.txt    # + pytest (CI uses this)
```

There is no linter or formatter configured. CI (`.github/workflows/tests.yml`) runs
only pytest on Python 3.11 with stubbed Supabase env vars.

### The `.jsx` → `dist/` build contract

Render's build env is **pip-only (no node)**, so it cannot run `./build.sh`.
`web/static/dist/*.js` is committed and served directly (`.gitignore` explicitly
un-ignores it). Editing a `.jsx` without re-running `./build.sh` and committing
`dist/` ships a no-op to production. `build.sh`'s `FILES` array order must match
the `<script>` order in `web/static/index.html` — the bundles are plain global
scripts (no module wrapper), so cross-file globals depend on load order.
Adding or removing a `.jsx` means editing **both** lists plus `dist/`.

### Design tokens

`:root` in `index.html` is the whole palette; there is no second source. Two
rules that are load-bearing and easy to break silently:

- **`--primary` is fill/border only, `--primary-2` is text only.** `--primary`
  (`#1E6FB0`) is dark enough that white button labels clear WCAG AA;
  `--primary-2` (`#6FBCEC`) is bright enough to read on the page but must never
  have white on it. Hover *darkens*. `--text-4` is 2.3:1 and is for
  disabled/decorative glyphs — never for text a user reads.
- **`TWEAK_DEFAULTS.accent` in `app-main.jsx` must equal `--primary`.** An
  effect writes it back onto `document.documentElement` as an *inline* style,
  which beats the stylesheet, so a stale value there overrides the token
  everywhere and the only symptom is that the CSS "doesn't work".

There are no gradients on accent surfaces, no blurred decorative orbs, and no
gradient-clipped text anywhere by deliberate choice — those were removed, and
re-adding one is a visual regression, not a flourish.

`:root` also carries four **scales**, and values in this stylesheet are expected
to resolve through them rather than being typed: radii `--r-xl/lg/md/sm/pill`
(with `--radius` / `--radius-sm` as aliases), spacing `--s-1` … `--s-12` (4px
steps — note there is deliberately **no `--s-7`, `--s-9` or `--s-11`**, so don't
"complete" the sequence), elevation (`--shadow-card`, `--shadow-pop`, `--ring`),
and two derived layout vars (`--row-px` for app row gutters, `--lp-px` for the
marketing gutters). `--primary-lo` (.10) is the row-level accent tint and
`--primary-hi` (.22) is for rings and badges: `.22` behind a whole row is
*inherited* by muted text, which then measures 4.45:1 and fails AA, so those two
are not interchangeable.

Three invariant tests own this contract, and they are worklist generators as much
as guards — run one, get every off-scale site, fix, re-run:
`tests/api_tests/test_css_tokens.py` (the scales, flat surfaces, the `--text-4`
allowlist), `test_css_guards.py` (no accent gradient / orb / gradient-text, and
the three-copy accent above), `test_build_stamp.py` (the `?v=` cache-bust
digest). Their exemption sets name **whole selectors, never prefixes**, and each
entry carries a written reason — an exemption justified by a property the test
does not inspect is not an exemption, it is a hole (that mistake shipped once).

Two CSS facts that have caused silent no-ops here, both worth checking *before*
you conclude an edit didn't work:

- **Most rules are declared twice.** There is a base rule and an unconditional
  copy in the later "density overrides" block, and **the later copy wins** by
  source order. Editing only the base ships nothing. `grep -n` for every copy of
  a selector before changing it.
- **`:hover:not(:disabled)` is (0,3,0)**, because the `:disabled` inside `:not()`
  counts. Doubling a class only reaches (0,2,0) and loses — a state rule that
  must beat a hover needs its own `:hover:not(:disabled)`. Relatedly, a
  declaration **on** an element always beats an inherited value regardless of
  ancestor specificity, so a colour "fix" applied to a parent is a no-op if the
  child sets its own `color`.

## Architecture

Single-process FastAPI app. One APScheduler `BackgroundScheduler` runs a scrape
pipeline every `REFRESH_INTERVAL_MINUTES`; all read endpoints serve
pre-serialized bytes from an in-memory cache. Supabase (PostgREST) is the only
persistence.

### The scrape pipeline (`web/app.py::_run_pipeline_body`, ~900 lines)

1. Five scrapers run concurrently in a `ThreadPoolExecutor` (PrizePicks,
   FanDuel, DraftKings, Pinnacle, Novig). Each returns dataclasses from
   `engine/matcher.py`.
2. **Bail-out guards**: if *all* scrapers return 0, or if PrizePicks alone
   returns 0, the pipeline preserves the previous state and returns. PP is the
   source of truth for what gets matched — wiping it wipes the match graph.
3. `engine.matcher.match_props` fuzzy-matches players/props across books
   (`FUZZY_THRESHOLD = 91`), including half-step equivalents (over PP-3 ==
   over book-3.5 under push-on-tie).
4. `engine.consensus.compute_true_probability` devigs each book and returns
   `(consensus_prob, worst_case_prob, meta)`. **The pipeline gates on
   `consensus_prob`**, not `worst_case_prob` — see "The decision number" below.
5. `engine.ev_calculator.BetResult.__init__` applies decision corrections and
   computes per-leg EV. Legs above `MIN_DISPLAY_PROB` become bets.
6. State is written under `_lock`, then `_refresh_payload_cache` serializes each
   dataset to JSON bytes once. Book Python lists are then *dropped* from
   `_state` (the bytes are authoritative) to halve RSS.
7. Background daemon threads fire: observatory logging, CLV closing lines,
   ESPN result resolution, per-user auto-backtest slip logging.

`run_pipeline()` is a thin wrapper that exists only so all large per-cycle
locals go out of scope before a forced `gc.collect()`.

### Shared state (`web/state.py`)

`_state`, `_payload_cache`, `_pending_slips`, `_analytics_cache` each have their
own lock. Rules that matter:

- Never rebind these names (`_state = {...}`) — other modules import the
  binding. Mutate in place.
- Multi-field reads of `_state` must hold `_lock`.
- Hold one lock at a time; never nest.
- New routers import from `web.state`, never from `web.app` (import cycle).

### Router split (in progress)

`web/app.py` is ~3,700 lines. `web/routers/admin.py` and `web/routers/public.py`
have been extracted. The convention (documented in `web/routers/__init__.py`)
is: one router module per endpoint group, `router` attribute, no
router-to-router imports, mounted at the bottom of `web/app.py`. Lift a group
when you touch it.

### Frontend

React 18 + Supabase JS from CDN, no bundler beyond esbuild's JSX transform, no
module system — every `.jsx` file defines globals consumed by later files.
`window.cpApi` (`api.jsx`) owns Supabase auth and the authenticated fetch/SWR
layer. Tabs load lazily: `/api/bootstrap/core` first-paints bets + meta, then
each tab fetches its own dataset on first visit.

**The marketing pages state no number they can't source.** `landing.jsx` and
`pricing.jsx` are the signed-out surface, and every figure on them is either
read from `GET /api/public/coverage` (books, leagues, refresh interval, trial
length — via the shared `useCoverage()` hook in `components.jsx`), derived from
`engine/constants.py` by the same formula the app uses (the per-leg break-even),
or absent. This is enforced: `tests/api_tests/test_landing_claims.py` bans the
specific invented statistics that used to ship there ("58.4% backtested hit
rate", "4,200+ sharps", "+11.7% ROI", "refreshed every 30 seconds", fabricated
"Yesterday's Winners", named testimonials nobody said), bans naming any league
absent from `config.ACTIVE_LEAGUES`, bans hardcoding the trial length (it was
`7` in six places while `BILLING_TRIAL_DAYS` is env-configurable), and requires
the break-even to be *computed* from `POWER_PAYOUTS[6]` rather than typed — the
landing page had gone stale at the pre-37.5x `54.07%` exactly the way
`test_payout_table_mirror.py` describes. If a section looks empty, add a
coverage fact or leave it empty; don't invent a metric. A real performance stat
needs settled `market_observatory` rows behind it first.

Two copy rules that came out of the same pass, both easy to undo by accident:

- **Count sources with `coverage.books_noun`, not the word "books".** Novig is a
  peer-to-peer exchange, so "4 books" is wrong whenever `NOVIG_ENABLED` is on.
  The endpoint returns the correct noun for the current set; interpolate it.
- **Quote the price that gets charged.** The pricing page used to render `$50/mo`
  as a 72px "$1.67" (the monthly divided by 30) with the real figure in 12px
  grey underneath. Per-day framing of a subscription is a minimization tactic;
  the per-month equivalent on the *annual* plan is a real comparison and stays.

Disclaimers are load-bearing but not free: one legal line in the footer, one
"Worth knowing" section, one FAQ answer about the missing track record. Saying
it four times reads as defensiveness and buries the one place it matters.

## Conventions specific to this codebase

**Payout tables are mirrored in five places.** `engine/constants.py`
(`POWER_PAYOUTS`, `FLEX_PAYOUTS`, `BREAK_EVEN`), `web/static/ev-page.jsx`
(`EV_POWER_PAYOUTS`, `EV_FLEX_PAYOUTS`), `web/static/page-backtest.jsx`
(`BT_POWER_PAYOUTS`, `BT_FLEX_PAYOUTS`), and the two compiled bundles in
`web/static/dist/` must stay identical. PrizePicks lowered 6-Power from 40x to
37.5x; break-evens move with the table. There is no 2-leg Flex (it degenerates
to 2-leg Power and has no `BREAK_EVEN` entry) — both frontends short-circuit
`n === 2` to the Power-2 payout.

`tests/engine_tests/test_payout_table_mirror.py` enforces all of it: it parses
the tables out of the `.jsx` *and* the committed `dist/*.js` (so a `.jsx` edit
without `./build.sh` fails), and re-derives `BREAK_EVEN` from the payouts
(closed form for Power, bisection for Flex) so a payout change with a stale
break-even fails too. Change a payout and this test tells you every place that
still disagrees.

**`raw_true_prob` vs `true_prob` — one ruler.** `raw_true_prob` is the untouched
market consensus and is what CLV, the observatory training corpus, and every
refit measure against. `true_prob` is the *decision* number: raw plus
corrections. Never write a correction back into `raw_true_prob`.

**Corrections are env-killable and default to the validated state.** In
`config.py`: `SIDE_BIAS` (additive per league/side), `CELL_DROPS` (banned
league/side cells), `CALIBRATION_MAP_ENABLED` (isotonic map), and
`CONSENSUS_WEIGHTS_ENABLED` (per-book weighting) all have explicit reasons for
their current default written inline. Several default OFF *deliberately* —
their fits failed out-of-sample sign-stability, or they were fit on a different
"ruler" than the live thresholds. `analysis/CALIBRATION_RUNBOOK.md` is the
process for changing any of them: fit via `analysis/12_side_bias_refit.py`,
review, paste into `config.py` by hand. Never auto-write these tables, and never
lower `AUTO_SLIP_MIN_PROB_FLOOR` without a fresh fit.

**Dedup is layered and has broken silently before.** `engine/dedup.py` runs a
post-emit invariant check on the actual emitted slips rather than trusting the
greedy builder's `used_pair`/`used_leg` sets. Keys are built through
`engine.backtest.make_bet_key` / `make_leg_key`, keyed on `sports_day` (UTC date
of `game_start − 12h`, so a late ET game doesn't straddle midnight). Add call
sites through those helpers, not by hand.

**Per-call Supabase clients, on purpose.** `engine/database.py` builds a fresh
`SyncPostgrestClient` per call. A shared connection pool was tried and reverted
(commit `7f052d6`): under `--workers 1 --threads 1` plus the scraper pool and
four daemon threads, one poisoned pooled connection made writes silently no-op
because callers swallow exceptions broadly. Don't reintroduce the pool.

**Writes should go through `engine/writer.py`.** `writer("purpose")` wraps the
service-role client to log table/row-count/caller for every mutation, and is the
seam for a future API/writer pod split. New write paths use it; reads stay on
`get_db()`. Old call sites migrate opportunistically.

**Memory is a hard constraint (Render 512 MB free tier).** `render.yaml` carries
the malloc tuning (`MALLOC_ARENA_MAX=1`, trim/mmap thresholds, `PYTHONMALLOC=malloc`)
and `--workers 1 --threads 1 --max-requests 1000`. `gc.set_threshold(500, 5, 5)`
at the top of `web/app.py`. Strings for league/stat_type/side are `sys.intern`ed.
Before adding an allocation to the request path or holding a dataset in `_state`,
check `GET /api/admin/memory`. The `--max-requests` value is tuned so a worker
recycle is unlikely to land in the POST→GET window of an in-memory pending slip.

**Comments carry decision history.** Many inline comments cite the commit or
audit that produced a value and explain why the obvious alternative was
rejected. Read them before "simplifying" a constant or a default — most were
paid for with a production regression.

## Env vars

Only the four `SUPABASE_*` values in `.env.example` are required; everything else
has a working default in `config.py`. For local work against a shared Supabase
project, set `DISABLE_PERSISTENCE=true` and `DISABLE_AUTO_BACKTEST=true` so the
instance doesn't clobber the production seed snapshot or log slips.

Not in the README: `LOCAL_AUTO_BACKTEST_USER_IDS` (comma-separated user IDs that
auto-log regardless of their `auto_backtest` flag — pairs with
`ENABLE_ADMIN_TRIGGERS=true` and `POST /api/admin/trigger-auto-backtest` to make
localhost the exclusive logger), `MAX_AUTO_SLIPS_PER_CYCLE` (default 10).

## Database

Supabase Postgres with RLS. `migrations/migration_0NN.sql` are the source of
truth and historical record — apply in order to an existing project.
`migrations/schema.sql` is the flattened equivalent for a fresh project; keep it
in sync when adding a migration. `auth.users` / `auth.identities` are
Supabase-managed and not defined in any migration.

**Every table must have RLS enabled** — `tests/engine_tests/test_rls_coverage.py`
parses `schema.sql` and fails if a `create table` has no matching
`enable row level security`. This is not optional hardening: `SUPABASE_ANON_KEY`
is published to the browser (`web/app.py::root()` injects it into `index.html`;
`GET /api/ui-config` serves it), so PostgREST is directly reachable by any
visitor and FastAPI is *not* the only path to the data. Tiers:

- `slips`, `legs`, `user_config` — owner policy, `user_id = auth.uid()`
- `market_observatory` — deliberate public-read (`for select using (true)`), no
  write policy
- `app_state_cache`, `calibration_cells`, `calibration_history`,
  `strategy_performance_compare` — RLS with **no policy** (deny-all for
  anon/authenticated) plus revoked grants, migration_018. The service-role
  client bypasses RLS, so server-side writers are unaffected.

**Isolation is two layers, and the app layer is one of them.** User-scoped reads
pass an explicit `.eq("user_id", ...)` *in addition to* relying on RLS — see
`get_backtest_slips`, `delete_backtest_slip`, and the loaders in
`engine/calibration.py` (which take a `user_id` argument alongside `user_jwt`).
Do not drop those filters on the grounds that "RLS already handles it": RLS is a
single remote config away from being off, and a service-role client bypasses it
entirely. (`SUPABASE_ANON_KEY` used to *fall back* to the service key when
unset, which made that a one-env-var accident; `engine/database.py` now raises at
import instead, and `tests/api_tests/test_anon_key_exposure.py` pins it. The
app-layer filters stay regardless — they're the second layer.)
`delete_backtest_slip`'s ownership check is only an ownership check *because* of
the `user_id` filter — without it, the SELECT merely proves the row is visible.
`tests/api_tests/test_tenant_isolation.py` pins this by simulating an
RLS-disabled database.

Background workers (`engine/results_checker.py`, `engine/clv_checker.py`) are
intentionally cross-user and use the service-role `get_db()` — do not add
`user_id` filters there.

Artefacts refit hourly by the scheduler land in `data/` (`correlation_map.json`,
`calibration_map.json`) with a Supabase `app_state_cache` mirror. `data/` is
gitignored apart from `.gitkeep`.

## Removed subsystems

The Sandbox tab (`web/static/page-sandbox.jsx`) and the strategy simulator
(`engine/strategy_tester.py`) were removed in simplify-v1 (`71c1091`), along
with `POST /api/sandbox/run`. `SANDBOX_DESIGN.md` and
`tests/frontend/test_sandbox_live.mjs` documented and tested them and have been
deleted too. Inline comments in `engine/dedup.py`, `engine/constants.py`,
`migrations/migration_007.sql` and `migration_009.sql` still name
`strategy_tester` / the sandbox to explain why a field alias, a constant, or a
column exists — those are decision history, not live references. The columns
`migration_009` added (`first_seen_at` / `last_seen_at`) are still part of
`schema.sql` and are still maintained by the `market_observatory` BEFORE UPDATE
trigger (migration_017), so don't drop them because the sandbox is gone — no
Python reads them today, but the trigger and both indexes are live.

Payout-table drift is the one thing that subsystem's test suite covered and
nothing else did; `tests/engine_tests/test_payout_table_mirror.py` now owns
that contract in pytest (see below).

`web/static/data.jsx` (`EV_BETS`, `TICKER_PLAYS`, `YESTERDAY_WINS`) held the
fabricated demo rows the old landing page rendered as if they were real results.
It was deleted along with those sections — nothing else ever imported it. Its
entries are gone from `build.sh`'s `FILES` array and from `index.html`'s script
list. The landing page has no sample data now; it shows one clearly-labelled
worked example whose arithmetic it computes.

## Commit style

Conventional-commit prefixes where scoped (`fix(backtest):`, `feat(extension):`),
plain imperative sentences otherwise. Subject lines state the user-visible
outcome, not the mechanism.

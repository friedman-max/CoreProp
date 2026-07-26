# Migration status — live Supabase project

**As-of:** 2026-07-26, immediately after `migration_018.sql` was applied.
**Method:** static analysis of the repo (grep of every `.table("…")` call site and
every SQL object) cross-referenced against the one hard runtime signal we have:
migration_018's own `raise notice` output.

---

## TL;DR — no live code path is broken

**There is no bug.** No file under `web/` or `engine/` references
`calibration_cells`, `calibration_history`, or `strategy_performance_compare` —
not in the request path, not in the APScheduler pipeline, not in the four daemon
threads, not in the frontend. The three missing tables are read and written
**only** by three offline scripts under `analysis/`, two of which cannot even
import on the production image (they need the `supabase` package, which is not in
`requirements.txt`).

Specifically resolving the `engine/calibration_map.py` concern raised in the
investigation brief: the hourly isotonic refit does **not** touch
`calibration_cells`. It reads `market_observatory` and writes
`data/calibration_map.json` + the `app_state_cache` Supabase mirror. The hourly
job in `web/app.py::_run_periodic_models` is **not** failing. Evidence is in
§3 below.

The only documentation defect found is cosmetic: `migration_010b_rwbc.sql`'s
header cites `engine/rwbc_calibration.calibrate()` and a
`GET /api/observatory/rwbc` endpoint, both of which were deleted in commit
`71c1091` ("Simplify CoreProp to a conservative line-comparison tool",
2026-06-20). Reading that header today implies a live consumer that does not
exist. See §5 for the recommendation.

---

## 1. Migration-by-migration verdict

| Migration | Objects it creates | Verdict | Confidence | Evidence |
|---|---|---|---|---|
| pre-001 base | `slips`, `legs`, `app_state_cache` | **APPLIED** | High | migration_018 secured `app_state_cache` (did not skip it). `slips`/`legs` are read and written on every pipeline cycle and the backtest tab works. |
| 001 | `user_id` on slips/legs, `user_config`, RLS + owner policies | **APPLIED** | High | The whole multi-tenant app depends on it; `GET /api/backtest/slips` filters `.eq("user_id", …)` and returns rows. |
| 002 | `market_observatory` + public-read policy | **APPLIED** | High | Observatory upsert runs every cycle (`web/app.py:1276`) and the hourly calibration fit reads settled rows from it. |
| 003 | `market_observatory.closing_prob`, `.books` | **APPLIED** | High-ish | `engine/clv_checker.py:301` `select("… closing_prob, books")` — PostgREST 400s on an unknown column in a select list, and the CLV close pass is reported as working. |
| 004 | `user_config.auto_slip_type / auto_slip_legs / auto_slip_min_prob` + check constraints | **APPLIED** | High | The slip-prefs endpoint upserts these three columns with **no** column-missing fallback (`web/app.py:2232-2247`); a missing column would 500 the endpoint. |
| 005 | `resolved_at` on observatory + legs, backfill, indexes | **APPLIED** | Medium-High | `engine/results_checker.py:222/247` writes `resolved_at` in an unguarded `.update()`; a missing column would raise into the `except` and log "ResultsChecker DB update failed" on *every* grade. |
| 006 | `raw_true_prob` + `market_width` on observatory, `raw_true_prob` on legs, backfill | **APPLIED** | High | The entire "one ruler" architecture (`raw_true_prob`) depends on it; the hourly calibration fit selects `raw_true_prob` (`engine/calibration_map.py:198`). |
| 007 | `team` on observatory + legs, indexes | **APPLIED** | Medium | Written on the leg-insert path; has a strip-and-retry fallback (`_OPTIONAL_LEG_COLS`), so a miss would be silent. PP's two-team rule works, which needs `team`. |
| 008 | `legs.dedup_key` + partial unique index | **APPLIED** | Medium-High | The cross-slip dedup relies on Postgres raising 23505; `engine/backtest.py` comments describe that index firing in production. |
| 009 | `first_seen_at` / `last_seen_at` + `market_observatory_upsert_guard` trigger | **APPLIED** | High | The trigger's misbehaviour is directly observed and worked around in `engine/results_checker._write_observatory_result` (DELETE + re-INSERT). That workaround only makes sense if the trigger exists. |
| 010 | Stripe columns on `user_config` + index | **APPLIED** | Medium | `_read_user_billing` selects all four in one `select()`; a missing column would 400 and log `billing: read user billing failed` on every gate check. |
| **010b_rwbc** | **`calibration_cells`, `calibration_history` + 2 indexes** | **MISSING (confirmed)** | **Certain** | migration_018 reported both as SKIPPED — its `to_regclass('public.<t>') is null` branch fired. |
| 011 | `auto_slip_min_prob` default → 0.60 + one-time bump | **UNKNOWN** | Low | Pure default change + one-time UPDATE. No code reads the column *default* (the app always sends an explicit value), so there is no runtime signal either way. Cost of being un-applied: brand-new users who never open the slip builder inherit the legacy 0.5407 floor instead of 0.60 — a real but low-blast-radius behaviour difference. |
| 012 | `closing_captured_at` / `closing_lead_min` on legs + observatory, index | **UNKNOWN** | Low | `engine/clv_checker.py:160-183` writes these inside a try, and on **any** failure retries with only `closing_prob`/`clv_pct`. So CLV keeps working whether or not the columns exist, and the failure is invisible in logs (the fallback logs nothing). Cost of being un-applied: CLV capture-quality metrics are always NULL. |
| **013** | **`strategy_performance_compare` + 2 indexes** | **MISSING (confirmed)** | **Certain** | migration_018 reported it as SKIPPED. |
| 014 | One-time `first_seen_at` backfill repair | **UNKNOWN** | Low | Data-only repair, no schema. Nothing in live code reads `first_seen_at`, so there is no signal and no live cost. |
| 015 | `user_config.auto_backtest_green_devils` | **APPLIED** (probably) | Medium | `web/app.py:2249` has an explicit "Pre-migration_015 the column may not exist" retry that logs `slip-prefs: retrying without auto_backtest_green_devils`. **Check your logs for that warning** — its absence is positive evidence the column exists. |
| 016 | `books`/`closing_books`/`closing_lead_min`/`closing_captured_at` on legs, `closing_books`+2 on observatory | **MISSING (assumed, by design)** | Medium | Header says it cannot be applied from this environment, and `engine/clv_checker.py:346-372` stores the per-book close inside the existing `market_observatory.books` jsonb under reserved `_close*` keys precisely *because* `closing_books` doesn't exist. `closing_books` is also listed in `_OPTIONAL_LEG_COLS` so leg inserts strip it silently. **Cost: zero** — the jsonb workaround is the live implementation. |
| 017 | Replaces `market_observatory_upsert_guard()` to allow pending→graded | **MISSING (assumed, by design)** | Medium-High | `engine/results_checker._write_observatory_result` still uses the DELETE + re-INSERT workaround, whose docstring says the proper fix is migration_017 and that we have no DDL access. **Cost: the workaround is slower and has a (documented, argued-safe) delete/insert window, but grading works.** |
| 018 | RLS + revoked grants on the four server-only tables | **APPLIED (partially, as designed)** | Certain | Reported `1 secured, 3 skipped`. `app_state_cache` is now locked down. The other three will be picked up if 010b/013 are ever applied and 018 is re-run. |

> **Note on the "OPTIONAL / no DDL access" headers on 016 and 017.** Those notes
> were written when the only Supabase access was the PostgREST service key. The
> user has since demonstrated SQL-editor access (that is how 018 was applied), so
> the environmental blocker described in those headers **no longer applies**. 016
> and 017 are now *appliable*; they are simply *unapplied*.

---

## 2. What each missing table actually costs — verified runtime behaviour

### `calibration_cells` (migration_010b_rwbc)

**Dependents (complete list):**

- `/Users/maxfried/CoreProp/analysis/apply_migration_010.py:82` — a *verification*
  probe inside a bare `try/except Exception`. On a missing table it prints
  `✗ calibration_cells NOT reachable: …` and continues. Working as designed;
  this script's entire purpose is to apply 010b and then check.
- `/Users/maxfried/CoreProp/analysis/strategy_compare.py:504` — calls
  `rwbc_cal.load_cell_cache_from_db(sb)` inside a try that prints
  `RWBC cache hydration skipped: …`. **Unreachable regardless**: that line imports
  `engine.rwbc_calibration`, which was deleted in commit `71c1091`. The module
  raises `ModuleNotFoundError` at import time, long before the table matters.

**Live cost: ZERO.** No `web/` or `engine/` file mentions the table.

### `calibration_history` (migration_010b_rwbc)

**Dependents (complete list):** `analysis/apply_migration_010.py:90`, same
`try/except` verification probe as above.

**Live cost: ZERO.**

### `strategy_performance_compare` (migration_013)

**Dependents (complete list):**

- `/Users/maxfried/CoreProp/analysis/strategy_compare.py:461` — `persist_metrics()`
  upserts per-`(scoped_at, branch)` rows inside a `try/except` that prints
  `persist failed for … @ …` to stderr and keeps going, returning a written-count
  of 0. **Unreachable anyway**: the module's top-level imports
  (`engine.tier`, `engine.shade_signal`, `engine.beta_calibration`,
  `engine.isotonic_calibration`, `engine.rwbc_calibration`) were **all deleted in
  commit 71c1091**. `python analysis/strategy_compare.py` dies with
  `ModuleNotFoundError` before it touches Supabase.
- `/Users/maxfried/CoreProp/analysis/perf_logger.py:69` — `summary()` selects the
  last N days. This one is **not** wrapped in a try: a missing table surfaces as
  an unhandled PostgREST `APIError` (42P01) and a traceback. But it is also
  unreachable — `perf_logger.py:32` imports `from supabase import create_client`
  at module top level, and `supabase` is **not in `requirements.txt`** (only
  `postgrest` is), so the import fails first.
- `/Users/maxfried/CoreProp/analysis/run_phase3_backfill.sh` — a shell wrapper that
  calls both of the above, and whose own header already says "Apply
  migration_013.sql first".

**Live cost: ZERO.** No `web/` or `engine/` file mentions the table.

### Summary of the swallow-audit

Every dependent site was read individually rather than assumed:

| Site | Guarded? | Behaviour on missing table |
|---|---|---|
| `apply_migration_010.py:82` | yes, bare `except Exception` | prints `✗ … NOT reachable`, exit 0 |
| `apply_migration_010.py:90` | yes, bare `except Exception` | prints `✗ … NOT reachable`, exit 0 |
| `strategy_compare.py:461` (`persist_metrics`) | yes, per-row `except Exception` | prints to stderr, returns 0 written |
| `strategy_compare.py:504` (RWBC hydration) | yes | prints `RWBC cache hydration skipped` |
| `perf_logger.py:69` (`summary`) | **no** | would raise `APIError` 42P01 — but the module can't import |

---

## 3. `engine/calibration_map.py` — confirmed: it does NOT use `calibration_cells`

The brief asked whether the hourly refit could be failing silently. It is not.

**What it reads.** `_load_settled_rows()` (`engine/calibration_map.py:180-213`)
pages `market_observatory`, selecting
`league, side, raw_true_prob, true_prob, result, game_start` filtered to
`result in ('hit','miss')` over a 90-day window. That is the only table it reads.

**What it writes.** `fit_calibration_map()` (lines 271-284) writes exactly two
places:

1. `data/calibration_map.json` on local disk, and
2. `sync_state_to_supabase("calibration_map", payload)` →
   `engine/persistence.py:64` → `app_state_cache.upsert(..., on_conflict="key")`.

So the "Supabase mirror" named in CLAUDE.md and `analysis/CALIBRATION_RUNBOOK.md`
is a **row in `app_state_cache`** keyed `"calibration_map"` — the table that
migration_018 just secured and that demonstrably exists. Both docs are accurate;
the naming collision between the *isotonic map's* per-cell dict (a JSON blob
inside one `app_state_cache` row) and the *RWBC* `calibration_cells` **table** is
what made this look suspicious. They are unrelated artefacts from two different
architectures, and the RWBC one is dead code.

**The read-back path** is `load_calibration_map()` →
`engine/persistence.load_artefact("calibration_map", …)` →
`load_state_from_supabase` → `app_state_cache.select(...)`. Again no
`calibration_cells`.

**Corroborating detail:** `engine/rwbc_calibration.py` — the module that
migration_010b's header names as the table's inference-time reader — does not
exist. It was deleted in `71c1091` along with `engine/tier.py`,
`engine/isotonic_calibration.py`, `engine/beta_calibration.py`,
`engine/shade_signal.py`, `engine/portfolio_kelly.py`, and
`engine/sharpness_calibration.py`. The RWBC calibration stack is gone from this
branch entirely.

**Conclusion:** `_run_periodic_models` (`web/app.py:1436-1497`) is healthy.
Its calibration block will log one of:

- `Hourly refit: N calibration cells, M trusted (live=False)` — normal, and
  `live=False` because `CALIBRATION_MAP_ENABLED` defaults off (`config.py:135`);
- `Hourly refit: calibration — no data yet` — the settled-rows query returned
  nothing (which *would* be a real signal, but about `market_observatory`
  grading, not about a missing table);
- `Hourly refit: calibration error: …` — the only failure line, and no missing
  table can produce it.

---

## 4. Do 010b and 013 contain anything besides the table creations?

**No.** Both files were read end to end. Neither adds a column to `legs`,
`market_observatory`, `user_config`, or `slips`; neither creates a trigger, a
function, a constraint, a policy, or a data backfill.

**migration_010b_rwbc.sql** — full inventory:
- `create table if not exists calibration_cells` (…, `primary key (league, prop, side)`)
- `create index if not exists idx_cal_cells_w_cell on calibration_cells(w_cell)`
- `create table if not exists calibration_history` (`id bigserial primary key`, …)
- `create index if not exists idx_cal_history_recent on calibration_history(fit_at desc)`
- one `raise notice`

**migration_013.sql** — full inventory:
- `create table if not exists strategy_performance_compare` (…, `unique (scoped_at, branch)`)
- `create index if not exists idx_strategy_perf_scoped_at on strategy_performance_compare(scoped_at desc)`
- `create index if not exists idx_strategy_perf_branch_scoped on strategy_performance_compare(branch, scoped_at desc)`
- one `raise notice`

Every index and constraint is scoped to a table that only exists inside its own
migration. **Production is missing nothing outside those two/one tables.**
The `legs` / `market_observatory` column question the brief flagged as
high-stakes resolves clean.

Note that these two files are also the *only* migrations in the directory whose
entire content is self-contained new tables. Every other migration in the set
`ALTER`s a table that the live app writes to, which is why the applied-state of
those matters more and is easier to infer from code.

---

## 5. Recommendation: retire 010b, keep 013 shelved

### `migration_010b_rwbc.sql` — **RETIRE (do not apply)**

Its two tables have no consumer. The only code that ever read them
(`engine/rwbc_calibration.py`) and the endpoint its header advertises
(`GET /api/observatory/rwbc`) were both deleted in `71c1091`. Applying it creates
two permanently-empty tables that then need RLS (a re-run of 018) to not be a
security finding — you would be adding attack surface to store nothing.

The current live calibrator is `engine/calibration_map.py` (isotonic, per
`(league, side)`, persisted in `app_state_cache`), and it needs no schema.

Concrete steps, when someone wants to do the cleanup (**out of scope for this
document — nothing has been changed**):

1. Correct the stale header in `migration_010b_rwbc.sql` — it currently points at
   `engine/rwbc_calibration.calibrate()` and `GET /api/observatory/rwbc`, neither
   of which exists. Add a `⚠️ RETIRED / SUPERSEDED` banner naming commit
   `71c1091` and pointing at `engine/calibration_map.py`. This is the one change
   worth making regardless, because the header actively misleads.
2. `analysis/apply_migration_010.py` becomes dead — it exists only to apply this
   migration. Its closing instruction (`USE_RWBC=true python3 main.py`) references
   an env var that no longer exists anywhere in the codebase.
3. Leave the `calibration_cells` / `calibration_history` lines in `schema.sql` and
   `migration_018.sql` **alone** — `tests/engine_tests/test_rls_coverage.py`
   (`_M018_TARGETS`, lines 111-116) asserts 018 still names all four tables, and
   the schema.sql RLS lines are what that test's coverage check pairs against.
   Removing them breaks the suite. Deciding to drop them is a separate,
   test-touching change.

### `migration_013.sql` — **SHELVE (do not apply now)**

The table has a legitimate design (a daily A/B scoreboard for strategy branches)
but zero working consumers on this branch: `analysis/strategy_compare.py` imports
five deleted engine modules, and `analysis/perf_logger.py` imports a package
(`supabase`) that isn't a dependency. Applying 013 today creates an empty table
nothing can fill.

If the strategy-comparison harness is ever revived, apply 013 **then**, and re-run
018 afterwards so the new table gets RLS. Until then, add a
`⚠️ NOT APPLIED — harness is broken (see 71c1091)` banner to the header so the
next operator does not apply it speculatively.

### 016 and 017 — **worth reconsidering now that you have SQL-editor access**

Both headers claim "cannot be applied from this environment." That claim is now
false — 018 proved you have DDL access. Neither is urgent:

- **017** would let `engine/results_checker._write_observatory_result` drop its
  DELETE + re-INSERT workaround and go back to a plain UPDATE. The workaround
  works, but it deletes a row and re-inserts it, and its safety argument rests on
  "the scraper no longer upserts markets whose games ended ≥2h ago." Applying 017
  removes that reasoning from the critical path. **This is the higher-value of
  the two.** If you apply it, the workaround stays harmless (as its own comment
  notes), so there is no coordinated code change required.
- **016** would graduate the per-book close out of the `market_observatory.books`
  jsonb `_close*` keys into typed columns. Cost of *not* applying: none — the
  jsonb encoding is the live implementation and is fully functional. If you do
  apply it, you must **also** update
  `engine.clv_checker.update_observatory_closing_lines` to write the columns, or
  you gain nothing. Lower priority.

---

## 6. Determine applied-state yourself — run this in the Supabase SQL editor

Read-only. Returns one row per migration with a verdict. Paste as-is.

Read-only: no DDL, no writes, no locks. `to_regclass()` is schema-qualified for the
same reason migration_018 qualifies it — an unqualified name resolves through
`search_path`, so a missing `public` would make every table look absent.

```sql
-- CoreProp migration applied-state probe. Read-only; no DDL, no writes.
-- Every "got" value is cast to text so the UNION ALL cannot fail on type
-- unification, and every row carries an explicit `ord` so output order is
-- deterministic (a bare VALUES/UNION list has no guaranteed order).
with obj as (
  select
    -- ── tables ────────────────────────────────────────────────────────────
    (to_regclass('public.slips')                        is not null) as t_slips,
    (to_regclass('public.legs')                         is not null) as t_legs,
    (to_regclass('public.app_state_cache')              is not null) as t_state,
    (to_regclass('public.user_config')                  is not null) as t_uconfig,
    (to_regclass('public.market_observatory')           is not null) as t_obs,
    (to_regclass('public.calibration_cells')            is not null) as t_cal_cells,
    (to_regclass('public.calibration_history')          is not null) as t_cal_hist,
    (to_regclass('public.strategy_performance_compare') is not null) as t_strat,
    -- ── columns (counted, so a missing table just yields 0) ───────────────
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='market_observatory'
        and column_name in ('closing_prob','books'))                 as c003,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='user_config'
        and column_name in ('auto_slip_type','auto_slip_legs',
                            'auto_slip_min_prob'))                   as c004,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name in ('market_observatory','legs')
        and column_name='resolved_at')                               as c005,
    (select count(*) from information_schema.columns
      where table_schema='public'
        and ((table_name='market_observatory'
              and column_name in ('raw_true_prob','market_width'))
          or (table_name='legs' and column_name='raw_true_prob')))    as c006,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name in ('market_observatory','legs')
        and column_name='team')                                      as c007,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='legs'
        and column_name='dedup_key')                                 as c008,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='market_observatory'
        and column_name in ('first_seen_at','last_seen_at'))          as c009,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='user_config'
        and column_name in ('stripe_customer_id','subscription_status',
                            'subscription_plan','current_period_end')) as c010,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name in ('market_observatory','legs')
        and column_name in ('closing_captured_at','closing_lead_min')) as c012,
    (select count(*) from information_schema.columns
      where table_schema='public' and table_name='user_config'
        and column_name='auto_backtest_green_devils')                as c015,
    (select count(*) from information_schema.columns
      where table_schema='public'
        and ((table_name='legs' and column_name in ('books','closing_books'))
          or (table_name='market_observatory'
              and column_name='closing_books')))                     as c016,
    -- ── migration_011: the DEFAULT on auto_slip_min_prob ─────────────────
    -- column_default is domain `information_schema.character_data`; cast to
    -- text so LIKE and COALESCE behave predictably.
    (select column_default::text from information_schema.columns
      where table_schema='public' and table_name='user_config'
        and column_name='auto_slip_min_prob')                        as d011,
    -- ── migration_009: is the guard trigger attached at all? ─────────────
    (select count(*) from pg_trigger
      where tgname='market_observatory_upsert_guard_trg'
        and not tgisinternal)                                        as trg009,
    -- ── migration_017: the guard body must ALLOW pending -> graded, i.e.
    -- it wraps the freeze in `if old.result is distinct from 'pending'`.
    -- migration_009's original body freezes unconditionally and has no such
    -- branch, so this substring is what distinguishes 017 from 009.
    (select count(*) from pg_proc
      where proname='market_observatory_upsert_guard'
        and prosrc ilike '%is distinct from%pending%')                as fn017
)
select ord, migration, expect, got, verdict from (
  select 1 as ord, 'pre-001 base tables' as migration, '3 tables' as expect,
         format('slips=%s legs=%s app_state_cache=%s',
                t_slips, t_legs, t_state)                       as got,
         case when t_slips and t_legs and t_state
              then 'APPLIED' else 'MISSING' end                 as verdict from obj
  union all select 2, '001 multitenancy', 'user_config exists', t_uconfig::text,
         case when t_uconfig then 'APPLIED' else 'MISSING' end from obj
  union all select 3, '002 market_observatory', 'table exists', t_obs::text,
         case when t_obs then 'APPLIED' else 'MISSING' end from obj
  union all select 4, '003 closing_prob + books', '2 cols', c003::text,
         case when c003 = 2 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 5, '004 slip prefs', '3 cols', c004::text,
         case when c004 = 3 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 6, '005 resolved_at', '2 cols', c005::text,
         case when c005 = 2 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 7, '006 raw_true_prob + market_width', '3 cols', c006::text,
         case when c006 = 3 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 8, '007 team', '2 cols', c007::text,
         case when c007 = 2 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 9, '008 dedup_key', '1 col', c008::text,
         case when c008 = 1 then 'APPLIED' else 'MISSING' end from obj
  union all select 10, '009 seen-at cols + guard trigger', '2 cols + 1 trigger',
         format('cols=%s trigger=%s', c009, trg009),
         case when c009 = 2 and trg009 >= 1
              then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 11, '010 stripe billing', '4 cols', c010::text,
         case when c010 = 4 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 12, '010b RWBC tables', '2 tables',
         format('calibration_cells=%s calibration_history=%s',
                t_cal_cells, t_cal_hist),
         case when t_cal_cells and t_cal_hist then 'APPLIED'
              when t_cal_cells or  t_cal_hist then 'PARTIAL'
              else 'MISSING' end from obj
  union all select 13, '011 auto_slip_min_prob default', 'default 0.60',
         coalesce(d011, '(column absent)'),
         case when d011 like '%0.60%'   then 'APPLIED'
              when d011 like '%0.5407%' then 'MISSING'
              else 'UNKNOWN' end from obj
  union all select 14, '012 CLV capture quality', '4 cols', c012::text,
         case when c012 = 4 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 15, '013 strategy_performance_compare', 'table exists',
         t_strat::text,
         case when t_strat then 'APPLIED' else 'MISSING' end from obj
  union all select 16, '014 first_seen_at repair', 'data-only (no schema)',
         'n/a', 'UNKNOWN — probe separately, see below' from obj
  union all select 17, '015 green devils', '1 col', c015::text,
         case when c015 = 1 then 'APPLIED' else 'MISSING' end from obj
  union all select 18, '016 per-book snapshots', '3 cols', c016::text,
         case when c016 = 3 then 'APPLIED' else 'PARTIAL/MISSING' end from obj
  union all select 19, '017 guard allows grading',
         'guard fn has pending-branch', fn017::text,
         case when fn017 >= 1 then 'APPLIED' else 'MISSING' end from obj
) probe
order by ord;
```

### Companion: migration_018 / RLS state

```sql
-- Every row present should show rls_enabled = true and NO permissive policy.
-- The has_table_privilege() calls are guarded on the role existing so this also
-- runs on a plain Postgres restore without Supabase's anon/authenticated roles.
select c.relname                as table_name,
       c.relrowsecurity         as rls_enabled,
       coalesce(p.n, 0)         as policy_count,
       case when exists (select 1 from pg_roles where rolname = 'anon')
            then has_table_privilege('anon', c.oid, 'SELECT')::text
            else '(no anon role)' end          as anon_can_select,
       case when exists (select 1 from pg_roles where rolname = 'authenticated')
            then has_table_privilege('authenticated', c.oid, 'SELECT')::text
            else '(no authenticated role)' end as auth_can_select
from pg_class c
left join (select polrelid, count(*) n from pg_policy group by 1) p
       on p.polrelid = c.oid
where c.relnamespace = 'public'::regnamespace
  and c.relkind = 'r'
order by c.relname;
```

Expected after migration_018:

| table | rls_enabled | policy_count | notes |
|---|---|---|---|
| `app_state_cache` | `true` | 0 | deny-all, grants revoked |
| `slips` / `legs` / `user_config` | `true` | 1 | owner policy `user_id = auth.uid()` |
| `market_observatory` | `true` | 1 | deliberate public **SELECT**-only |
| `calibration_cells` / `calibration_history` / `strategy_performance_compare` | *absent* | — | skipped by 018 because they do not exist |

### Migration_014's applied-state (why the probe says UNKNOWN)

014 is a pure data repair and leaves no schema fingerprint. Probe it directly —
a non-zero count means 014 has **not** run (or new bad rows appeared):

```sql
select count(*) as rows_still_needing_migration_014
from market_observatory
where first_seen_at >= game_start
  and created_at    <  game_start;
```

---

## 7. Notes for whoever maintains this file

- The verdicts in §1 marked Medium/High but not Certain are **inferences from
  code shape**, not observations of the database. Run the §6 query to convert
  them to Certain. The only Certain-by-observation rows are 010b, 013, and 018,
  which come from migration_018's own notice output, plus `app_state_cache`
  (018 secured it, so it exists).
- `migrations/schema.sql`'s header says "the flattened equivalent of applying
  migration_001 … migration_017" while its body includes migration_018. Minor
  drift; not corrected here (schema.sql is out of scope for this document).
- `README.md:95` says `migration_001 … 017.sql`, now off by one after 018 landed.
- The RLS coverage test (`tests/engine_tests/test_rls_coverage.py`) is a *static*
  parse of `schema.sql` and `migration_018.sql`. It proves the **repo** is
  internally consistent; it says nothing about the live database. That gap is
  exactly what the §6 query closes.

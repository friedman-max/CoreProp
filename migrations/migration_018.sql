-- migration_018: enable RLS on the server-only tables.
--
-- Why this is needed
-- ------------------
-- CoreProp ships SUPABASE_URL + SUPABASE_ANON_KEY to the browser: web/app.py's
-- `root()` injects them into index.html, and GET /api/ui-config returns them.
-- That is correct and necessary (the frontend runs Supabase auth directly), but
-- it means the PostgREST endpoint is reachable by anyone who loads the site.
-- FastAPI is NOT the only path to the data.
--
-- migration_001 enabled RLS on slips / legs / user_config, and migration_002
-- gave market_observatory a deliberate public-read policy. These four tables
-- were never covered, so with the published anon key they are readable AND
-- writable by any visitor:
--
--   app_state_cache             — the pipeline's scrape snapshots. The worst of
--                                 the four: web/app.py::_seed_state_from_db_sync
--                                 trusts this table on boot, so a poisoned row
--                                 is served to every user on first paint.
--   calibration_cells           — isotonic recalibration artefacts
--   calibration_history         — refit history
--   strategy_performance_compare — strategy comparison rows
--
-- The fix
-- -------
-- Enable RLS and add NO policy. In Postgres, RLS-enabled + zero policies =
-- deny-all for the `anon` and `authenticated` roles. The service-role client
-- (engine/database.py `get_db()`) BYPASSES RLS, so every server-side reader and
-- writer keeps working with no code change — the scrape pipeline's persistence
-- (engine/persistence.py, service role throughout), the hourly
-- correlation/calibration refits, the results checker, and the analysis/
-- scripts that read calibration_cells / strategy_performance_compare.
--
-- Nothing in the frontend touches these tables:
-- `grep -rn 'app_state_cache\|calibration_cells' web/static/` returns nothing.
--
-- Why every statement is guarded
-- ------------------------------
-- This migration runs against EXISTING projects, which may never have applied
-- the migrations that created its targets — calibration_cells and
-- calibration_history come from migration_010b_rwbc.sql, and
-- strategy_performance_compare from migration_013.sql, both of which are marked
-- OPTIONAL / not-applicable-from-this-environment in this repo. An
-- unconditional `alter table` on a missing relation fails with
--     ERROR: 42P01: relation "..." does not exist
-- and because Supabase's SQL editor runs the script as a single transaction,
-- that aborts the tables that DO exist along with it — leaving nothing secured.
--
-- So each table is handled independently inside a to_regclass() check: present
-- tables are locked down, absent ones are skipped with a notice. Re-running
-- after you later apply 010b / 013 will pick those up. If you never apply them,
-- schema.sql already carries the RLS lines, so a fresh project is covered.
--
-- Verifying after apply (every row present should show rowsecurity = true):
--   select relname, relrowsecurity from pg_class
--   where relname in ('app_state_cache','calibration_cells',
--                     'calibration_history','strategy_performance_compare');
--
-- Fully idempotent: `enable row level security` is a no-op when already on, and
-- `revoke` is a no-op when the grant is already absent.

do $$
declare
  t text;
  targets text[] := array[
    'app_state_cache',
    'calibration_cells',
    'calibration_history',
    'strategy_performance_compare'
  ];
  n_secured int := 0;
  n_skipped int := 0;
begin
  foreach t in array targets loop
    -- Schema-qualify the existence check. An unqualified to_regclass() resolves
    -- through search_path, so if `public` were absent from it EVERY table would
    -- look missing and this migration would silently secure nothing — a false
    -- sense of safety that is worse than an outright error.
    if to_regclass('public.' || t) is null then
      -- PL/pgSQL `raise` uses bare `%` placeholders, not printf-style `%s`.
      raise notice 'migration_018: skipping % — table does not exist in this project', t;
      n_skipped := n_skipped + 1;
      continue;
    end if;

    execute format('alter table public.%I enable row level security', t);

    -- Defence in depth: drop the default PostgREST role grants too, so the
    -- table stays closed even if RLS is later disabled on it by accident.
    -- Service role is unaffected (it bypasses both grants and RLS). Guarded on
    -- role existence so a non-Supabase Postgres (local restore, CI) doesn't
    -- abort the whole script on a missing role.
    if exists (select 1 from pg_roles where rolname = 'anon') then
      execute format('revoke all on public.%I from anon', t);
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
      execute format('revoke all on public.%I from authenticated', t);
    end if;

    n_secured := n_secured + 1;
  end loop;

  raise notice 'migration_018 applied: % table(s) secured, % skipped (missing)',
    n_secured, n_skipped;

  if n_secured = 0 then
    raise warning 'migration_018 secured NOTHING — every target was missing. '
                  'Check you are connected to the right project.';
  end if;
end $$;

-- ============================================================================
-- CoreProp — converge.sql  ·  "bring this database up to date, whatever state
--                              it is in, and tell me what the state now is"
-- ============================================================================
-- Run this file against a project that is CURRENT, BRAND NEW, or ANYWHERE IN
-- BETWEEN. It is idempotent, non-destructive, and every statement is guarded on
-- its target existing, so partial drift converges and re-running is a no-op.
--
-- How this file relates to the others in migrations/
-- --------------------------------------------------
--   migration_001 … migration_018   the source of truth and the historical
--                                   record. Keep them. They document WHY each
--                                   change was made; converge.sql only knows
--                                   the end state.
--   schema.sql                      the flattened bootstrap for a brand-new
--                                   project ("use this OR the numbered
--                                   migrations, not both").
--   converge.sql (this file)        the third case neither of the above covers:
--                                   an EXISTING project whose applied-migration
--                                   set is unknown. Safe to run at any time,
--                                   any number of times.
--
-- Why it exists
-- -------------
-- migration_018 aborted on a live project with
--     ERROR: 42P01: relation "strategy_performance_compare" does not exist
-- because migration_013 had never been applied. Supabase's SQL editor runs a
-- script as ONE TRANSACTION, so that abort rolled back the tables that DID
-- exist too — nothing was secured, and there was no way to answer "am I up to
-- date?" beforehand. This file answers it, and fixes it in the same pass.
--
-- Design rules, all enforced by tests/engine_tests/test_converge_sql.py
-- --------------------------------------------------------------------
-- 1. IDEMPOTENT. Everything is `if not exists` / `create or replace` /
--    drop-then-recreate. Re-run as often as you like.
-- 2. NEVER DESTRUCTIVE. No drop table, no drop column, no drop index, no
--    truncate, no delete, no `set not null`. Dropping and recreating a POLICY,
--    TRIGGER or CHECK CONSTRAINT is the only way to be idempotent in Postgres
--    (there is no CREATE POLICY IF NOT EXISTS / ADD CONSTRAINT IF NOT EXISTS)
--    and is the idiom migration_001/004/009 already use.
-- 3. EVERY STATEMENT GUARDED. Each target is checked with a SCHEMA-QUALIFIED
--    to_regclass('public.' || t) first; a missing relation SKIPS with a notice
--    instead of aborting. Qualification matters: an unqualified to_regclass()
--    resolves through search_path, so if `public` were absent EVERY check would
--    return NULL and this script would report success having converged NOTHING.
--    The preflight block below raises a warning for exactly that case.
-- 4. RISKY DDL IS SANDBOXED. Statements that can fail on pre-existing data
--    (a check constraint older rows violate, a unique index with duplicates
--    already in the table) sit in `begin … exception when others …` sub-blocks.
--    A PL/pgSQL exception handler is a subtransaction, so one failure degrades
--    to a warning instead of rolling the whole run back.
-- 5. STRUCTURE ONLY — NO DML. See the next section.
-- 6. IT REPORTS. The last block prints tables, RLS, columns, indexes,
--    constraints, policies and the trigger, then a single verdict line. If it
--    says CONVERGED you are up to date; anything MISSING is named.
--
-- One-time data migrations are NOT re-run — on purpose
-- ----------------------------------------------------
-- Several numbered migrations contain UPDATE statements that were correct
-- exactly once. Re-running them would corrupt data, so converge.sql runs no DML
-- at all. It DETECTS the two conditions that still matter and tells you to run
-- the original file by hand:
--
--   migration_005 / _006 / _009 backfills  — one-shot, only meaningful the day
--       the column appeared. New columns added by this file are NULL/defaulted
--       for existing rows, which is what the app already expects (the refits
--       read `coalesce(raw_true_prob, true_prob)`).
--   migration_009's first_seen_at backfill — stamped every historical row with
--       the migration's own run timestamp. migration_014 repairs it from
--       created_at. Re-running 009's backfill would RE-BREAK those rows, so
--       this file only counts them and points you at migration_014.sql.
--   migration_011's default bump           — the DDL half (`alter column
--       auto_slip_min_prob set default 0.60`) IS converged here, because
--       `add column if not exists` is a no-op on a project that already ran
--       migration_004 and would otherwise leave the stale 0.5407 default
--       forever. The DML half (bumping rows still sitting at 0.5407) is NOT
--       run — a user may have deliberately chosen that value. The report counts
--       those rows so you can decide.
--   migration_014                          — pure repair. Detected, reported,
--       never executed. Apply migration_014.sql yourself if the report flags it.
--   repair_duplicate_legs.sql              — a DELETE. Never executed here. The
--       report counts (slip_id, leg_num) groups with more than one row and
--       tells you to run that file if any exist.
--
-- ⚠️ Base-table honesty note
-- --------------------------
-- `slips`, `legs` and `app_state_cache` predate migration_001 (which only
-- ALTERs slips/legs). They were created by hand in the Supabase dashboard, so
-- NO migration file defines them. Their CREATE TABLE statements below are
-- RECONSTRUCTED from the application's read/write code (engine/backtest.py,
-- engine/persistence.py, web/app.py) — they are not the original DDL. They
-- match every column the app touches, but a column the app never touches, or a
-- narrower type/default than the original, would not show up here. On an
-- EXISTING project this does not matter: `create table if not exists` skips a
-- table that is already present and never rewrites it. It matters only when
-- this file has to create them from nothing.
--
-- ⚠️ Two things this file deliberately does NOT do
-- ------------------------------------------------
--   * `alter … set not null` on slips.user_id / legs.user_id. migration_001
--     leaves that as a manual step because it fails on any legacy row with a
--     NULL user_id, and on Supabase that abort rolls back everything. The
--     report counts NULL-user_id rows instead.
--   * Widen or retype an existing column. If a column exists with the wrong
--     type, converging requires a destructive rewrite; the report warns and
--     leaves it to you.
--
-- migration_016 and migration_017 carry "cannot be applied from this
-- environment (no DDL access)" headers. That was a constraint of the authoring
-- environment, not of the change — both are additive and safe, and both are
-- folded in below. The live code does not require 016's typed columns (the
-- close is stashed in market_observatory.books under reserved "_close*" keys),
-- but having them costs nothing. 017's trigger body IS important: without it
-- the migration_009 trigger silently blocks observatory grading.
--
-- Usage
-- -----
--   Supabase SQL editor: paste and run. Read the NOTICE/WARNING output.
--   psql:  \i migrations/converge.sql        (psql prints notices by default)
--   Nothing here needs superuser; it needs DDL rights on `public`.
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 0. Preflight
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
begin
  if to_regnamespace('public') is null then
    raise warning 'converge: the "public" schema is not resolvable from this '
                  'connection. Every existence check below will report its '
                  'target as missing and NOTHING will be converged. Fix the '
                  'connection/schema before trusting any output from this run.';
  end if;

  if to_regclass('auth.users') is null then
    raise warning 'converge: auth.users is absent — this does not look like a '
                  'Supabase project. Foreign keys to auth.users and the '
                  'auth.uid() RLS policies will be skipped with warnings; '
                  'everything else still converges.';
  else
    raise notice 'converge: preflight OK (public schema present, auth.users present).';
  end if;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Tables
--    Base tables (reconstructed, pre-migration_001) + every table a numbered
--    migration created. Each is created only when absent, inside its own
--    exception sub-block so a failure on one does not abort the others.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  n_created int := 0;
  n_present int := 0;
begin
  -- slips — one row per logged slip. RECONSTRUCTED from app code.
  begin
    if to_regclass('public.slips') is null then
      execute $ddl$
        create table if not exists public.slips (
          id               text        primary key,   -- 8-char uppercase uuid slice
          timestamp        timestamptz not null default now(),
          slip_type        text,                       -- 'Power' | 'Flex' | 'Manual'
          n_legs           int,
          proj_slip_ev_pct numeric
        )
      $ddl$;
      raise notice 'converge: created public.slips (reconstructed from app code)';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.slips: %', sqlerrm;
  end;

  -- legs — individual legs of a slip. RECONSTRUCTED from app code.
  begin
    if to_regclass('public.legs') is null then
      execute $ddl$
        create table if not exists public.legs (
          id           bigserial primary key,
          slip_id      text not null references public.slips(id) on delete cascade,
          leg_num      int  not null,
          player       text,
          league       text,
          prop         text,
          line         text,                   -- stored as text; the line value
          side         text,                   -- 'over' | 'under'
          true_prob    numeric,
          ind_ev_pct   numeric,
          game_start   timestamptz,
          closing_prob numeric,
          clv_pct      numeric,
          result       text default 'pending', -- pending|hit|miss|push|dnp|won|lost
          stat_actual  numeric
        )
      $ddl$;
      raise notice 'converge: created public.legs (reconstructed from app code)';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.legs: %', sqlerrm;
  end;

  -- app_state_cache — scrape-state snapshots. RECONSTRUCTED from app code.
  begin
    if to_regclass('public.app_state_cache') is null then
      execute $ddl$
        create table if not exists public.app_state_cache (
          key        text        primary key,
          value      jsonb,
          updated_at timestamptz not null default now()
        )
      $ddl$;
      raise notice 'converge: created public.app_state_cache (reconstructed from app code)';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.app_state_cache: %', sqlerrm;
  end;

  -- user_config — migration_001. The primary key is a foreign key into
  -- auth.users, so the shape depends on whether this is a Supabase project.
  begin
    if to_regclass('public.user_config') is null then
      if to_regclass('auth.users') is null then
        execute $ddl$
          create table if not exists public.user_config (
            user_id              uuid primary key,
            min_ev_pct           numeric default 0.01,
            active_leagues       jsonb   default '{"NBA":true,"MLB":true,"NHL":true,"NCAAB":true}'::jsonb,
            refresh_interval_min int     default 15,
            auto_backtest        boolean default false,
            created_at           timestamptz default now(),
            updated_at           timestamptz default now()
          )
        $ddl$;
        raise warning 'converge: created public.user_config WITHOUT the '
                      'auth.users foreign key (auth.users is absent)';
      else
        execute $ddl$
          create table if not exists public.user_config (
            user_id              uuid primary key references auth.users(id) on delete cascade,
            min_ev_pct           numeric default 0.01,
            active_leagues       jsonb   default '{"NBA":true,"MLB":true,"NHL":true,"NCAAB":true}'::jsonb,
            refresh_interval_min int     default 15,
            auto_backtest        boolean default false,
            created_at           timestamptz default now(),
            updated_at           timestamptz default now()
          )
        $ddl$;
        raise notice 'converge: created public.user_config';
      end if;
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.user_config: %', sqlerrm;
  end;

  -- market_observatory — migration_002.
  begin
    if to_regclass('public.market_observatory') is null then
      execute $ddl$
        create table if not exists public.market_observatory (
          id          uuid primary key default gen_random_uuid(),
          market_key  text unique not null,   -- player|league|prop|line|side|game_start
          player      text  not null,
          league      text  not null,
          prop        text  not null,
          line        float not null,
          side        text  not null,
          true_prob   float not null,
          game_start  timestamptz not null,
          result      text  default 'pending',
          stat_actual float,
          created_at  timestamptz default now()
        )
      $ddl$;
      raise notice 'converge: created public.market_observatory';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.market_observatory: %', sqlerrm;
  end;

  -- calibration_cells — migration_010b_rwbc.
  begin
    if to_regclass('public.calibration_cells') is null then
      execute $ddl$
        create table if not exists public.calibration_cells (
          league             text    not null,
          prop               text    not null,
          side               text    not null,  -- 'over' | 'under'
          w_cell             numeric not null,  -- trust weight in [0, 1]
          p_post             numeric not null,  -- Beta-Binomial posterior mean
          n_eff              numeric not null,  -- recency-weighted obs count
          resolution         numeric not null,
          reliability_error  numeric not null,
          mean_pred          numeric not null,
          mean_obs           numeric not null,
          last_fit_at        timestamptz default now(),
          last_publish_at    timestamptz default now(),
          last_publish_n_eff numeric not null,
          primary key (league, prop, side)
        )
      $ddl$;
      raise notice 'converge: created public.calibration_cells';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.calibration_cells: %', sqlerrm;
  end;

  -- calibration_history — migration_010b_rwbc.
  begin
    if to_regclass('public.calibration_history') is null then
      execute $ddl$
        create table if not exists public.calibration_history (
          id              bigserial primary key,
          fit_at          timestamptz default now(),
          scope           text    not null,     -- 'global' | <league>
          brier_current   numeric,
          brier_rwbc      numeric,
          n_settled       int     not null,
          publish_skipped boolean default false
        )
      $ddl$;
      raise notice 'converge: created public.calibration_history';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.calibration_history: %', sqlerrm;
  end;

  -- strategy_performance_compare — migration_013. This is the table whose
  -- absence aborted migration_018.
  begin
    if to_regclass('public.strategy_performance_compare') is null then
      execute $ddl$
        create table if not exists public.strategy_performance_compare (
          id              bigserial primary key,
          recorded_at     timestamptz default now(),
          scoped_at       date not null,   -- the date being scored
          branch          text not null,   -- 'baseline' | 'holy' | 'maybe'
          n_legs          int  not null,
          mean_pred_prob  numeric,
          mean_obs_hit    numeric,
          mean_clv_pct    numeric,
          beat_close_rate numeric,
          brier           numeric,
          log_loss        numeric,
          n_slips         int,
          mean_slip_ev    numeric,
          realized_roi    numeric,
          win_rate        numeric,
          max_drawdown    numeric,
          log_wealth_end  numeric,
          kelly_variance  numeric,
          sharpe_ratio    numeric,
          tier_breakdown  jsonb,
          config_snapshot jsonb,
          notes           text,
          unique (scoped_at, branch)
        )
      $ddl$;
      raise notice 'converge: created public.strategy_performance_compare';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.strategy_performance_compare: %', sqlerrm;
  end;

  -- landing_events — migration_021. Landing-minigame funnel telemetry,
  -- service-role only (the events arrive at an unauthenticated endpoint, so
  -- the browser must never write here directly with the published anon key).
  begin
    if to_regclass('public.landing_events') is null then
      execute $ddl$
        create table if not exists public.landing_events (
          id        uuid        primary key default gen_random_uuid(),
          ts        timestamptz default now(),
          event     text        not null,
          day_index int,
          pick_id   text,
          meta      jsonb       default '{}'::jsonb
        )
      $ddl$;
      raise notice 'converge: created public.landing_events';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.landing_events: %', sqlerrm;
  end;

  -- push_subscriptions — migration_022. Web Push endpoints for the installable
  -- PWA. Owner-scoped user data; RLS + owner policy are converged in sections
  -- 6 and 7 below, the index in the index section.
  begin
    if to_regclass('public.push_subscriptions') is null then
      execute $ddl$
        create table if not exists public.push_subscriptions (
          id         uuid        primary key default gen_random_uuid(),
          user_id    uuid        not null,
          endpoint   text        not null unique,
          p256dh     text        not null,
          auth       text        not null,
          user_agent text,
          created_at timestamptz default now()
        )
      $ddl$;
      raise notice 'converge: created public.push_subscriptions';
      n_created := n_created + 1;
    else
      n_present := n_present + 1;
    end if;
  exception when others then
    raise warning 'converge: could not create public.push_subscriptions: %', sqlerrm;
  end;

  raise notice 'converge tables: % created, % already present', n_created, n_present;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Columns — the union of every `add column` across migrations 001-016.
--    `add column if not exists` is itself idempotent; the guard here is on the
--    TABLE existing, which is what turns 42P01 into a skip.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  r record;
  t text;               -- FOREACH ... IN ARRAY needs a SCALAR, not a record
  user_id_def text;
  n_ok      int := 0;
  n_skipped int := 0;
  n_failed  int := 0;
begin
  -- user_id (migration_001) is special: its type carries a foreign key into
  -- auth.users, which only exists on Supabase.
  if to_regclass('auth.users') is null then
    user_id_def := 'uuid';
    raise warning 'converge: adding slips.user_id / legs.user_id WITHOUT the '
                  'auth.users foreign key (auth.users is absent). Referential '
                  'integrity for user ownership is NOT enforced on this database.';
  else
    user_id_def := 'uuid references auth.users(id) on delete cascade';
  end if;

  foreach t in array array['slips', 'legs'] loop
    if to_regclass('public.' || t) is null then
      raise notice 'converge columns: skipping %.user_id — table absent', t;
      n_skipped := n_skipped + 1;
      continue;
    end if;
    begin
      execute format('alter table public.%I add column if not exists user_id %s', t, user_id_def);
      n_ok := n_ok + 1;
    exception when others then
      raise warning 'converge: could not add %.user_id: %', t, sqlerrm;
      n_failed := n_failed + 1;
    end;
  end loop;

  -- Everything else. (table, column, definition) — definitions are literals
  -- from this file, never user input, so %s interpolation is safe here.
  for r in
    select v.tbl, v.col, v.def
      from (values
        -- migration_003 — closing_prob + per-book snapshot on observatory
        ('market_observatory', 'closing_prob',        'numeric'),
        ('market_observatory', 'books',               'jsonb default ''{}''::jsonb'),
        -- migration_004 (+ 011 default) — per-user slip preferences
        ('user_config',        'auto_slip_type',      'text default ''Power'''),
        ('user_config',        'auto_slip_legs',      'int default 6'),
        ('user_config',        'auto_slip_min_prob',  'numeric default 0.60'),
        -- migration_005 — resolved_at cursor for the incremental refit
        ('market_observatory', 'resolved_at',         'timestamptz'),
        ('legs',               'resolved_at',         'timestamptz'),
        -- migration_006 — raw (pre-calibration) prob + market width
        ('market_observatory', 'raw_true_prob',       'numeric'),
        ('market_observatory', 'market_width',        'numeric'),
        ('legs',               'raw_true_prob',       'numeric'),
        -- migration_007 — team abbreviation
        ('market_observatory', 'team',                'text'),
        ('legs',               'team',                'text'),
        -- migration_008 — DB-level duplicate-leg prevention
        ('legs',               'dedup_key',           'text'),
        -- migration_009 — availability windows
        ('market_observatory', 'first_seen_at',       'timestamptz default now()'),
        ('market_observatory', 'last_seen_at',        'timestamptz default now()'),
        -- migration_010 — Stripe billing state
        ('user_config',        'stripe_customer_id',  'text'),
        ('user_config',        'subscription_status', 'text'),
        ('user_config',        'subscription_plan',   'text'),
        ('user_config',        'current_period_end',  'timestamptz'),
        -- migration_012 — closing-line capture quality
        ('legs',               'closing_captured_at', 'timestamptz'),
        ('legs',               'closing_lead_min',    'numeric'),
        ('market_observatory', 'closing_captured_at', 'timestamptz'),
        ('market_observatory', 'closing_lead_min',    'numeric'),
        -- migration_015 — green-devils auto-backtest opt-in
        ('user_config',        'auto_backtest_green_devils', 'boolean default false'),
        -- migration_016 — per-book entry/closing snapshots
        ('legs',               'books',               'jsonb default ''{}''::jsonb'),
        ('legs',               'closing_books',       'jsonb default ''{}''::jsonb'),
        ('market_observatory', 'closing_books',       'jsonb default ''{}''::jsonb')
      ) as v(tbl, col, def)
  loop
    if to_regclass('public.' || r.tbl) is null then
      raise notice 'converge columns: skipping %.% — table absent', r.tbl, r.col;
      n_skipped := n_skipped + 1;
      continue;
    end if;
    begin
      execute format('alter table public.%I add column if not exists %I %s', r.tbl, r.col, r.def);
      n_ok := n_ok + 1;
    exception when others then
      raise warning 'converge: could not add %.%: %', r.tbl, r.col, sqlerrm;
      n_failed := n_failed + 1;
    end;
  end loop;

  raise notice 'converge columns: % converged, % skipped (table absent), % failed',
    n_ok, n_skipped, n_failed;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Column defaults that a later migration changed
--    `add column if not exists` above is a NO-OP on a project that already ran
--    migration_004, so the 0.5407 default it created would survive forever.
--    migration_011 raised it to 0.60 (Tier B floor) — converge the DDL half.
--    The row-level bump is DML and is only REPORTED (see section 8).
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
begin
  if to_regclass('public.user_config') is null then
    raise notice 'converge defaults: skipping user_config — table absent';
    return;
  end if;
  begin
    execute format('alter table public.%I alter column auto_slip_min_prob set default 0.60', 'user_config');
    raise notice 'converge defaults: user_config.auto_slip_min_prob default = 0.60 (migration_011)';
  exception when others then
    raise warning 'converge: could not set user_config.auto_slip_min_prob default: %', sqlerrm;
  end;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Check constraints (migration_004)
--    Postgres has no ADD CONSTRAINT IF NOT EXISTS, so each is dropped first —
--    the same idiom migration_004 uses. Dropping a CONSTRAINT is not
--    destructive to data, and it is immediately re-added.
--    A constraint that pre-existing rows VIOLATE cannot be added; that is
--    reported as a warning rather than aborting the run.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  r record;
  n_ok     int := 0;
  n_failed int := 0;
begin
  if to_regclass('public.user_config') is null then
    raise notice 'converge constraints: skipping user_config — table absent';
    return;
  end if;

  for r in
    select v.name, v.body
      from (values
        ('user_config_auto_slip_type_chk',
         'check (auto_slip_type in (''Power'', ''Flex''))'),
        ('user_config_auto_slip_legs_chk',
         'check (auto_slip_legs between 2 and 6)'),
        -- A 2-leg Flex degenerates to a 2-leg Power, so Flex needs >= 3 legs.
        ('user_config_flex_min_legs_chk',
         'check (auto_slip_type <> ''Flex'' or auto_slip_legs >= 3)'),
        ('user_config_auto_slip_min_prob_chk',
         'check (auto_slip_min_prob is null or (auto_slip_min_prob > 0 and auto_slip_min_prob < 1))')
      ) as v(name, body)
  loop
    begin
      execute format('alter table public.%I drop constraint if exists %I', 'user_config', r.name);
      execute format('alter table public.%I add constraint %I %s', 'user_config', r.name, r.body);
      n_ok := n_ok + 1;
    exception when others then
      -- The drop already committed inside this subtransaction's rollback scope,
      -- so the constraint is left exactly as it was before this iteration.
      raise warning 'converge: constraint % not added (existing rows probably '
                    'violate it — inspect user_config by hand): %', r.name, sqlerrm;
      n_failed := n_failed + 1;
    end;
  end loop;

  raise notice 'converge constraints: % applied, % failed', n_ok, n_failed;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Indexes — the union across migrations 001-013.
--    Guarded on the table; `if not exists` makes each one a no-op when present.
--    legs_user_dedup_key_unique is UNIQUE and can fail if duplicates already
--    exist, so it gets the same warn-don't-abort treatment.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  r record;
  n_ok      int := 0;
  n_skipped int := 0;
  n_failed  int := 0;
begin
  for r in
    select v.idx, v.tbl, v.cols, v.uniq
      from (values
        -- base / migration_001
        ('idx_legs_slip',                   'legs',               '(slip_id)', false),
        ('idx_slips_user',                  'slips',              '(user_id, timestamp desc)', false),
        ('idx_legs_user',                   'legs',               '(user_id, slip_id)', false),
        -- migration_002
        ('idx_observatory_pending',         'market_observatory', '(result) where result = ''pending''', false),
        ('idx_observatory_calibration',     'market_observatory', '(league, prop, result)', false),
        -- migration_003
        ('idx_observatory_pending_closing', 'market_observatory', '(game_start) where closing_prob is null and result = ''pending''', false),
        -- migration_005
        ('idx_observatory_resolved_at',     'market_observatory', '(resolved_at) where resolved_at is not null', false),
        ('idx_legs_resolved_at',            'legs',               '(resolved_at) where resolved_at is not null', false),
        -- migration_007
        ('idx_observatory_team',            'market_observatory', '(league, team) where team is not null', false),
        ('idx_legs_team',                   'legs',               '(league, team) where team is not null', false),
        -- migration_008 (partial UNIQUE — the DB-level dedup guarantee)
        ('legs_user_dedup_key_unique',      'legs',               '(user_id, dedup_key) where dedup_key is not null', true),
        -- migration_009
        ('idx_observatory_first_seen_at',   'market_observatory', '(first_seen_at)', false),
        ('idx_observatory_last_seen_at',    'market_observatory', '(last_seen_at)', false),
        -- migration_010
        ('idx_user_config_stripe_customer', 'user_config',        '(stripe_customer_id)', false),
        -- migration_010b_rwbc
        ('idx_cal_cells_w_cell',            'calibration_cells',  '(w_cell)', false),
        ('idx_cal_history_recent',          'calibration_history','(fit_at desc)', false),
        -- migration_012
        ('idx_legs_closing_captured',       'legs',               '(closing_captured_at) where closing_captured_at is not null', false),
        -- migration_013
        ('idx_strategy_perf_scoped_at',     'strategy_performance_compare', '(scoped_at desc)', false),
        ('idx_strategy_perf_branch_scoped', 'strategy_performance_compare', '(branch, scoped_at desc)', false),
        -- migration_021
        ('idx_landing_events_day_event',    'landing_events',     '(day_index, event)', false),
        -- migration_022
        ('idx_push_subscriptions_user',     'push_subscriptions', '(user_id)', false)
      ) as v(idx, tbl, cols, uniq)
  loop
    if to_regclass('public.' || r.tbl) is null then
      raise notice 'converge indexes: skipping % — table % absent', r.idx, r.tbl;
      n_skipped := n_skipped + 1;
      continue;
    end if;
    begin
      if r.uniq then
        execute format('create unique index if not exists %I on public.%I %s', r.idx, r.tbl, r.cols);
      else
        execute format('create index if not exists %I on public.%I %s', r.idx, r.tbl, r.cols);
      end if;
      n_ok := n_ok + 1;
    exception when others then
      raise warning 'converge: index % not created (a UNIQUE index fails when '
                    'duplicates already exist — inspect %): %', r.idx, r.tbl, sqlerrm;
      n_failed := n_failed + 1;
    end;
  end loop;

  raise notice 'converge indexes: % converged, % skipped (table absent), % failed',
    n_ok, n_skipped, n_failed;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. Row-level security — EVERY table, no exceptions.
--    SUPABASE_ANON_KEY is published to the browser (web/app.py::root() injects
--    it into index.html; GET /api/ui-config serves it), so PostgREST is
--    reachable by any visitor and FastAPI is not the only path to the data.
--    Enabling RLS with no policy = deny-all for anon/authenticated; the
--    service-role client (engine/database.py) bypasses RLS, so every
--    server-side reader and writer is unaffected.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  t text;
  n_ok      int := 0;
  n_skipped int := 0;
begin
  foreach t in array array[
    'slips',
    'legs',
    'app_state_cache',
    'user_config',
    'market_observatory',
    'calibration_cells',
    'calibration_history',
    'strategy_performance_compare',
    'landing_events',
    'push_subscriptions'
  ] loop
    -- Schema-qualified on purpose: an unqualified to_regclass() resolves via
    -- search_path, and a missing `public` would make every table look absent.
    if to_regclass('public.' || t) is null then
      raise notice 'converge rls: skipping % — table absent', t;
      n_skipped := n_skipped + 1;
      continue;
    end if;
    begin
      execute format('alter table public.%I enable row level security', t);
      n_ok := n_ok + 1;
    exception when others then
      raise warning 'converge: could not enable RLS on %: %', t, sqlerrm;
    end;
  end loop;

  raise notice 'converge rls: % table(s) with RLS on, % skipped (missing)', n_ok, n_skipped;

  if n_ok = 0 then
    raise warning 'converge: RLS was enabled on NOTHING — every target was '
                  'missing. Check you are connected to the right project.';
  end if;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. Policies (migration_001, migration_002)
--    Postgres has no CREATE POLICY IF NOT EXISTS, so drop-then-create is the
--    idempotent idiom — the same one migration_001/002 use in schema.sql.
--    Dropping a policy destroys no data, and it is recreated immediately.
--
--    The four SERVER-ONLY tables (app_state_cache, calibration_cells,
--    calibration_history, strategy_performance_compare) deliberately get NO
--    policy: RLS-on + zero policies is deny-all. Section 8 also revokes their
--    grants.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  r record;
  n_ok      int := 0;
  n_skipped int := 0;
  n_failed  int := 0;
begin
  for r in
    select v.tbl, v.pol, v.body
      from (values
        -- Owner policies — the actual tenant-isolation boundary for user data.
        ('slips',              'slips_owner',
         'for all using (user_id = auth.uid()) with check (user_id = auth.uid())'),
        ('legs',               'legs_owner',
         'for all using (user_id = auth.uid()) with check (user_id = auth.uid())'),
        ('user_config',        'user_config_owner',
         'for all using (user_id = auth.uid()) with check (user_id = auth.uid())'),
        -- market_observatory is DELIBERATELY public-read (the Transparent
        -- Observatory) and has no write policy.
        ('market_observatory', 'Allow public read-only access',
         'for select using (true)'),
        -- migration_022 — owner policy, same tenant boundary as slips/legs.
        ('push_subscriptions',  'push_subscriptions_owner',
         'for all using (user_id = auth.uid()) with check (user_id = auth.uid())')
      ) as v(tbl, pol, body)
  loop
    if to_regclass('public.' || r.tbl) is null then
      raise notice 'converge policies: skipping % on % — table absent', r.pol, r.tbl;
      n_skipped := n_skipped + 1;
      continue;
    end if;
    begin
      execute format('drop policy if exists %I on public.%I', r.pol, r.tbl);
      execute format('create policy %I on public.%I %s', r.pol, r.tbl, r.body);
      n_ok := n_ok + 1;
    exception when others then
      raise warning 'converge: policy % on % not created (auth.uid() needs '
                    'Supabase; a missing user_id column also fails here): %',
                    r.pol, r.tbl, sqlerrm;
      n_failed := n_failed + 1;
    end;
  end loop;

  raise notice 'converge policies: % applied, % skipped (table absent), % failed',
    n_ok, n_skipped, n_failed;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. Revoke the default PostgREST grants on the SERVER-ONLY tables
--    (migration_018). Defence in depth: they stay closed even if RLS is later
--    switched off on one by accident. Service role bypasses both grants and
--    RLS, so server-side writers are unaffected.
--
--    slips / legs / user_config / market_observatory are NOT in this list —
--    the browser reads those directly with the anon key plus the user's JWT,
--    and revoking their grants would break the app.
--
--    Guarded on the role existing so a plain Postgres (local restore, CI) does
--    not abort on a missing anon/authenticated role.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
declare
  t text;
  g text;
  n_ok int := 0;
begin
  foreach t in array array[
    'app_state_cache',
    'calibration_cells',
    'calibration_history',
    'strategy_performance_compare',
    'landing_events'
  ] loop
    if to_regclass('public.' || t) is null then
      raise notice 'converge grants: skipping % — table absent', t;
      continue;
    end if;
    foreach g in array array['anon', 'authenticated'] loop
      if not exists (select 1 from pg_roles where rolname = g) then
        continue;
      end if;
      begin
        execute format('revoke all on public.%I from %I', t, g);
        n_ok := n_ok + 1;
      exception when others then
        raise warning 'converge: could not revoke % on %: %', g, t, sqlerrm;
      end;
    end loop;
  end loop;

  raise notice 'converge grants: % revoke(s) applied on the server-only tables', n_ok;
end $converge$;


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. The observatory upsert guard — migration_009's trigger with
--    migration_017's body (the FINAL version).
--
--    009 froze result/stat_actual/resolved_at on EVERY update to protect graded
--    rows from scraper upserts, which also silently reverted the results
--    checker's legitimate grading — observatory resolution died the day the
--    trigger landed. 017 allows the pending -> graded transition and keeps
--    every other protection. If you converge a project that still has 009's
--    body, this replaces it, which is the point.
--
--    `create or replace function` is wrapped so that a pre-existing function
--    with an incompatible signature warns instead of aborting the run.
-- ─────────────────────────────────────────────────────────────────────────────
do $converge$
begin
  begin
    execute $fnwrap$
      create or replace function public.market_observatory_upsert_guard()
      returns trigger as $fnbody$
      begin
        -- Preserve first_seen_at across re-scrapes; always bump last_seen_at.
        new.first_seen_at := old.first_seen_at;
        new.last_seen_at  := now();
        -- An already-graded row stays frozen (a scraper upsert must not roll it
        -- back to pending). A pending row MAY be graded — migration_017's fix.
        if old.result is distinct from 'pending' then
          new.result      := old.result;
          new.stat_actual := old.stat_actual;
          new.resolved_at := old.resolved_at;
        end if;
        new.created_at := old.created_at;
        return new;
      end;
      $fnbody$ language plpgsql
    $fnwrap$;
    raise notice 'converge trigger: market_observatory_upsert_guard() is at the '
                 'migration_017 body (pending -> graded allowed)';
  exception when others then
    raise warning 'converge: could not replace market_observatory_upsert_guard(): %', sqlerrm;
  end;

  if to_regclass('public.market_observatory') is null then
    raise notice 'converge trigger: skipping the trigger — market_observatory absent';
    return;
  end if;

  begin
    execute format('drop trigger if exists %I on public.%I',
                   'market_observatory_upsert_guard_trg', 'market_observatory');
    execute format('create trigger %I before update on public.%I '
                   'for each row execute function public.market_observatory_upsert_guard()',
                   'market_observatory_upsert_guard_trg', 'market_observatory');
    raise notice 'converge trigger: market_observatory_upsert_guard_trg installed';
  exception when others then
    raise warning 'converge: could not install market_observatory_upsert_guard_trg: %', sqlerrm;
  end;
end $converge$;


-- ============================================================================
-- 10. VERIFICATION REPORT
-- ============================================================================
-- Everything above converged what it could. This block reports the RESULTING
-- state and ends with one verdict line. Read it top to bottom:
--
--   NOTICE  lines are the inventory.
--   WARNING lines are things still wrong, or data repairs you must run by hand.
--   The last line says either
--       "converge.sql: CONVERGED — …"                  (you are up to date)
--   or  "converge.sql: N item(s) MISSING …"            (read the warnings)
--
-- In the Supabase SQL editor these appear under the results pane; in psql they
-- print inline. If you see NO notices at all, your client is suppressing them
-- (`set client_min_messages = notice;`) — do not read that as success.
-- ============================================================================
do $converge$
declare
  t              text;
  r              record;
  expected       text[] := array[
    'slips', 'legs', 'app_state_cache', 'user_config', 'market_observatory',
    'calibration_cells', 'calibration_history', 'strategy_performance_compare',
    'landing_events', 'push_subscriptions'
  ];
  missing_tables text[] := '{}';
  no_rls         text[] := '{}';
  missing_cols   text[] := '{}';
  missing_idx    text[] := '{}';
  missing_pol    text[] := '{}';
  missing_con    text[] := '{}';
  present_ct     int := 0;
  n              bigint;
  problems       int := 0;
begin
  raise notice '════════ converge.sql verification report ════════';

  if to_regnamespace('public') is null then
    raise warning 'converge report: the "public" schema is not resolvable — '
                  'this report is meaningless and NOTHING was converged.';
    return;
  end if;

  -- ── tables + RLS ───────────────────────────────────────────────────────────
  foreach t in array expected loop
    if to_regclass('public.' || t) is null then
      missing_tables := missing_tables || t;
      continue;
    end if;
    present_ct := present_ct + 1;
    if not exists (
      select 1 from pg_class c
        join pg_namespace ns on ns.oid = c.relnamespace
       where ns.nspname = 'public' and c.relname = t and c.relrowsecurity
    ) then
      no_rls := no_rls || t;
    end if;
  end loop;

  raise notice 'tables present : % of % — %', present_ct, array_length(expected, 1),
    array_to_string(array(select x from unnest(expected) x
                          where not (x = any(missing_tables))), ', ');
  if array_length(missing_tables, 1) > 0 then
    raise warning 'tables MISSING : %', array_to_string(missing_tables, ', ');
    problems := problems + array_length(missing_tables, 1);
  end if;
  if array_length(no_rls, 1) > 0 then
    raise warning 'RLS NOT ENABLED (readable AND writable with the published '
                  'anon key): %', array_to_string(no_rls, ', ');
    problems := problems + array_length(no_rls, 1);
  else
    raise notice 'row level security: enabled on every table that exists';
  end if;

  -- ── columns ────────────────────────────────────────────────────────────────
  for r in
    select v.tbl, v.col
      from (values
        ('slips', 'id'), ('slips', 'timestamp'), ('slips', 'slip_type'),
        ('slips', 'n_legs'), ('slips', 'proj_slip_ev_pct'), ('slips', 'user_id'),
        ('legs', 'id'), ('legs', 'slip_id'), ('legs', 'leg_num'),
        ('legs', 'player'), ('legs', 'league'), ('legs', 'prop'), ('legs', 'line'),
        ('legs', 'side'), ('legs', 'true_prob'), ('legs', 'ind_ev_pct'),
        ('legs', 'game_start'), ('legs', 'closing_prob'), ('legs', 'clv_pct'),
        ('legs', 'result'), ('legs', 'stat_actual'), ('legs', 'user_id'),
        ('legs', 'resolved_at'), ('legs', 'raw_true_prob'), ('legs', 'team'),
        ('legs', 'dedup_key'), ('legs', 'closing_captured_at'),
        ('legs', 'closing_lead_min'), ('legs', 'books'), ('legs', 'closing_books'),
        ('app_state_cache', 'key'), ('app_state_cache', 'value'),
        ('app_state_cache', 'updated_at'),
        ('user_config', 'user_id'), ('user_config', 'min_ev_pct'),
        ('user_config', 'active_leagues'), ('user_config', 'refresh_interval_min'),
        ('user_config', 'auto_backtest'), ('user_config', 'created_at'),
        ('user_config', 'updated_at'), ('user_config', 'auto_slip_type'),
        ('user_config', 'auto_slip_legs'), ('user_config', 'auto_slip_min_prob'),
        ('user_config', 'stripe_customer_id'), ('user_config', 'subscription_status'),
        ('user_config', 'subscription_plan'), ('user_config', 'current_period_end'),
        ('user_config', 'auto_backtest_green_devils'),
        ('market_observatory', 'id'), ('market_observatory', 'market_key'),
        ('market_observatory', 'player'), ('market_observatory', 'league'),
        ('market_observatory', 'prop'), ('market_observatory', 'line'),
        ('market_observatory', 'side'), ('market_observatory', 'true_prob'),
        ('market_observatory', 'game_start'), ('market_observatory', 'result'),
        ('market_observatory', 'stat_actual'), ('market_observatory', 'created_at'),
        ('market_observatory', 'closing_prob'), ('market_observatory', 'books'),
        ('market_observatory', 'resolved_at'), ('market_observatory', 'raw_true_prob'),
        ('market_observatory', 'market_width'), ('market_observatory', 'team'),
        ('market_observatory', 'first_seen_at'), ('market_observatory', 'last_seen_at'),
        ('market_observatory', 'closing_captured_at'),
        ('market_observatory', 'closing_lead_min'),
        ('market_observatory', 'closing_books'),
        ('calibration_cells', 'league'), ('calibration_cells', 'prop'),
        ('calibration_cells', 'side'), ('calibration_cells', 'w_cell'),
        ('calibration_cells', 'p_post'), ('calibration_cells', 'n_eff'),
        ('calibration_cells', 'resolution'), ('calibration_cells', 'reliability_error'),
        ('calibration_cells', 'mean_pred'), ('calibration_cells', 'mean_obs'),
        ('calibration_cells', 'last_fit_at'), ('calibration_cells', 'last_publish_at'),
        ('calibration_cells', 'last_publish_n_eff'),
        ('calibration_history', 'id'), ('calibration_history', 'fit_at'),
        ('calibration_history', 'scope'), ('calibration_history', 'brier_current'),
        ('calibration_history', 'brier_rwbc'), ('calibration_history', 'n_settled'),
        ('calibration_history', 'publish_skipped'),
        ('strategy_performance_compare', 'id'),
        ('strategy_performance_compare', 'recorded_at'),
        ('strategy_performance_compare', 'scoped_at'),
        ('strategy_performance_compare', 'branch'),
        ('strategy_performance_compare', 'n_legs'),
        ('strategy_performance_compare', 'mean_pred_prob'),
        ('strategy_performance_compare', 'mean_obs_hit'),
        ('strategy_performance_compare', 'mean_clv_pct'),
        ('strategy_performance_compare', 'beat_close_rate'),
        ('strategy_performance_compare', 'brier'),
        ('strategy_performance_compare', 'log_loss'),
        ('strategy_performance_compare', 'n_slips'),
        ('strategy_performance_compare', 'mean_slip_ev'),
        ('strategy_performance_compare', 'realized_roi'),
        ('strategy_performance_compare', 'win_rate'),
        ('strategy_performance_compare', 'max_drawdown'),
        ('strategy_performance_compare', 'log_wealth_end'),
        ('strategy_performance_compare', 'kelly_variance'),
        ('strategy_performance_compare', 'sharpe_ratio'),
        ('strategy_performance_compare', 'tier_breakdown'),
        ('strategy_performance_compare', 'config_snapshot'),
        ('strategy_performance_compare', 'notes')
      ) as v(tbl, col)
  loop
    -- Only report columns on tables that exist; a missing table is already
    -- counted above and would otherwise report 30 "missing columns" too.
    if to_regclass('public.' || r.tbl) is null then
      continue;
    end if;
    if not exists (
      select 1 from information_schema.columns
       where table_schema = 'public' and table_name = r.tbl and column_name = r.col
    ) then
      missing_cols := missing_cols || (r.tbl || '.' || r.col);
    end if;
  end loop;

  if array_length(missing_cols, 1) > 0 then
    raise warning 'columns MISSING on existing tables: %',
      array_to_string(missing_cols, ', ');
    problems := problems + array_length(missing_cols, 1);
  else
    raise notice 'columns: every expected column present on every table that exists';
  end if;

  -- ── indexes ────────────────────────────────────────────────────────────────
  foreach t in array array[
    'idx_legs_slip', 'idx_slips_user', 'idx_legs_user',
    'idx_observatory_pending', 'idx_observatory_calibration',
    'idx_observatory_pending_closing', 'idx_observatory_resolved_at',
    'idx_legs_resolved_at', 'idx_observatory_team', 'idx_legs_team',
    'legs_user_dedup_key_unique', 'idx_observatory_first_seen_at',
    'idx_observatory_last_seen_at', 'idx_user_config_stripe_customer',
    'idx_cal_cells_w_cell', 'idx_cal_history_recent', 'idx_legs_closing_captured',
    'idx_strategy_perf_scoped_at', 'idx_strategy_perf_branch_scoped',
    'idx_landing_events_day_event', 'idx_push_subscriptions_user'
  ] loop
    if not exists (
      select 1 from pg_indexes where schemaname = 'public' and indexname = t
    ) then
      missing_idx := missing_idx || t;
    end if;
  end loop;

  if array_length(missing_idx, 1) > 0 then
    raise warning 'indexes MISSING (each belongs to a table that may itself be '
                  'absent — check the table list first): %',
      array_to_string(missing_idx, ', ');
    problems := problems + array_length(missing_idx, 1);
  else
    raise notice 'indexes: all 21 expected indexes present';
  end if;

  -- ── constraints ────────────────────────────────────────────────────────────
  foreach t in array array[
    'user_config_auto_slip_type_chk', 'user_config_auto_slip_legs_chk',
    'user_config_flex_min_legs_chk', 'user_config_auto_slip_min_prob_chk'
  ] loop
    if not exists (select 1 from pg_constraint where conname = t) then
      missing_con := missing_con || t;
    end if;
  end loop;

  if array_length(missing_con, 1) > 0 then
    raise warning 'check constraints MISSING: %', array_to_string(missing_con, ', ');
    problems := problems + array_length(missing_con, 1);
  else
    raise notice 'check constraints: all 4 user_config checks present';
  end if;

  -- ── policies ───────────────────────────────────────────────────────────────
  for r in
    select v.tbl, v.pol
      from (values
        ('slips', 'slips_owner'),
        ('legs', 'legs_owner'),
        ('user_config', 'user_config_owner'),
        ('market_observatory', 'Allow public read-only access')
      ) as v(tbl, pol)
  loop
    if to_regclass('public.' || r.tbl) is null then
      continue;
    end if;
    if not exists (
      select 1 from pg_policy p
        join pg_class c  on c.oid = p.polrelid
        join pg_namespace ns on ns.oid = c.relnamespace
       where ns.nspname = 'public' and c.relname = r.tbl and p.polname = r.pol
    ) then
      missing_pol := missing_pol || (r.tbl || '.' || r.pol);
    end if;
  end loop;

  if array_length(missing_pol, 1) > 0 then
    raise warning 'policies MISSING (user data is not isolated without the '
                  'owner policies): %', array_to_string(missing_pol, ', ');
    problems := problems + array_length(missing_pol, 1);
  else
    raise notice 'policies: owner policies + the observatory public-read policy present';
  end if;

  -- The four server-only tables must have RLS and NO policy (deny-all).
  for r in
    select c.relname as tbl, count(p.oid) as n_pol
      from pg_class c
      join pg_namespace ns on ns.oid = c.relnamespace
      left join pg_policy p on p.polrelid = c.oid
     where ns.nspname = 'public'
       and c.relname in ('app_state_cache', 'calibration_cells',
                         'calibration_history', 'strategy_performance_compare')
     group by c.relname
  loop
    if r.n_pol > 0 then
      raise warning 'server-only table % has % policy(ies); it is meant to be '
                    'deny-all (RLS on, no policy)', r.tbl, r.n_pol;
      problems := problems + 1;
    end if;
  end loop;

  -- ── trigger ────────────────────────────────────────────────────────────────
  if to_regclass('public.market_observatory') is null then
    raise notice 'trigger: not checked — market_observatory absent';
  elsif not exists (
    select 1 from pg_trigger
     where tgname = 'market_observatory_upsert_guard_trg' and not tgisinternal
  ) then
    raise warning 'trigger MISSING: market_observatory_upsert_guard_trg — '
                  'first_seen_at/last_seen_at will not be maintained';
    problems := problems + 1;
  else
    raise notice 'trigger: market_observatory_upsert_guard_trg present';
  end if;

  -- ── data-repair advisories (this file runs NO DML — these are reports) ─────
  if to_regclass('public.market_observatory') is not null and exists (
    select 1 from information_schema.columns
     where table_schema = 'public' and table_name = 'market_observatory'
       and column_name = 'first_seen_at'
  ) then
    execute 'select count(*) from public.market_observatory '
            'where first_seen_at >= game_start and created_at < game_start'
      into n;
    if n > 0 then
      raise warning 'DATA: % market_observatory row(s) still carry the '
                    'migration_009 first_seen_at artifact (first seen AFTER '
                    'kickoff). converge.sql never runs data repairs — apply '
                    'migrations/migration_014.sql by hand to fix them.', n;
    else
      raise notice 'data: no first_seen_at artifact rows (migration_014 not needed)';
    end if;
  end if;

  if to_regclass('public.legs') is not null then
    execute 'select count(*) from (select slip_id from public.legs '
            'group by slip_id, leg_num having count(*) > 1) d'
      into n;
    if n > 0 then
      raise warning 'DATA: % (slip_id, leg_num) group(s) have duplicate leg '
                    'rows (the "12-leg slip" bug). converge.sql never deletes '
                    'rows — run migrations/repair_duplicate_legs.sql by hand.', n;
    else
      raise notice 'data: no duplicate legs (repair_duplicate_legs.sql not needed)';
    end if;

    execute 'select count(*) from public.legs where user_id is null' into n;
    if n > 0 then
      raise notice 'data: % legs row(s) have a NULL user_id. migration_001 '
                   'leaves `set not null` as a MANUAL step and converge.sql '
                   'will not force it — those rows are invisible to the owner '
                   'RLS policy.', n;
    end if;
  end if;

  if to_regclass('public.user_config') is not null and exists (
    select 1 from information_schema.columns
     where table_schema = 'public' and table_name = 'user_config'
       and column_name = 'auto_slip_min_prob'
  ) then
    execute 'select count(*) from public.user_config where auto_slip_min_prob is null '
            'or auto_slip_min_prob in (0.5407, 0.5408)' into n;
    if n > 0 then
      raise notice 'data: % user_config row(s) still sit at the legacy '
                   'auto_slip_min_prob default. The DEFAULT is now 0.60 '
                   '(migration_011) but converge.sql does not rewrite rows — '
                   'a user may have chosen that value deliberately. Run '
                   'migration_011.sql''s UPDATE if you want them bumped.', n;
    end if;
  end if;

  -- ── verdict ────────────────────────────────────────────────────────────────
  raise notice '──────────────────────────────────────────────────';
  if problems = 0 then
    raise notice 'converge.sql: CONVERGED — schema is current (base tables + '
                 'migrations 001-018). Nothing MISSING. Any DATA warnings '
                 'above are one-time repairs you must run by hand.';
  else
    raise warning 'converge.sql: % item(s) MISSING or unconverged. Read the '
                  'WARNING lines above, fix the cause, then re-run this file — '
                  'it is idempotent.', problems;
  end if;
end $converge$;


-- ============================================================================
-- Companion query — run this on its own for a tabular view of the same state.
-- (Commented out so this file stays a single pure-DDL script.)
-- ============================================================================
-- select c.relname                                     as table_name,
--        c.relrowsecurity                              as rls_enabled,
--        (select count(*) from pg_policy p where p.polrelid = c.oid) as policies,
--        (select count(*) from pg_index  i where i.indrelid = c.oid) as indexes,
--        (select count(*) from information_schema.columns ic
--          where ic.table_schema = 'public' and ic.table_name = c.relname) as columns
--   from pg_class c
--   join pg_namespace ns on ns.oid = c.relnamespace
--  where ns.nspname = 'public'
--    and c.relkind = 'r'
--    and c.relname in ('slips', 'legs', 'app_state_cache', 'user_config',
--                      'market_observatory', 'calibration_cells',
--                      'calibration_history', 'strategy_performance_compare')
--  order by c.relname;

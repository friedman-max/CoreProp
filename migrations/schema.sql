-- ============================================================================
-- CoreProp — consolidated schema (fresh-project bootstrap)
-- ============================================================================
-- This file is the full current schema folded into one script, for standing up
-- a BRAND-NEW Supabase project in a single pass. It is the flattened
-- equivalent of applying migration_001 … migration_017 in order.
--
-- Use this OR the numbered migrations, not both:
--   * New project      → run this file once.
--   * Existing project → the numbered migration_0NN.sql files remain the
--     source of truth and historical record; apply any you haven't yet.
--
-- The numbered files stay in this directory on purpose — the app code and docs
-- reference specific migration numbers (e.g. "pre-migration_015 the column may
-- not exist"), and they document why each change was made.
--
-- Idempotent throughout (IF NOT EXISTS / CREATE OR REPLACE / DROP … IF EXISTS),
-- so re-running is safe.
--
-- ⚠️ Base tables note: `slips`, `legs`, and `app_state_cache` predate
-- migration_001 (which only ALTERs slips/legs) — they were created directly in
-- the Supabase dashboard, so no migration file defines them. Their CREATE TABLE
-- statements below are RECONSTRUCTED from the application's insert/read code
-- (engine/backtest.py, web/app.py, engine/persistence.py). If you have the
-- original DDL, prefer it; this reconstruction matches every column the app
-- reads or writes.
-- ============================================================================


-- ─────────────────────────────────────────────────────────────────────────
-- Base tables (reconstructed — pre-migration_001)
-- ─────────────────────────────────────────────────────────────────────────

-- Slip header: one row per logged slip. Legs hang off it via slip_id.
create table if not exists slips (
  id                text        primary key,   -- 8-char uppercase uuid slice
  timestamp         timestamptz not null default now(),
  slip_type         text,                       -- 'Power' | 'Flex' | 'Manual'
  n_legs            int,
  proj_slip_ev_pct  numeric
  -- user_id added by migration_001 (below)
);

-- Individual legs of a slip.
create table if not exists legs (
  id           bigserial primary key,
  slip_id      text not null references slips(id) on delete cascade,
  leg_num      int  not null,
  player       text,
  league       text,
  prop         text,
  line         text,                            -- stored as text; line value
  side         text,                            -- 'over' | 'under'
  true_prob    numeric,
  ind_ev_pct   numeric,
  game_start   timestamptz,
  closing_prob numeric,
  clv_pct      numeric,
  result       text default 'pending',          -- pending | hit | miss | push | dnp | won | lost
  stat_actual  numeric
  -- user_id, raw_true_prob, team, dedup_key, resolved_at, closing_captured_at,
  -- closing_lead_min, books, closing_books all added by later migrations below
);

create index if not exists idx_legs_slip on legs(slip_id);

-- Scrape-state cache: the pipeline persists each dataset here (gzip-enveloped
-- when large) so a cold start can serve the last snapshot instantly.
create table if not exists app_state_cache (
  key        text        primary key,
  value      jsonb,
  updated_at timestamptz not null default now()
);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_001 — multi-tenancy: user_id, user_config, RLS
-- ─────────────────────────────────────────────────────────────────────────

alter table slips add column if not exists user_id uuid references auth.users(id) on delete cascade;
alter table legs  add column if not exists user_id uuid references auth.users(id) on delete cascade;

create table if not exists user_config (
  user_id              uuid primary key references auth.users(id) on delete cascade,
  min_ev_pct           numeric default 0.01,
  active_leagues       jsonb   default '{"NBA":true,"MLB":true,"NHL":true,"NCAAB":true}'::jsonb,
  refresh_interval_min int     default 15,
  auto_backtest        boolean default false,
  created_at           timestamptz default now(),
  updated_at           timestamptz default now()
);

create index if not exists idx_slips_user on slips(user_id, timestamp desc);
create index if not exists idx_legs_user  on legs(user_id, slip_id);

alter table slips       enable row level security;
alter table legs        enable row level security;
alter table user_config enable row level security;

drop policy if exists "slips_owner" on slips;
create policy "slips_owner" on slips
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

drop policy if exists "legs_owner" on legs;
create policy "legs_owner" on legs
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());

drop policy if exists "user_config_owner" on user_config;
create policy "user_config_owner" on user_config
  for all using (user_id = auth.uid()) with check (user_id = auth.uid());


-- ─────────────────────────────────────────────────────────────────────────
-- migration_002 — market_observatory (global market feed)
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists market_observatory (
  id          uuid primary key default gen_random_uuid(),
  market_key  text unique not null,   -- "player|league|prop|line|side|game_start"
  player      text not null,
  league      text not null,
  prop        text not null,
  line        float not null,
  side        text not null,
  true_prob   float not null,
  game_start  timestamptz not null,
  result      text default 'pending', -- pending | hit | miss | push | dnp
  stat_actual float,
  created_at  timestamptz default now()
);

create index if not exists idx_observatory_pending
  on market_observatory(result) where result = 'pending';
create index if not exists idx_observatory_calibration
  on market_observatory(league, prop, result);

alter table market_observatory enable row level security;

drop policy if exists "Allow public read-only access" on market_observatory;
create policy "Allow public read-only access" on market_observatory
  for select using (true);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_003 — closing_prob + per-book snapshot on observatory
-- ─────────────────────────────────────────────────────────────────────────

alter table market_observatory add column if not exists closing_prob numeric;
alter table market_observatory add column if not exists books jsonb default '{}'::jsonb;

create index if not exists idx_observatory_pending_closing
  on market_observatory(game_start)
  where closing_prob is null and result = 'pending';


-- ─────────────────────────────────────────────────────────────────────────
-- migration_004 — per-user slip preferences (+ migration_011 defaults folded in)
-- ─────────────────────────────────────────────────────────────────────────

alter table user_config
  add column if not exists auto_slip_type     text    default 'Power',
  add column if not exists auto_slip_legs     int     default 6,
  -- Default raised 0.5407 → 0.60 by migration_011 (Tier B floor).
  add column if not exists auto_slip_min_prob numeric default 0.60;

alter table user_config drop constraint if exists user_config_auto_slip_type_chk;
alter table user_config add constraint user_config_auto_slip_type_chk
  check (auto_slip_type in ('Power', 'Flex'));

alter table user_config drop constraint if exists user_config_auto_slip_legs_chk;
alter table user_config add constraint user_config_auto_slip_legs_chk
  check (auto_slip_legs between 2 and 6);

alter table user_config drop constraint if exists user_config_flex_min_legs_chk;
alter table user_config add constraint user_config_flex_min_legs_chk
  check (auto_slip_type <> 'Flex' or auto_slip_legs >= 3);

alter table user_config drop constraint if exists user_config_auto_slip_min_prob_chk;
alter table user_config add constraint user_config_auto_slip_min_prob_chk
  check (auto_slip_min_prob is null or (auto_slip_min_prob > 0 and auto_slip_min_prob < 1));


-- ─────────────────────────────────────────────────────────────────────────
-- migration_005 — resolved_at cursor for incremental refit
-- ─────────────────────────────────────────────────────────────────────────

alter table market_observatory add column if not exists resolved_at timestamptz;
alter table legs               add column if not exists resolved_at timestamptz;

create index if not exists idx_observatory_resolved_at
  on market_observatory(resolved_at) where resolved_at is not null;
create index if not exists idx_legs_resolved_at
  on legs(resolved_at) where resolved_at is not null;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_006 — raw (pre-calibration) prob + market width
-- ─────────────────────────────────────────────────────────────────────────

alter table market_observatory add column if not exists raw_true_prob numeric;
alter table market_observatory add column if not exists market_width  numeric;
alter table legs               add column if not exists raw_true_prob numeric;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_007 — team abbreviation per leg / observation
-- ─────────────────────────────────────────────────────────────────────────

alter table market_observatory add column if not exists team text;
alter table legs               add column if not exists team text;

create index if not exists idx_observatory_team
  on market_observatory(league, team) where team is not null;
create index if not exists idx_legs_team
  on legs(league, team) where team is not null;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_008 — DB-level duplicate-leg prevention
-- ─────────────────────────────────────────────────────────────────────────

alter table legs add column if not exists dedup_key text;

create unique index if not exists legs_user_dedup_key_unique
  on legs (user_id, dedup_key) where dedup_key is not null;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_009 — availability windows + state-preservation trigger
--   (trigger body superseded by migration_017 — final version defined below)
-- ─────────────────────────────────────────────────────────────────────────

alter table market_observatory add column if not exists first_seen_at timestamptz default now();
alter table market_observatory add column if not exists last_seen_at  timestamptz default now();

create index if not exists idx_observatory_first_seen_at on market_observatory(first_seen_at);
create index if not exists idx_observatory_last_seen_at  on market_observatory(last_seen_at);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_010 — Stripe billing state on user_config
-- ─────────────────────────────────────────────────────────────────────────

alter table user_config
  add column if not exists stripe_customer_id  text,
  add column if not exists subscription_status text,
  add column if not exists subscription_plan   text,
  add column if not exists current_period_end  timestamptz;

create index if not exists idx_user_config_stripe_customer
  on user_config (stripe_customer_id);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_010b_rwbc — RWBC per-cell calibration tables
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists calibration_cells (
  league             text    not null,
  prop               text    not null,
  side               text    not null,           -- 'over' | 'under'
  w_cell             numeric not null,            -- trust weight in [0, 1]
  p_post             numeric not null,            -- Beta-Binomial posterior mean
  n_eff              numeric not null,            -- recency-weighted obs count
  resolution         numeric not null,
  reliability_error  numeric not null,
  mean_pred          numeric not null,
  mean_obs           numeric not null,
  last_fit_at        timestamptz default now(),
  last_publish_at    timestamptz default now(),
  last_publish_n_eff numeric not null,
  primary key (league, prop, side)
);

create index if not exists idx_cal_cells_w_cell on calibration_cells(w_cell);

create table if not exists calibration_history (
  id              bigserial primary key,
  fit_at          timestamptz default now(),
  scope           text    not null,               -- 'global' | <league>
  brier_current   numeric,
  brier_rwbc      numeric,
  n_settled       int     not null,
  publish_skipped boolean default false
);

create index if not exists idx_cal_history_recent on calibration_history(fit_at desc);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_012 — closing-line capture quality (legs + observatory)
-- ─────────────────────────────────────────────────────────────────────────

alter table legs               add column if not exists closing_captured_at timestamptz;
alter table legs               add column if not exists closing_lead_min    numeric;
alter table market_observatory add column if not exists closing_captured_at timestamptz;
alter table market_observatory add column if not exists closing_lead_min    numeric;

create index if not exists idx_legs_closing_captured
  on legs(closing_captured_at) where closing_captured_at is not null;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_013 — strategy A/B comparison logging
-- ─────────────────────────────────────────────────────────────────────────

create table if not exists strategy_performance_compare (
  id              bigserial primary key,
  recorded_at     timestamptz default now(),
  scoped_at       date not null,
  branch          text not null,                  -- 'baseline' | 'holy' | 'maybe'
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
);

create index if not exists idx_strategy_perf_scoped_at
  on strategy_performance_compare(scoped_at desc);
create index if not exists idx_strategy_perf_branch_scoped
  on strategy_performance_compare(branch, scoped_at desc);


-- ─────────────────────────────────────────────────────────────────────────
-- migration_015 — green-devils auto-backtest opt-in
-- ─────────────────────────────────────────────────────────────────────────

alter table user_config
  add column if not exists auto_backtest_green_devils boolean default false;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_016 — per-book entry/closing snapshots (OPTIONAL — see note)
--   The live code stores the close inside market_observatory.books under
--   reserved "_close*" keys and does NOT require these columns. Included here
--   because a fresh project has DDL access and may as well have the typed
--   columns. On legs the `books` jsonb also holds the entry per-book snapshot.
-- ─────────────────────────────────────────────────────────────────────────

alter table legs add column if not exists books               jsonb default '{}'::jsonb;
alter table legs add column if not exists closing_books        jsonb default '{}'::jsonb;
alter table market_observatory add column if not exists closing_books jsonb default '{}'::jsonb;


-- ─────────────────────────────────────────────────────────────────────────
-- migration_009 + migration_017 — observatory upsert guard (final version)
--   Preserves first_seen_at, bumps last_seen_at, freezes resolution state on
--   ALREADY-graded rows, but ALLOWS the pending → graded transition (017 fix).
-- ─────────────────────────────────────────────────────────────────────────

create or replace function market_observatory_upsert_guard()
returns trigger as $$
begin
  new.first_seen_at := old.first_seen_at;
  new.last_seen_at  := now();
  if old.result is distinct from 'pending' then
    new.result      := old.result;
    new.stat_actual := old.stat_actual;
    new.resolved_at := old.resolved_at;
  end if;
  new.created_at := old.created_at;
  return new;
end;
$$ language plpgsql;

drop trigger if exists market_observatory_upsert_guard_trg on market_observatory;
create trigger market_observatory_upsert_guard_trg
  before update on market_observatory
  for each row execute function market_observatory_upsert_guard();


-- ─────────────────────────────────────────────────────────────────────────
-- migration_018 — RLS on the server-only tables
--   SUPABASE_ANON_KEY is published to the browser (web/app.py injects it into
--   index.html; GET /api/ui-config serves it), so PostgREST is reachable by any
--   visitor. These four tables had no RLS, making them readable AND writable
--   with that key — app_state_cache most dangerously, since
--   _seed_state_from_db_sync() trusts it on boot.
--
--   RLS enabled with NO policy = deny-all for anon/authenticated. The
--   service-role client (engine/database.py) bypasses RLS, so the pipeline,
--   the hourly refits, and the results checker are unaffected. Nothing in the
--   frontend reads these tables directly.
-- ─────────────────────────────────────────────────────────────────────────

alter table app_state_cache              enable row level security;
alter table calibration_cells            enable row level security;
alter table calibration_history          enable row level security;
alter table strategy_performance_compare enable row level security;

-- Grants revoked too (defence in depth: closed even if RLS is later disabled on
-- one by accident). Guarded on role existence so this file still applies on a
-- plain Postgres without Supabase's anon/authenticated roles.
do $$
declare
  t text;
  r text;
begin
  foreach t in array array['app_state_cache', 'calibration_cells',
                           'calibration_history', 'strategy_performance_compare'] loop
    foreach r in array array['anon', 'authenticated'] loop
      if exists (select 1 from pg_roles where rolname = r) then
        execute format('revoke all on public.%I from %I', t, r);
      end if;
    end loop;
  end loop;
end $$;


-- migrations 011 (default bump — folded into 004 above) and 014 (one-time
-- backfill repair) have no residual schema in a fresh project and are omitted
-- here intentionally.

do $$ begin raise notice 'CoreProp schema.sql applied: base tables + migrations 001-018'; end $$;

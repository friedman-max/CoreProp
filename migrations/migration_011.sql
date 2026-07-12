-- migration_011: Phase 1A tier-aware auto-backtest defaults.
--
-- Old default `auto_slip_min_prob` of 0.5407 (the Power-6 break-even,
-- migration_004) produced a logged-leg distribution with a 50.3% hit rate
-- across the 38,788-row settled market_observatory — at chance for every
-- slip size. The audit found three threshold bands with materially
-- different hit-rate behavior:
--
--   p >= 0.65  →  69% hit rate (n≈80 / 30d)        — Tier A
--   p >= 0.60  →  59% hit rate (n≈250 / 30d)       — Tier B (5/6-pick only)
--   p >= 0.55  →  53% hit rate                     — Tier C (display only)
--
-- This migration raises the default `auto_slip_min_prob` from 0.5407 to
-- 0.60 (Tier B floor). The application-side `effective_min_prob()` in
-- engine/tier.py then enforces Tier A for short slips (2/3/4-pick) on top
-- of the per-user override, so a 3-Power user still gets Tier A routing
-- without having to manually set min_prob = 0.65.
--
-- Existing rows whose user has explicitly set a non-default value are
-- left alone. Only rows still using the legacy 0.5407 default are bumped.

alter table user_config
  alter column auto_slip_min_prob set default 0.60;

-- Bump any row still sitting at the legacy default (or NULL — meaning the
-- application was falling back to the break-even constant). Anything else
-- is treated as a deliberate user choice and preserved.
update user_config
  set auto_slip_min_prob = 0.60
  where auto_slip_min_prob is null
     or auto_slip_min_prob = 0.5407
     or auto_slip_min_prob = 0.5408;

-- Notice so the runner sees the migration ran.
do $$ begin raise notice 'migration_011 applied: auto_slip_min_prob default raised to 0.60 (Tier B floor)'; end $$;

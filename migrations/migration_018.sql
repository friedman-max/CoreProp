-- migration_018: persist the PrizePicks odds variant (standard / goblin / demon)
-- on each logged leg.
--
-- Why: goblins (green devils) and demons pay differently from the standard
-- Power/Flex tables, and the payout scoring paths (web/app.py get_backtest_slips,
-- engine/calibration.py evaluate_analytics, engine/backtest.py try_log_slip EV
-- gating) now apply engine.constants.slip_payout_factor based on this column.
-- Without it, every logged leg is assumed standard and a goblin slip's EV / P&L
-- is overstated. Storing odds_type also makes historical goblin slips
-- identifiable and re-scoreable if an exact multiplier is captured later.
--
-- Additive and idempotent. Existing rows default to 'standard', so backfilled
-- history is scored exactly as before (factor 1.0) — no behavior change for
-- pre-existing standard slips.

alter table legs add column if not exists odds_type text default 'standard';

-- Optional: index for future per-odds-type analytics queries. Cheap; only the
-- non-standard rows are indexed.
create index if not exists idx_legs_odds_type
  on legs(odds_type) where odds_type is not null and odds_type <> 'standard';

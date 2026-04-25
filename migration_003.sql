-- migration_003: enable richer calibration signals.
--
-- Two additive columns on market_observatory, both nullable / defaulted so this
-- migration is non-breaking. After applying, the pipeline starts populating the
-- new columns and the calibration refit picks them up automatically.
--
--   closing_prob — VWAP consensus probability captured at game start. Used as
--     a low-variance secondary calibration signal (see CLV_OBSERVATION_WEIGHT
--     in engine/isotonic_calibration.py).
--
--   books        — per-book devigged probabilities at log time, stored as a
--     compact JSON object: {"fanduel": 0.62, "draftkings": 0.61, "pinnacle":
--     0.62}. Required for empirical sharpness-weight refitting in
--     engine/sharpness_calibration.py.

alter table market_observatory
    add column if not exists closing_prob numeric;

alter table market_observatory
    add column if not exists books jsonb default '{}'::jsonb;

-- Optional: index pending rows that still need closing-prob backfill.
create index if not exists idx_observatory_pending_closing
    on market_observatory(game_start)
    where closing_prob is null and result = 'pending';

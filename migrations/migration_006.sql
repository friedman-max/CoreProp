-- migration_006: separate raw devig prob from calibrated prob, and persist
-- the per-row market width so the calibrator can weight observations by
-- confidence.
--
-- Background — the calibration feedback loop:
-- Until now, `market_observatory.true_prob` and `legs.true_prob` stored the
-- *calibrated* probability returned by `BetResult.true_prob` (see
-- engine/ev_calculator.py:85). The hierarchical isotonic refit then used
-- that same value as the x-axis when learning the empirical hit-rate
-- mapping. The result was a closed loop: the calibrator was training on
-- its own output, not on the raw consensus probability the consensus
-- engine produced. With the schema change below, we keep the calibrated
-- value in `true_prob` for display/auditing and write the pre-calibration
-- consensus probability into a new `raw_true_prob` column. The refit
-- reads `coalesce(raw_true_prob, true_prob)` so legacy rows (where the
-- two were the same anyway, since the live calibrator was off until the
-- bug was identified) continue to contribute.
--
-- `market_width` (overround % across the contributing books) is added so
-- the calibrator can weight tight, well-discovered markets more than wide,
-- noisy ones. NULL means "unknown / fall back to neutral weight".

alter table market_observatory
  add column if not exists raw_true_prob numeric;

alter table market_observatory
  add column if not exists market_width  numeric;

alter table legs
  add column if not exists raw_true_prob numeric;

-- Backfill: for every existing row, copy the (calibrated) true_prob into
-- raw_true_prob so the refit doesn't lose them. Going forward, new writes
-- distinguish the two values explicitly. NULL `raw_true_prob` is treated
-- by the refit as `true_prob` via COALESCE; the explicit backfill is
-- just so a future audit query can see the historical equivalence.
update market_observatory
  set raw_true_prob = true_prob
  where raw_true_prob is null
    and true_prob is not null;

update legs
  set raw_true_prob = true_prob
  where raw_true_prob is null
    and true_prob is not null;

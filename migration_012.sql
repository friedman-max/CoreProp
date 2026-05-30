-- migration_012: closing-line capture quality tracking.
--
-- Phase 1A audit found two CLV data-quality problems on the `legs` table
-- (the bets users actually place):
--
--   1. 18% of CLV-tracked legs have clv_pct EXACTLY 0.0 (closing_prob ==
--      true_prob). A real closing line almost never equals the bet-time
--      line to the 4th decimal — these are legs where the closing capture
--      effectively never fired after log time, so the bet-time value was
--      frozen as the "close." They masquerade as "0% CLV" and bias the
--      average toward zero.
--
--   2. There was no record of WHEN closing_prob was captured, so a value
--      written 6 hours pre-game (unreliable) was indistinguishable from
--      one written 5 minutes before tip (the real close).
--
-- These columns let the CLV tracker record capture time + lead, so the
-- analytics tab can report CLV *quality* (median capture lead) and
-- distinguish a genuinely-tracked close from a stale frozen one.
--
-- Additive + nullable: non-breaking. Historical rows stay NULL (we can't
-- reconstruct when their close was captured); going forward the CLV
-- tracker populates both on every write.

alter table legs
  add column if not exists closing_captured_at timestamptz;

alter table legs
  add column if not exists closing_lead_min numeric;   -- minutes before game_start at capture

-- Mirror on market_observatory so the training-corpus CLV (PR-3a capture
-- path) carries the same quality signal.
alter table market_observatory
  add column if not exists closing_captured_at timestamptz;

alter table market_observatory
  add column if not exists closing_lead_min numeric;

-- Index legs by capture recency so the analytics quality query is cheap.
create index if not exists idx_legs_closing_captured
  on legs(closing_captured_at)
  where closing_captured_at is not null;

do $$ begin
  raise notice 'migration_012 applied: closing_captured_at + closing_lead_min on legs + market_observatory';
end $$;

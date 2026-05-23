-- migration_009: track per-row availability windows on market_observatory.
--
-- Adds two timestamps:
--   first_seen_at — set on insert, never overwritten by a re-scrape
--   last_seen_at  — bumped every time the same market_key is scraped again
--
-- Why: the sandbox needs to replay BacktestLogger.try_log_slip() against
-- historical scrape moments. Without per-row availability windows, the
-- sandbox can't reconstruct "which legs were on the board at time T",
-- which means it can't reproduce what auto-backtest would actually have
-- done (the live builder only ever sees the current scrape's pool).
--
-- Implementation notes:
--   * Existing observatory writes use `upsert(..., ignore_duplicates=True)`
--     which silently skips conflicts. To capture last_seen_at we need
--     the conflict path to fire an UPDATE, but we also can't let that
--     UPDATE clobber the row's resolution state (a row already graded
--     'hit' must not revert to 'pending' on a re-scrape).
--   * Solution: a BEFORE UPDATE trigger that (a) preserves first_seen_at,
--     (b) bumps last_seen_at to NOW(), (c) freezes the columns that
--     downstream consumers treat as ground truth (result, stat_actual,
--     resolved_at, created_at). The scraper can then upsert freely
--     without worrying about overwriting state.
--
-- Backfill: legacy rows get first_seen_at = last_seen_at = created_at.
-- This is a single-instant observation — honest about the fact that we
-- don't actually know how long pre-migration legs stayed on the board.

alter table market_observatory
  add column if not exists first_seen_at timestamptz default now();

alter table market_observatory
  add column if not exists last_seen_at timestamptz default now();

-- One-shot backfill: anything pre-migration treated as a single-instant
-- observation at created_at. The COALESCE guards against rows missing
-- created_at (shouldn't exist but cheap insurance).
update market_observatory
   set first_seen_at = coalesce(created_at, now()),
       last_seen_at  = coalesce(created_at, now())
 where first_seen_at is null
    or last_seen_at  is null;

-- Index on first_seen_at: the sandbox replay sorts by it to walk scrape
-- moments in order. Without an index this is a sequential scan on every
-- /api/sandbox/run.
create index if not exists idx_observatory_first_seen_at
  on market_observatory(first_seen_at);

-- Index on last_seen_at supports range queries like "rows still on the
-- board after time T" used inside the replay loop.
create index if not exists idx_observatory_last_seen_at
  on market_observatory(last_seen_at);

-- ── State-preservation trigger ─────────────────────────────────────────
-- Fires on UPDATE (the upsert conflict path). Restores the columns that
-- represent durable state (set once and only mutated by results_checker
-- or by the insert defaults). Anything NOT listed here — true_prob,
-- raw_true_prob, market_width, books_probs, team — is allowed to update,
-- so the row always reflects the most recent scrape's prob/MW. (The
-- market_key locks the player|league|prop|line|side|game_start tuple, so
-- "the same market" really is the same offer between scrapes; prob
-- movement comes from consensus moving on the books, not a different bet.)
create or replace function market_observatory_upsert_guard()
returns trigger as $$
begin
  -- Preserve first_seen_at across re-scrapes.
  new.first_seen_at := old.first_seen_at;
  -- Always bump last_seen_at to the moment of this scrape.
  new.last_seen_at  := now();
  -- Freeze resolution state. results_checker is the only legitimate
  -- writer of these columns; a re-scrape coming in after grading must
  -- not roll them back to 'pending'/NULL.
  new.result        := old.result;
  new.stat_actual   := old.stat_actual;
  new.resolved_at   := old.resolved_at;
  new.created_at    := old.created_at;
  return new;
end;
$$ language plpgsql;

drop trigger if exists market_observatory_upsert_guard_trg
  on market_observatory;

create trigger market_observatory_upsert_guard_trg
  before update on market_observatory
  for each row
  execute function market_observatory_upsert_guard();

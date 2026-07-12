-- migration_005: add resolved_at cursor for incremental calibration refit
--
-- Both `market_observatory` and `legs` flip `result` from 'pending' to a
-- terminal value (hit/miss/won/lost/push/dnp) some time after row creation.
-- Without a column that tracks WHEN that flip happened, an incremental
-- calibration loader has no monotonic cursor on resolution events: it can
-- detect new rows via created_at, but not newly-resolved rows. This column
-- gives us that cursor so the calibration refit can fold in only the rows
-- that became resolved since the previous run.

alter table market_observatory
  add column if not exists resolved_at timestamptz;

alter table legs
  add column if not exists resolved_at timestamptz;

-- Backfill: any row that is already resolved gets a resolved_at set to the
-- best timestamp we have. created_at is the right shape on observatory; legs
-- doesn't have a created_at so we fall back to the slip's timestamp via a
-- join. Rows still pending stay NULL.
update market_observatory
  set resolved_at = coalesce(created_at, now())
  where resolved_at is null
    and result in ('hit', 'miss', 'push', 'dnp');

update legs
  set resolved_at = coalesce(
    (select s.timestamp from slips s where s.id = legs.slip_id),
    now()
  )
  where resolved_at is null
    and lower(result) in ('hit', 'miss', 'push', 'dnp', 'won', 'win', 'lost', 'loss');

-- Indexes for the calibration cursor query (`resolved_at > $1`).
create index if not exists idx_observatory_resolved_at
  on market_observatory(resolved_at)
  where resolved_at is not null;

create index if not exists idx_legs_resolved_at
  on legs(resolved_at)
  where resolved_at is not null;

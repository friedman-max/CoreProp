-- migration_017: let the results checker grade market_observatory rows.
--
-- ⚠️ OPTIONAL / FUTURE — like migration_016, this cannot be applied from the
-- current environment (no DDL access). Until it is applied,
-- engine/results_checker._write_observatory_result works around the problem
-- via DELETE + re-INSERT (BEFORE UPDATE triggers don't fire on INSERT).
-- Applying this migration makes plain UPDATEs work again; if you apply it,
-- the workaround becomes unnecessary but stays harmless.
--
-- Root cause: migration_009's market_observatory_upsert_guard trigger
-- unconditionally freezes result / stat_actual / resolved_at on EVERY update
-- to protect graded rows from scraper upserts. It cannot distinguish writers,
-- so it also silently reverted the results checker's legitimate grading —
-- observatory resolution died the day the trigger landed (~2026-05-24; the
-- table's max resolved_at is 2026-05-23).
--
-- Fix: allow the pending -> graded transition; keep every other protection.

create or replace function market_observatory_upsert_guard()
returns trigger as $$
begin
  -- Preserve first_seen_at across re-scrapes; bump last_seen_at.
  new.first_seen_at := old.first_seen_at;
  new.last_seen_at  := now();
  -- Resolution state: a row that is already graded stays frozen (scraper
  -- upserts must not roll it back to pending). A pending row may be graded.
  if old.result is distinct from 'pending' then
    new.result      := old.result;
    new.stat_actual := old.stat_actual;
    new.resolved_at := old.resolved_at;
  end if;
  new.created_at := old.created_at;
  return new;
end;
$$ language plpgsql;

do $$ begin
  raise notice 'migration_017 applied: observatory grading unblocked (pending -> graded transitions allowed)';
end $$;

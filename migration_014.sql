-- migration_014: repair the first_seen_at backfill artifact.
--
-- migration_009's backfill stamped every pre-existing market_observatory
-- row with the migration's own run timestamp (observed: every historical
-- row carries the identical value 2026-05-23T05:46:29Z — AFTER the games
-- those rows describe). Any availability-window analysis (placeability,
-- live-replay sandbox, time-to-tip studies) silently breaks on those rows:
-- "first seen" appears to be after kickoff, so no slip is ever feasible.
--
-- created_at is the true first-sight time for those rows (it was written
-- by the original insert). Repair rule: wherever first_seen_at is not
-- earlier than game_start but created_at is, restore created_at.

update market_observatory
   set first_seen_at = created_at
 where first_seen_at >= game_start
   and created_at    <  game_start;

-- last_seen_at sanity: never earlier than first_seen_at.
update market_observatory
   set last_seen_at = first_seen_at
 where last_seen_at is not null
   and last_seen_at < first_seen_at;

do $$ begin
  raise notice 'migration_014 applied: first_seen_at artifact repaired from created_at';
end $$;

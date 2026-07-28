-- migration_019: record the PrizePicks odds variant on market_observatory.
--
-- WHY (alpha audit, 2026-07-28):
--   market_observatory logs every priced side we scan, but NOT whether the
--   PrizePicks line was a `standard` 6x-table leg or a `goblin` (green devil).
--   Goblins are discounted, higher-hit-rate lines whose payout is VARIABLE
--   and is not published in the public projections feed — so a goblin can
--   never be scored against the standard Power/Flex payout table.
--
--   Without this column, every historical analysis silently pools the two.
--   That made the high-probability slice of the observatory look like a huge
--   untapped edge ("raw >= 0.65 hits 73%!") when in fact that slice is almost
--   entirely goblins, which are excluded from backtesting precisely because
--   their payout is unknown. The bettable standard universe behaves very
--   differently.
--
--   The `legs` table does not need this: the auto-logger already filters to
--   `odds_type == 'standard'` at log time, so every logged leg is standard by
--   construction.
--
-- Additive + nullable: non-breaking. Historical rows stay NULL (we cannot
-- reconstruct which variant they were); going forward the observatory writer
-- populates it on every upsert. Analyses over historical data must therefore
-- treat NULL as "unknown", not as "standard".

alter table market_observatory
  add column if not exists odds_type text;

-- Partial index: analyses almost always filter to the bettable standard
-- universe, so index that path rather than the whole column.
create index if not exists idx_observatory_odds_type
  on market_observatory(odds_type)
  where odds_type is not null;

do $$ begin
  raise notice 'migration_019 applied: odds_type on market_observatory';
end $$;

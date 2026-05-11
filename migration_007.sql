-- migration_007: persist team abbreviation per leg.
--
-- PrizePicks requires every slip to contain picks from at least two
-- distinct teams. To enforce this both at slip-construction time
-- (engine/backtest.BacktestLogger) and on historical replays
-- (engine/strategy_tester), every leg-shaped row needs to know which
-- team the player belongs to.
--
-- The PP API ships team abbreviation (e.g. "MIN", "LAL") in
-- `included.new_player.attributes.team`. The scraper now captures it,
-- and downstream layers (matcher, observatory log, legs insert) carry
-- it through.
--
-- Backfill: the API doesn't expose historical team membership for past
-- games we already logged without team info, so legacy rows stay NULL.
-- Slip construction treats NULL as "unknown" — see
-- engine/backtest._distinct_teams() and
-- engine/strategy_tester._distinct_teams() — and falls back to an
-- alternative grouping (player_id) so a slip with all-NULL teams can
-- still be sanity-checked rather than silently passing the rule.

alter table market_observatory
  add column if not exists team text;

alter table legs
  add column if not exists team text;

-- Index on (league, team) speeds up cross-slip team-conflict checks for
-- future enforcement queries (currently we read in-Python, but having
-- the index up front means migrations later can rely on it).
create index if not exists idx_observatory_team
  on market_observatory(league, team)
  where team is not null;

create index if not exists idx_legs_team
  on legs(league, team)
  where team is not null;

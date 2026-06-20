-- Green devils (PrizePicks goblins) auto-backtest opt-in.
--
-- Green-devil legs are NEVER auto-backtested unless the user explicitly turns
-- this on. When on, they're logged as their OWN separate slip (never mixed
-- into standard +EV slips). Default false preserves existing behavior for all
-- current users.
alter table user_config
  add column if not exists auto_backtest_green_devils boolean default false;

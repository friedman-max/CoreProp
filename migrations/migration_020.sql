-- migration_020: auto-placement of auto-backtested slips.
--
-- WHAT THIS ENABLES
--   A user can arm CoreProp to place a slip on PrizePicks automatically, with
--   no further interaction, the moment auto-backtest logs it. This moves real
--   money without a human in the loop, so every column below exists to bound
--   the blast radius rather than to add features.
--
-- WHY THE RAILS ARE IN THE DATABASE, NOT THE CLIENT
--   The placing agent is a browser extension the user can open devtools on.
--   Stake caps, daily limits and the arm state are therefore authoritative
--   server-side: the extension asks what it is allowed to do and the server
--   decides. A tampered client can still misbehave on the PrizePicks side, but
--   it cannot talk CoreProp into handing it a larger stake than the row allows.
--
-- MODES
--   'off'   — never place (default for every existing and new user)
--   'paper' — do everything except click submit; record what WOULD have gone
--             through. This is the mandatory proving state: it is how you
--             confirm the bot targets the stake field rather than the adjacent
--             "to win" field, which would otherwise place a wrong-sized wager.
--   'live'  — actually submit.
--
-- Additive and defaulted, so applying this changes no existing behaviour: every
-- user lands in 'off'.

alter table user_config
  add column if not exists auto_place_mode        text    default 'off',
  -- Stake written into PrizePicks' entry box, in dollars. Default 1.
  add column if not exists auto_place_stake       numeric default 1,
  -- Refuse any single placement above this, regardless of what the client asks.
  add column if not exists auto_place_max_stake   numeric default 5,
  -- Cumulative stake allowed per UTC day before the bot disarms itself.
  add column if not exists auto_place_daily_cap   numeric default 25,
  -- Consecutive failures before auto-disarm. A bot that cannot read the board
  -- should stop, not keep firing at it.
  add column if not exists auto_place_fail_streak int     default 0,
  -- Set when the user accepted the "this places real bets" disclaimer. Null
  -- means they never consented and the mode must be treated as 'off'.
  add column if not exists auto_place_consent_at  timestamptz;

alter table user_config drop constraint if exists user_config_auto_place_mode_chk;
alter table user_config add constraint user_config_auto_place_mode_chk
  check (auto_place_mode in ('off', 'paper', 'live'));

-- Caps must be sane. A null or negative cap would read as "unlimited" in any
-- naive comparison, which is the last thing you want guarding real money.
alter table user_config drop constraint if exists user_config_auto_place_stake_chk;
alter table user_config add constraint user_config_auto_place_stake_chk
  check (auto_place_stake > 0 and auto_place_stake <= 1000);

alter table user_config drop constraint if exists user_config_auto_place_max_chk;
alter table user_config add constraint user_config_auto_place_max_chk
  check (auto_place_max_stake > 0 and auto_place_max_stake <= 1000);

alter table user_config drop constraint if exists user_config_auto_place_cap_chk;
alter table user_config add constraint user_config_auto_place_cap_chk
  check (auto_place_daily_cap >= 0 and auto_place_daily_cap <= 10000);


-- Audit trail. One row per ATTEMPT, not per success — the failures are the
-- interesting ones, and this is the only record that can answer "why was this
-- bet placed on my account" if a user ever disputes one.
create table if not exists auto_place_log (
  id             uuid primary key default gen_random_uuid(),
  user_id        uuid not null references auth.users(id) on delete cascade,
  slip_id        text,
  created_at     timestamptz not null default now(),
  mode           text not null,                  -- 'paper' | 'live' at attempt time
  status         text not null,                  -- placed | skipped | failed | capped | disarmed
  reason         text,                           -- human-readable, e.g. 'line moved 27.5 -> 28.5'
  stake          numeric,                        -- what was actually entered
  legs_total     int,
  legs_staged    int,
  -- What the board actually showed, so a dispute can be reconstructed without
  -- trusting the client's summary.
  detail         jsonb default '{}'::jsonb,
  extension_ver  text
);

create index if not exists idx_auto_place_log_user_time
  on auto_place_log(user_id, created_at desc);

-- Daily-cap lookups scan today's placed rows per user.
create index if not exists idx_auto_place_log_user_placed
  on auto_place_log(user_id, created_at)
  where status = 'placed';

alter table auto_place_log enable row level security;

drop policy if exists auto_place_log_own_rows on auto_place_log;
create policy auto_place_log_own_rows on auto_place_log
  for select using (auth.uid() = user_id);

-- Writes go through the service role only. The extension reports results via
-- the CoreProp API, which validates them; it must not be able to forge log
-- rows directly (that would let a tampered client hide a placement or fake a
-- cap reset).

do $$ begin
  raise notice 'migration_020 applied: auto-placement config + audit log';
end $$;

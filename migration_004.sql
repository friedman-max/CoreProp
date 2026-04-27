-- Per-user slip preferences for Auto-Backtest.
-- The Slip Builder UI lets users pick the slip type (Power vs Flex) and the
-- number of legs (2-6). When Auto-Backtest is on, the background worker
-- builds slips that match these preferences instead of the hardcoded
-- 6-leg Power default.

alter table user_config
  add column if not exists auto_slip_type     text    default 'Power',
  add column if not exists auto_slip_legs     int     default 6,
  add column if not exists auto_slip_min_prob numeric default 0.5407;

alter table user_config
  add constraint user_config_auto_slip_type_chk
    check (auto_slip_type in ('Power', 'Flex'));

alter table user_config
  add constraint user_config_auto_slip_legs_chk
    check (auto_slip_legs between 2 and 6);

-- Flex requires at least 3 legs (a 2-leg Flex degenerates to Power).
alter table user_config
  add constraint user_config_flex_min_legs_chk
    check (auto_slip_type <> 'Flex' or auto_slip_legs >= 3);

-- Min per-leg probability must be a sane probability.
alter table user_config
  add constraint user_config_auto_slip_min_prob_chk
    check (auto_slip_min_prob is null or (auto_slip_min_prob > 0 and auto_slip_min_prob < 1));

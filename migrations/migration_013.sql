-- migration_013: Phase 3 strategy comparison logging.
--
-- The strategy comparison harness (analysis/strategy_compare.py) replays
-- the production pipeline through three configurations and writes daily
-- aggregate metrics here. Source-of-truth for which branch is most
-- profitable over the next 7 days of live data — answers the user's
-- explicit acceptance criterion: "ensure the logger captures expected
-- value, actual Kelly growth, and variance to definitively prove which
-- branch is more accurate and profitable."

create table if not exists strategy_performance_compare (
  id              bigserial primary key,
  recorded_at     timestamptz default now(),
  scoped_at       date        not null,                -- the date being scored
  branch          text        not null,                -- 'baseline' | 'holy' | 'maybe'

  -- Per-leg metrics (mean over n_legs scored)
  n_legs          int         not null,                -- legs that passed the branch's filter
  mean_pred_prob  numeric,                             -- mean of calibrated probabilities
  mean_obs_hit    numeric,                             -- mean of resolved outcomes (hit rate)
  mean_clv_pct    numeric,                             -- mean closing-line value
  beat_close_rate numeric,                             -- share with CLV > 0
  brier           numeric,                             -- Brier score on resolved
  log_loss        numeric,

  -- Slip-level metrics (mean over n_slips simulated)
  n_slips         int,
  mean_slip_ev    numeric,                             -- predicted EV per slip
  realized_roi    numeric,                             -- realized P/L per unit stake
  win_rate        numeric,
  max_drawdown    numeric,

  -- Kelly growth: log-wealth at end of period if you'd followed this branch
  log_wealth_end  numeric,                             -- ln(bankroll / starting_bankroll)
  kelly_variance  numeric,                             -- variance of log-wealth increment
  sharpe_ratio    numeric,                             -- mean / std of unit-stake returns

  -- Tier breakdown JSON {tier_A: {n, hit_rate, ...}, tier_B: ..., ...}
  tier_breakdown  jsonb,

  -- Comparison metadata
  config_snapshot jsonb,                               -- env vars / config used
  notes           text,

  unique (scoped_at, branch)
);

create index if not exists idx_strategy_perf_scoped_at
  on strategy_performance_compare(scoped_at desc);

create index if not exists idx_strategy_perf_branch_scoped
  on strategy_performance_compare(branch, scoped_at desc);

do $$ begin
  raise notice 'migration_013 applied: strategy_performance_compare table for Phase 3 A/B logging';
end $$;

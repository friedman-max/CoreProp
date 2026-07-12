-- migration_010: per-cell calibration state for RWBC (Reliability-Weighted
-- Bayesian Calibration). Additive only — safe to apply alongside the
-- existing isotonic calibration code path.
--
-- Two new tables:
--
--   calibration_cells     — one row per (league, prop, side) cell, holding
--                            the most recently *gated* fit. This is what
--                            engine/rwbc_calibration.calibrate() reads at
--                            inference time.
--
--   calibration_history   — audit trail; every refit attempt captures
--                            Brier (current + RWBC) so the Observatory
--                            trend panel can plot the 30-day sparkline
--                            without recomputing each time.
--
-- The publication gate (Δn_eff ≥ 50 since last publish) sits on the
-- application side — the SQL layer just holds whatever the gate last
-- committed. `last_publish_n_eff` is the cursor the gate compares
-- against on the next refit.
--
-- No RLS is needed: only the service-role refit job writes these tables,
-- and the GET /api/observatory/rwbc endpoint reads them via the service
-- role too (Observatory data is non-PII).

create table if not exists calibration_cells (
  league               text       not null,
  prop                 text       not null,
  side                 text       not null,                -- 'over' | 'under'
  w_cell               numeric    not null,                -- trust weight in [0, 1]
  p_post               numeric    not null,                -- Beta-Binomial posterior mean
  n_eff                numeric    not null,                -- recency-weighted obs count
  resolution           numeric    not null,
  reliability_error    numeric    not null,
  mean_pred            numeric    not null,
  mean_obs             numeric    not null,
  last_fit_at          timestamptz default now(),          -- last RECOMPUTE
  last_publish_at      timestamptz default now(),          -- last PUBLISH (gated)
  last_publish_n_eff   numeric    not null,                -- n_eff at last publish
  primary key (league, prop, side)
);

create index if not exists idx_cal_cells_w_cell on calibration_cells(w_cell);

create table if not exists calibration_history (
  id                   bigserial  primary key,
  fit_at               timestamptz default now(),
  scope                text       not null,                -- 'global' | <league>
  brier_current        numeric,                            -- isotonic Brier (when computed)
  brier_rwbc           numeric,                            -- RWBC Brier
  n_settled            int        not null,                -- sample size feeding this fit
  publish_skipped      boolean    default false            -- true if n_eff gate suppressed publish
);

create index if not exists idx_cal_history_recent
  on calibration_history(fit_at desc);

-- One-time visibility: emit a notice so the runner sees the migration ran.
do $$ begin raise notice 'migration_010 applied: calibration_cells + calibration_history'; end $$;

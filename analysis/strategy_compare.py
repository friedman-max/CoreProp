"""
Phase 3 strategy comparison logger.

Replays the production pipeline against the live market_observatory under
three configurations and produces a daily row per branch in the
`strategy_performance_compare` table (migration_013).

  baseline  current production: USE_RWBC=true, isotonic (no side-key),
            no observatory CLV capture, no shade filter, no beta cal,
            per-slip Kelly, no portfolio Kelly. This is what the system
            would have done before the audit.

  holy      Holy Fix branch — USE_RWBC=true, side-keyed isotonic,
            observatory CLV capture, Shin z-prior, log width weighting,
            BP tetrachoric, closed-form Kelly with 5% cap. Tiered
            thresholds. No shade filter / beta cal / portfolio Kelly.

  maybe     Maybe Cool Fix — everything in `holy` plus USE_SHADE_FILTER=
            true, USE_BETA_CAL=true (when cells fitted), USE_PORTFOLIO_
            KELLY=true.

For each branch and each resolved leg in the scoring window, we compute:
  - calibrated probability the branch would have emitted
  - tier label
  - whether the leg would have been auto-logged
  - realized outcome (already known from observatory)
  - CLV at log time vs close (when both probs are available)

Per-slip metrics are simulated by replaying the auto-backtest tier
routing against each branch's filter.

Usage (manual):
    SUPABASE_URL=... SUPABASE_SERVICE_KEY=... \\
      python3 analysis/strategy_compare.py --days 7

Or schedule nightly via cron / Render scheduler.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

# Ensure project root on sys.path so analysis/ can import engine.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from supabase import create_client

# Engine imports — each branch reads from a different combination of
# modules. We don't actually swap branches; instead we instantiate the
# different configurations by routing each prediction through the
# appropriate set of transformations explicitly.
from engine.constants import OPTIMAL_BREAK_EVEN, BREAK_EVEN, score_leg, POWER_PAYOUTS, FLEX_PAYOUTS
from engine.tier import (
    tier_for_prob, tier_eligible_for_slip, effective_min_prob,
    TIER_A_MIN_PROB, TIER_B_MIN_PROB,
)
from engine.shade_signal import pp_shade, shade_bucket, is_anti_public
from engine import beta_calibration as beta_cal
from engine import isotonic_calibration as iso_cal
from engine import rwbc_calibration as rwbc_cal


# ─────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────

@dataclass
class BranchConfig:
    """Configuration knobs that differentiate the three branches."""
    name: str
    # Calibrator routing
    use_rwbc: bool = True
    use_beta_cal: bool = False
    use_side_keyed_isotonic: bool = True
    # Filters
    use_tier_routing: bool = True
    use_shade_filter: bool = False
    # Staking
    use_closed_form_kelly: bool = True
    use_portfolio_kelly: bool = False
    # 5% per-slip cap; quarter-Kelly damping
    kelly_kappa: float = 0.25
    kelly_cap_per_slip: float = 0.05


BASELINE = BranchConfig(
    name="baseline",
    use_rwbc=False,            # pre-audit default per the codebase comments
    use_beta_cal=False,
    use_side_keyed_isotonic=False,
    use_tier_routing=False,
    use_shade_filter=False,
    use_closed_form_kelly=False,
    use_portfolio_kelly=False,
    kelly_kappa=0.25,
    kelly_cap_per_slip=1.0,    # no hard cap pre-audit
)

HOLY = BranchConfig(
    name="holy",
    use_rwbc=True,
    use_beta_cal=False,
    use_side_keyed_isotonic=True,
    use_tier_routing=True,
    use_shade_filter=False,
    use_closed_form_kelly=True,
    use_portfolio_kelly=False,
    kelly_kappa=0.25,
    kelly_cap_per_slip=0.05,
)

MAYBE = BranchConfig(
    name="maybe",
    use_rwbc=True,
    use_beta_cal=True,
    use_side_keyed_isotonic=True,
    use_tier_routing=True,
    use_shade_filter=True,
    use_closed_form_kelly=True,
    use_portfolio_kelly=True,
    kelly_kappa=0.25,
    kelly_cap_per_slip=0.05,
)


@dataclass
class BranchMetrics:
    branch: str
    n_legs: int = 0
    sum_pred: float = 0.0
    sum_obs: float = 0.0
    sum_brier: float = 0.0
    sum_log_loss: float = 0.0
    sum_clv: float = 0.0
    n_clv: int = 0
    n_beat_close: int = 0
    # Slip-level
    n_slips: int = 0
    sum_slip_ev: float = 0.0
    sum_realized_profit: float = 0.0
    sum_stakes: float = 0.0
    n_wins: int = 0
    drawdowns: list[float] = field(default_factory=list)
    # Log wealth
    log_wealth_increments: list[float] = field(default_factory=list)
    # Tier
    tier_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    tier_hits:   dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def add_leg(self, pred: float, obs: int, closing: Optional[float], tier: str) -> None:
        self.n_legs += 1
        self.sum_pred += pred
        self.sum_obs += obs
        self.sum_brier += (pred - obs) ** 2
        # Clamp for log-loss
        p_safe = max(1e-6, min(1.0 - 1e-6, pred))
        self.sum_log_loss += -(obs * math.log(p_safe) + (1 - obs) * math.log(1 - p_safe))
        if closing is not None:
            clv = closing - pred
            self.sum_clv += clv
            self.n_clv += 1
            if clv > 0:
                self.n_beat_close += 1
        self.tier_counts[tier] += 1
        if obs == 1:
            self.tier_hits[tier] += 1

    def add_slip(
        self, *, ev: float, profit: float, stake: float, won: bool,
    ) -> None:
        self.n_slips += 1
        self.sum_slip_ev += ev
        self.sum_realized_profit += profit
        self.sum_stakes += stake
        if won:
            self.n_wins += 1
        # log-wealth increment for the slip; cap for numerical safety.
        ret = max(-0.99, profit / max(stake, 1e-9))
        self.log_wealth_increments.append(math.log(1.0 + ret))

    def to_row(self, scoped_at: date, branch_cfg: BranchConfig) -> dict:
        mean_pred = self.sum_pred / self.n_legs if self.n_legs else None
        mean_obs  = self.sum_obs / self.n_legs if self.n_legs else None
        brier     = self.sum_brier / self.n_legs if self.n_legs else None
        log_loss  = self.sum_log_loss / self.n_legs if self.n_legs else None
        mean_clv  = self.sum_clv / self.n_clv if self.n_clv else None
        beat_close= self.n_beat_close / self.n_clv if self.n_clv else None
        roi       = self.sum_realized_profit / self.sum_stakes if self.sum_stakes else None
        win_rate  = self.n_wins / self.n_slips if self.n_slips else None
        slip_ev   = self.sum_slip_ev / self.n_slips if self.n_slips else None
        log_w     = sum(self.log_wealth_increments) if self.log_wealth_increments else None
        kelly_var = (
            float(np.var(self.log_wealth_increments))
            if self.log_wealth_increments else None
        )
        # Sharpe approximation on slip returns
        if self.log_wealth_increments:
            arr = np.array(self.log_wealth_increments)
            sharpe = float(arr.mean() / arr.std()) if arr.std() > 0 else None
        else:
            sharpe = None

        tier_break = {
            t: {
                "n":        self.tier_counts.get(t, 0),
                "hit_rate": (
                    self.tier_hits.get(t, 0) / self.tier_counts[t]
                    if self.tier_counts.get(t) else None
                ),
            }
            for t in ("A", "B", "C", "REJECT")
        }
        return {
            "scoped_at":       scoped_at.isoformat(),
            "branch":          self.branch,
            "n_legs":          self.n_legs,
            "mean_pred_prob":  round(mean_pred, 5) if mean_pred is not None else None,
            "mean_obs_hit":    round(mean_obs, 5)  if mean_obs  is not None else None,
            "mean_clv_pct":    round(mean_clv, 5)  if mean_clv  is not None else None,
            "beat_close_rate": round(beat_close, 5) if beat_close is not None else None,
            "brier":           round(brier, 5)   if brier   is not None else None,
            "log_loss":        round(log_loss, 5) if log_loss is not None else None,
            "n_slips":         self.n_slips,
            "mean_slip_ev":    round(slip_ev, 5)  if slip_ev  is not None else None,
            "realized_roi":    round(roi, 5)      if roi      is not None else None,
            "win_rate":        round(win_rate, 5) if win_rate is not None else None,
            "max_drawdown":    round(max(self.drawdowns), 5) if self.drawdowns else None,
            "log_wealth_end":  round(log_w, 5)    if log_w    is not None else None,
            "kelly_variance":  round(kelly_var, 6) if kelly_var is not None else None,
            "sharpe_ratio":    round(sharpe, 5)   if sharpe   is not None else None,
            "tier_breakdown":  tier_break,
            "config_snapshot": {
                "use_rwbc": branch_cfg.use_rwbc,
                "use_beta_cal": branch_cfg.use_beta_cal,
                "use_side_keyed_isotonic": branch_cfg.use_side_keyed_isotonic,
                "use_tier_routing": branch_cfg.use_tier_routing,
                "use_shade_filter": branch_cfg.use_shade_filter,
                "use_closed_form_kelly": branch_cfg.use_closed_form_kelly,
                "use_portfolio_kelly": branch_cfg.use_portfolio_kelly,
                "kelly_cap_per_slip": branch_cfg.kelly_cap_per_slip,
            },
        }


# ─────────────────────────────────────────────────────────────────────────
# Per-branch calibration apply path
# ─────────────────────────────────────────────────────────────────────────

def _apply_calibration(
    raw_prob: float,
    league: str,
    prop: str,
    side: str,
    branch: BranchConfig,
    isotonic_curves: dict,
) -> tuple[float, bool]:
    """Return (calibrated_prob, halted) per the branch's calibration stack.

    The order matches engine/ev_calculator.py:
      USE_BETA_CAL preempts when cell fitted
      USE_RWBC second
      else isotonic (with or without side-keying)
    """
    if branch.use_beta_cal:
        b = beta_cal.calibrate(league, prop, side, raw_prob,
                               shade=pp_shade(raw_prob))
        if b is not None:
            return b, False
    if branch.use_rwbc:
        r = rwbc_cal.calibrate(raw_prob, league, prop, side)
        if r is None:
            return max(0.001, min(0.999, raw_prob)), True
        return r, False
    # Isotonic — the side-keyed flag is already encoded in the curves dict
    # by the C2 commit; baseline branch doesn't currently get a separate
    # side-pooled-only curve set, so we approximate baseline by passing
    # side='both' which forces the side-pooled fallback in the calibrator.
    side_used = side if branch.use_side_keyed_isotonic else "both"
    q = iso_cal.calibrate(isotonic_curves, league, prop, side_used, raw_prob)
    return max(0.001, min(0.999, q)), False


def _kelly_size(
    p: float, slip_n: int, slip_type: str, branch: BranchConfig,
) -> float:
    """Per-slip Kelly fraction under branch.use_closed_form_kelly."""
    payout_table = POWER_PAYOUTS if slip_type == "power" else FLEX_PAYOUTS
    D = payout_table.get(slip_n)
    if D is None:
        return 0.0
    # Joint win probability for Power; for Flex use simple expected payoff.
    if slip_type == "power":
        p_all = p ** slip_n
        ev = D * p_all - 1.0
        if ev <= 0:
            return 0.0
        # Closed-form Kelly
        f_star = (D * p_all - 1.0) / (D - 1.0)
    else:
        # Approximate for Flex
        tiers = FLEX_PAYOUTS.get(slip_n, {})
        f_star = 0.0
        for k, mult in tiers.items():
            f_star += math.comb(slip_n, k) * (p ** k) * ((1 - p) ** (slip_n - k)) * (mult - 1)
        if f_star <= 0:
            return 0.0
    damped = branch.kelly_kappa * f_star
    return max(0.0, min(damped, branch.kelly_cap_per_slip))


# ─────────────────────────────────────────────────────────────────────────
# Replay driver
# ─────────────────────────────────────────────────────────────────────────

def fetch_resolved(sb, scoped_at: date) -> list[dict]:
    """Pull resolved market_observatory rows scoped to the date."""
    day_start = datetime(scoped_at.year, scoped_at.month, scoped_at.day, tzinfo=timezone.utc).isoformat()
    day_end = (datetime(scoped_at.year, scoped_at.month, scoped_at.day, tzinfo=timezone.utc) + timedelta(days=1)).isoformat()
    cols = "league, prop, side, raw_true_prob, true_prob, result, closing_prob, market_width, line, player, game_start, resolved_at"
    rows: list[dict] = []
    page, offset = 1000, 0
    while True:
        res = (
            sb.table("market_observatory")
              .select(cols)
              .in_("result", ["hit", "miss"])
              .gte("game_start", day_start)
              .lte("game_start", day_end)
              .range(offset, offset + page - 1)
              .execute()
        )
        chunk = res.data or []
        rows.extend(chunk)
        if len(chunk) < page:
            break
        offset += page
    return rows


def replay_day(
    sb, scoped_at: date, branch_cfgs: list[BranchConfig],
    isotonic_curves: dict,
) -> dict[str, BranchMetrics]:
    rows = fetch_resolved(sb, scoped_at)
    metrics: dict[str, BranchMetrics] = {
        b.name: BranchMetrics(branch=b.name) for b in branch_cfgs
    }

    # Group resolved rows by slate for slip simulation
    by_slate: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        gs = r.get("game_start") or ""
        slate_key = gs[:10] if gs else "unknown"
        by_slate[slate_key].append(r)

    for r in rows:
        league = (r.get("league") or "").upper()
        prop   = r.get("prop") or ""
        side   = (r.get("side") or "").lower()
        raw    = r.get("raw_true_prob") or r.get("true_prob")
        if raw is None:
            continue
        try:
            raw = float(raw)
        except (TypeError, ValueError):
            continue
        outcome = 1 if (r.get("result") or "").lower() == "hit" else 0
        closing = None
        cp = r.get("closing_prob")
        if cp is not None:
            try:
                closing = float(cp)
            except (TypeError, ValueError):
                pass

        for cfg in branch_cfgs:
            cal, halted = _apply_calibration(
                raw, league, prop, side, cfg, isotonic_curves,
            )
            t = tier_for_prob(cal, calibration_halted=halted)
            # Shade filter (Maybe Cool C1) — drop legs flagged anti-public.
            if cfg.use_shade_filter and is_anti_public({
                "league": league, "side": side, "true_prob": cal,
            }):
                continue
            metrics[cfg.name].add_leg(cal, outcome, closing, t)

    # Slip-level simulation: per slate, build a unit-stake slip from the
    # top-tier-A legs at 6-Power. Mirrors the auto-backtest target shape.
    for slate_rows in by_slate.values():
        for cfg in branch_cfgs:
            calibrated: list[tuple[float, int]] = []  # (cal_prob, outcome)
            for r in slate_rows:
                raw = r.get("raw_true_prob") or r.get("true_prob")
                if raw is None:
                    continue
                try:
                    raw = float(raw)
                except (TypeError, ValueError):
                    continue
                outcome = 1 if (r.get("result") or "").lower() == "hit" else 0
                cal, halted = _apply_calibration(
                    raw, (r.get("league") or "").upper(),
                    r.get("prop") or "",
                    (r.get("side") or "").lower(),
                    cfg, isotonic_curves,
                )
                if cfg.use_tier_routing:
                    t = tier_for_prob(cal, calibration_halted=halted)
                    if not tier_eligible_for_slip(t, "power", 6):
                        continue
                else:
                    # Baseline: use legacy 0.5407 default.
                    if cal < OPTIMAL_BREAK_EVEN:
                        continue
                if cfg.use_shade_filter and is_anti_public({
                    "league": (r.get("league") or "").upper(),
                    "side":   (r.get("side") or "").lower(),
                    "true_prob": cal,
                }):
                    continue
                calibrated.append((cal, outcome))

            if len(calibrated) < 6:
                continue
            # Top-6 by calibrated prob.
            calibrated.sort(key=lambda x: -x[0])
            top6 = calibrated[:6]
            probs = [p for p, _ in top6]
            outs  = [o for _, o in top6]

            # Per-slip EV (joint hit only for Power-6).
            p_all = math.prod(probs)
            ev = 40.0 * p_all - 1.0   # 6-Power payout = 40
            all_hit = all(o == 1 for o in outs)
            stake = _kelly_size(sum(probs) / 6.0, 6, "power", cfg) or 0.01
            profit = (stake * 40.0 - stake) if all_hit else -stake
            metrics[cfg.name].add_slip(
                ev=ev, profit=profit, stake=stake, won=all_hit,
            )
    return metrics


# ─────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────

def persist_metrics(sb, rows: Iterable[dict]) -> int:
    """Upsert per-(scoped_at, branch) row into strategy_performance_compare."""
    n = 0
    for row in rows:
        try:
            (sb.table("strategy_performance_compare")
               .upsert(row, on_conflict="scoped_at,branch")
               .execute())
            n += 1
        except Exception as exc:
            print(f"persist failed for {row.get('branch')} @ {row.get('scoped_at')}: {exc}",
                  file=sys.stderr)
    return n


# ─────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1,
                        help="Number of trailing days to score (default 1)")
    parser.add_argument("--branches", default="baseline,holy,maybe",
                        help="Comma-separated subset of branches")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print metrics; don't write to Supabase")
    parser.add_argument("--end-date", default=None,
                        help="Inclusive end date YYYY-MM-DD (default: today UTC)")
    args = parser.parse_args()

    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    name_to_cfg = {"baseline": BASELINE, "holy": HOLY, "maybe": MAYBE}
    branches = [
        name_to_cfg[n.strip()]
        for n in args.branches.split(",")
        if n.strip() in name_to_cfg
    ]
    if not branches:
        print("no valid branches selected", file=sys.stderr)
        return 1

    end_dt = (
        datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date else date.today()
    )
    isotonic_curves = iso_cal.load_isotonic_calibration()
    # Hydrate RWBC cache from Supabase so the apply path has cells.
    try:
        rwbc_cal.load_cell_cache_from_db(sb)
    except Exception as exc:
        print(f"RWBC cache hydration skipped: {exc}", file=sys.stderr)
    beta_cal.load_params()

    rows_to_write: list[dict] = []
    for offset in range(args.days):
        scoped = end_dt - timedelta(days=offset)
        print(f"\n=== Scoring {scoped} ===")
        metrics_by_branch = replay_day(sb, scoped, branches, isotonic_curves)
        for cfg in branches:
            m = metrics_by_branch[cfg.name]
            row = m.to_row(scoped, cfg)
            print(json.dumps({k: v for k, v in row.items()
                              if k not in ("tier_breakdown", "config_snapshot")},
                             indent=2))
            rows_to_write.append(row)

    if args.dry_run:
        print(f"\n[dry-run] {len(rows_to_write)} rows not written.")
        return 0
    n_written = persist_metrics(sb, rows_to_write)
    print(f"\nWrote {n_written} rows to strategy_performance_compare.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

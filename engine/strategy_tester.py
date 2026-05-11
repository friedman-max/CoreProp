"""
Strategy Sandbox: historical-performance evaluator for tracked props.

Pipeline (per `run_simulation` call):
  1. Pull resolved observatory rows (paged) including raw_true_prob, side,
     market_width.
  2. Apply the *current* isotonic calibration to raw_true_prob so the
     reported numbers reflect what today's pipeline would have produced
     on past raw markets — not whatever calibration vintage was logged
     at the time.
  3. Apply user filters (date range, leagues, prop includes/excludes,
     min_prob on the calibrated value).
  4. Group rows into slates (one per calendar day).
  5. Build slips per the chosen `slip_strategy`:
       top_prob       — top-N by calibrated_prob (legacy default)
       top_ev         — top-N by individual EV%
       top_ev_capped  — top-N by EV%, with a per-(league, game_date) cap
       live_replay    — mirrors engine/backtest.BacktestLogger:
                        EV-sorted, per-game cap, player-game dedup
  6. Simulate each slip with full PrizePicks push/dnp semantics: pushes
     and DNPs reduce the effective slip size and refund proportionally,
     matching the live API at web/app.py:2446-2458.
  7. Bootstrap (N=BOOTSTRAP_RESAMPLES) the slip set to produce 95% CIs
     on aggregate ROI, win-rate, max-drawdown.
  8. Build per-(league, prop, hits) breakdowns with bootstrap CI on each
     row; flag rows with n_slips < MIN_BUCKET_SLIPS.
  9. Emit a `funnel` diagnostic that reports row counts at every filter
     step plus push/dnp leg counts in formed slips, so the user can
     see exactly how many observations survived to inform the report.

The legacy `optimize_threshold` function is preserved but rewired to use
the calibration-on-the-fly path so its output is consistent with the
main report.
"""
import logging
import random
from typing import List, Dict, Optional, Iterable, Tuple
from datetime import datetime, timezone, date
from dataclasses import dataclass, field

import pandas as pd
import numpy as np

from engine.database import get_db
from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS, OPTIMAL_IMPLIED_DECIMAL
from engine.isotonic_calibration import load_isotonic_calibration, calibrate
from engine.backtest import make_bet_key, make_game_key, make_leg_key

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────

# PrizePicks rule: every slip must contain picks from at least two
# distinct teams. Players from opposing teams in the same game count as
# two. Enforced in `_enforce_two_teams()` below; supersedes the v2
# per-game cap that used to live here.
TWO_TEAMS_REQUIRED = 2

# Distinctness fallback: when a leg has no team value (legacy data
# pre-migration_007), use this prefix on player as the team identifier
# so the rule still has a sane meaning rather than silently passing
# every slip.
_FALLBACK_TEAM_PREFIX = "player:"

# Bootstrap sample count. 500 is enough for stable 95% CIs on n>=50 slips
# while keeping the simulation under ~1s for typical filter sets.
BOOTSTRAP_RESAMPLES = 500

# Minimum slips a per-(league, prop) breakdown bucket needs to qualify
# for a non-flagged row. Below this threshold the breakdown row is
# returned with `is_thin: True` so the UI can grey it out.
MIN_BUCKET_SLIPS = 20

# Bootstrap RNG seed. Deterministic so re-runs produce identical CIs;
# operational sandboxing wants reproducibility, not novel randomness.
_BOOTSTRAP_SEED = 0xC0DE


# ── Config ───────────────────────────────────────────────────────────────

@dataclass
class StrategyConfig:
    leagues: List[str] = field(default_factory=list)
    min_prob: float = 0.5408                    # break-even default
    slip_size: int = 6                          # 2..6
    slip_type: str = "flex"                     # "power" | "flex"
    bankroll: float = 100.0
    bet_size: float = 1.0
    excluded_props: List[str] = field(default_factory=list)
    included_props: List[str] = field(default_factory=list)
    use_calibration: bool = True
    use_kelly: bool = False

    # New (sandbox v2) fields:
    slip_strategy: str = "live_replay"          # see module docstring
    start_date: Optional[str] = None            # YYYY-MM-DD inclusive
    end_date:   Optional[str] = None            # YYYY-MM-DD inclusive
    bootstrap:  bool = True                     # toggle CI computation


# ── Data fetch ───────────────────────────────────────────────────────────

class StrategyTester:
    def __init__(self):
        self.db = get_db()
        # Load the live calibration curves once per simulation. The same
        # curves apply to every row; loading them per-row would re-read
        # the JSON ~10K times.
        self._curves: dict = load_isotonic_calibration()

    def reload_calibration(self) -> None:
        """Re-read isotonic_calibration.json. Call this between simulations
        if a refit ran in between (e.g. from /api/calibration/refit)."""
        self._curves = load_isotonic_calibration()

    def _fetch_resolved_observatory(self, leagues: List[str]) -> pd.DataFrame:
        """Page through every resolved market_observatory row matching the
        league filter. Pulls raw_true_prob and side too — raw_true_prob
        is the pre-calibration consensus probability we re-calibrate
        with the current curves, and `side` is needed for the per-(L,
        prop, side) bucket lookup."""
        # Three column tiers, tried in order on the first page. The fallback
        # is graceful per-migration: if migration_007 (team) isn't applied
        # we still get raw_true_prob/market_width from migration_006; if
        # neither is applied we drop to the v1 set. Switching tiers only
        # happens at offset 0 — once a tier succeeds, subsequent pages
        # use the same column list.
        cols_v3 = (
            "result, prop, true_prob, raw_true_prob, side, "
            "game_start, league, player, market_width, team"
        )
        cols_v2 = (
            "result, prop, true_prob, raw_true_prob, side, "
            "game_start, league, player, market_width"
        )
        cols_v1 = "result, prop, true_prob, side, game_start, league, player"
        tier_order = [cols_v3, cols_v2, cols_v1]

        PAGE = 1000
        offset = 0
        all_rows: list[dict] = []
        active_cols = tier_order[0]
        while True:
            try:
                q = self.db.table("market_observatory").select(active_cols).neq("result", "pending")
                if leagues:
                    q = q.in_("league", leagues)
                res = q.range(offset, offset + PAGE - 1).execute()
            except Exception as exc:
                # On the first page only, step down to the next column tier
                # rather than aborting. Lets the sandbox keep working on
                # deploys that haven't applied the latest migration.
                if offset == 0 and active_cols in tier_order:
                    idx = tier_order.index(active_cols)
                    if idx + 1 < len(tier_order):
                        next_cols = tier_order[idx + 1]
                        logger.info(
                            "Sandbox: falling back to lower observatory column tier "
                            "(missing migration?): %s", exc,
                        )
                        active_cols = next_cols
                        continue
                logger.warning("Sandbox: observatory page fetch failed at offset %d: %s", offset, exc)
                break
            rows = res.data or []
            all_rows.extend(rows)
            if len(rows) < PAGE:
                break
            offset += PAGE
            if offset > 500_000:
                break
        return pd.DataFrame(all_rows)


    # ── Calibration ──────────────────────────────────────────────────────

    def _apply_current_calibration(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply today's isotonic calibration to each row's raw_true_prob.

        Falls back to the stored true_prob when raw_true_prob is missing
        (legacy rows pre-migration_006), so historical data still
        contributes — just without the loop-bug correction.
        """
        if df.empty:
            df["calibrated_prob"] = []
            return df

        # Prefer raw_true_prob; fall back to true_prob.
        if "raw_true_prob" in df.columns:
            base = df["raw_true_prob"].combine_first(df["true_prob"])
        else:
            base = df["true_prob"]

        leagues = df["league"].fillna("")
        props   = df["prop"].fillna("")
        sides   = df.get("side", pd.Series([""] * len(df))).fillna("")
        curves  = self._curves

        def _cal(row):
            try:
                v = float(row["base"])
            except (TypeError, ValueError):
                return None
            if not (0.0 < v < 1.0):
                return None
            return calibrate(curves, row["league"], row["prop"], row["side"], v)

        tmp = pd.DataFrame({
            "base":    pd.to_numeric(base, errors="coerce"),
            "league":  leagues,
            "prop":    props,
            "side":    sides,
        })
        df = df.copy()
        df["calibrated_prob"] = tmp.apply(_cal, axis=1)
        return df


    # ── Slip construction strategies ────────────────────────────────────

    @staticmethod
    def _ev_pct(p: float) -> float:
        """Individual EV% at the optimal Power-6 implied decimal. Same
        formula BetResult uses — kept here so we don't import the whole
        ev_calculator just for one constant multiply."""
        return p * OPTIMAL_IMPLIED_DECIMAL - 1.0

    @staticmethod
    def _team_key(row) -> str:
        """PrizePicks distinct-team identifier. Prefer the team
        abbreviation captured in the observatory (added by
        migration_007); fall back to the player name so slips on legacy
        data still get sane (if structurally weaker) distinctness
        checks. Two players from opposing teams in the same game count
        as two distinct teams under PP rules — this is naturally
        satisfied because each player has a distinct `team`."""
        if hasattr(row, "get"):
            t = (row.get("team") or "")
        else:
            t = ""
        t = t.strip().upper() if isinstance(t, str) else ""
        if t:
            return t
        # Fallback: use the player as the team identity.
        player = (row.get("player") if hasattr(row, "get") else "") or ""
        return _FALLBACK_TEAM_PREFIX + str(player).strip().lower()

    @classmethod
    def _enforce_two_teams(cls, picks: list, sorted_legs: pd.DataFrame,
                           used_indices: set,
                           dedup_pair: set | None = None,
                           dedup_leg: set | None = None) -> list:
        """If the in-progress `picks` list represents a single-team
        slip, swap the lowest-ranked pick out for the highest-ranked
        unused leg from a different team. Returns the (possibly
        modified) picks list, or [] if no PP-legal slip can be formed.

        `dedup_pair`/`dedup_leg` are optional sets; when supplied, the
        replacement candidate must not already appear in either (used
        by live_replay to maintain its cross-slip dedup invariants
        during the swap)."""
        if not picks:
            return picks
        team_set = {cls._team_key(sorted_legs.loc[i]) for i in picks}
        if len(team_set) >= TWO_TEAMS_REQUIRED:
            return picks
        single_team = next(iter(team_set))

        # Search for the highest-ranked replacement from a different
        # team. sorted_legs is already sorted by the strategy's score
        # function, so the first qualifying candidate is the optimal one.
        replacement_idx = None
        for idx in sorted_legs.index:
            if idx in used_indices or idx in picks:
                continue
            row = sorted_legs.loc[idx]
            if cls._team_key(row) == single_team:
                continue
            if dedup_pair is not None or dedup_leg is not None:
                player = row.get("player", "") or ""
                start  = row.get("game_start", "") or ""
                p_key = make_bet_key(player, start)
                l_key = make_leg_key(
                    player,
                    row.get("prop", "") or "",
                    row.get("line", "") if "line" in row.index else "",
                    row.get("side", "") or "",
                    start,
                )
                if dedup_pair is not None and p_key in dedup_pair:
                    continue
                if dedup_leg is not None and l_key in dedup_leg:
                    continue
            replacement_idx = idx
            break

        if replacement_idx is None:
            return []

        # Drop the lowest-ranked existing pick (last index after the
        # ranked-descending pass) and append the replacement.
        picks = picks[:-1] + [replacement_idx]
        return picks

    def _select_ranked(self, slate: pd.DataFrame, slip_size: int,
                       *, score_col: str, dedup: bool) -> List[pd.DataFrame]:
        """Greedy slip builder used by all sandbox strategies.

        `score_col` selects the ranking signal ('calibrated_prob' or
        '_ev'). `dedup` toggles cross-slip player-game / leg dedup
        (live_replay turns it on; top_prob/top_ev leave it off for
        backward compat with v2). Two-team enforcement is always on —
        PrizePicks rejects single-team slips, so the sandbox can't
        report ROI on slips that wouldn't actually be bettable.

        Slips that can't satisfy the two-team rule are dropped, not
        downgraded to single-team — this matches the live system's
        behavior and keeps the simulated slip set apples-to-apples
        with what would actually have been logged."""
        sorted_legs = slate.sort_values(score_col, ascending=False)

        slips: List[pd.DataFrame] = []
        used_indices: set = set()
        used_pair: set = set()
        used_leg:  set = set()

        while True:
            picks: list = []
            seen_pair: set = set()
            seen_leg:  set = set()
            for idx in sorted_legs.index:
                if idx in used_indices:
                    continue
                row = sorted_legs.loc[idx]
                if dedup:
                    player = row.get("player", "") or ""
                    start  = row.get("game_start", "") or ""
                    p_key  = make_bet_key(player, start)
                    l_key  = make_leg_key(
                        player,
                        row.get("prop", "") or "",
                        row.get("line", "") if "line" in row.index else "",
                        row.get("side", "") or "",
                        start,
                    )
                    if p_key in used_pair: continue
                    if l_key in used_leg:  continue
                    if p_key in seen_pair: continue
                    if l_key in seen_leg:  continue
                    seen_pair.add(p_key); seen_leg.add(l_key)
                picks.append(idx)
                if len(picks) == slip_size:
                    break

            if len(picks) < slip_size:
                break

            picks = self._enforce_two_teams(
                picks, sorted_legs, used_indices,
                dedup_pair=used_pair if dedup else None,
                dedup_leg=used_leg if dedup else None,
            )
            if not picks:
                # Couldn't satisfy two-team rule with the remaining pool.
                # No more PP-legal slips can be formed in this slate.
                break

            slips.append(sorted_legs.loc[picks])
            used_indices.update(picks)
            if dedup:
                # Recompute dedup keys against the actual final picks
                # since _enforce_two_teams may have swapped a leg out.
                for idx in picks:
                    row = sorted_legs.loc[idx]
                    player = row.get("player", "") or ""
                    start  = row.get("game_start", "") or ""
                    used_pair.add(make_bet_key(player, start))
                    used_leg.add(make_leg_key(
                        player,
                        row.get("prop", "") or "",
                        row.get("line", "") if "line" in row.index else "",
                        row.get("side", "") or "",
                        start,
                    ))
        return slips

    def _build_slips(
        self, slate: pd.DataFrame, slip_size: int, strategy: str,
    ) -> List[pd.DataFrame]:
        """Dispatch to the chosen slip-construction strategy. Per-game
        cap is intentionally absent — superseded by the PrizePicks
        two-team requirement enforced inside `_select_ranked`."""
        slate = slate.copy()
        # Pre-compute EV column once; cheaper than per-strategy.
        slate["_ev"] = slate["calibrated_prob"].apply(self._ev_pct)

        if strategy == "top_prob":
            return self._select_ranked(slate, slip_size, score_col="calibrated_prob", dedup=False)
        if strategy == "top_ev":
            return self._select_ranked(slate, slip_size, score_col="_ev", dedup=False)
        if strategy == "live_replay":
            return self._select_ranked(slate, slip_size, score_col="_ev", dedup=True)
        logger.warning("Sandbox: unknown slip_strategy %r — using live_replay", strategy)
        return self._select_ranked(slate, slip_size, score_col="_ev", dedup=True)


    # ── Push-aware payout ───────────────────────────────────────────────

    @staticmethod
    def _payout_with_push(
        results: Iterable[str], slip_type: str,
    ) -> Tuple[float, int, int, int]:
        """Compute the slip's payout multiplier under PrizePicks push/dnp
        semantics. Mirrors the live API at web/app.py:2446-2458.

        Returns (payout_mult, n_eff, hits_eff, n_pushed).

        n_eff   = original n_legs - n_pushes - n_dnps (push and DNP both
                  refund the leg, reducing slip size).
        hits_eff = number of legs that actually hit (push/dnp don't count).
        payout = decimal multiplier on the original stake. payout=1.0
                 means stake refunded; payout > 1.0 is a win.
        """
        results = list(results)
        n_legs = len(results)
        effective = [r for r in results if r not in ("push", "dnp")]
        n_eff = len(effective)
        hits_eff = sum(1 for r in effective if r in ("hit", "won", "win"))
        n_pushed = n_legs - n_eff

        if n_eff == 0:
            # All legs pushed — full refund.
            return 1.0, 0, 0, n_pushed
        if n_eff == 1:
            return (1.0 if hits_eff == 1 else 0.0), n_eff, hits_eff, n_pushed
        if slip_type == "power":
            payout = POWER_PAYOUTS.get(n_eff, 0.0) if hits_eff == n_eff else 0.0
        else:  # flex
            if n_eff == 2:
                # PP treats 2-leg flex as Power 2.
                payout = POWER_PAYOUTS.get(2, 0.0) if hits_eff == 2 else 0.0
            else:
                payout = FLEX_PAYOUTS.get(n_eff, {}).get(hits_eff, 0.0)
        return float(payout), n_eff, hits_eff, n_pushed


    # ── Kelly (independent-bernoulli; same as v1) ───────────────────────

    def _calculate_kelly_fraction(self, probs: List[float], slip_size: int, slip_type: str) -> float:
        import itertools
        outcomes = list(itertools.product([0, 1], repeat=slip_size))
        ev = 0.0
        ev_sq = 0.0
        for outcome in outcomes:
            prob = 1.0
            for i in range(slip_size):
                prob *= probs[i] if outcome[i] == 1 else (1.0 - probs[i])
            hits = sum(outcome)
            mult = 0.0
            if slip_type == "power":
                if hits == slip_size:
                    mult = POWER_PAYOUTS.get(slip_size, 0.0)
            else:
                mult = FLEX_PAYOUTS.get(slip_size, {}).get(hits, 0.0)
            net_profit = mult - 1.0
            ev    += prob * net_profit
            ev_sq += prob * (net_profit ** 2)
        if ev <= 0:
            return 0.0
        variance = ev_sq - (ev ** 2)
        if variance <= 0:
            return 0.0
        return max(0.0, min((ev / variance) * 0.25, 1.0))   # quarter-Kelly


    # ── Bootstrap ───────────────────────────────────────────────────────

    @staticmethod
    def _bootstrap_metrics(
        slips: list[dict], n_resamples: int = BOOTSTRAP_RESAMPLES,
        bankroll: float = 100.0,
    ) -> dict:
        """Resample `slips` with replacement N times, computing total ROI,
        win rate, and max drawdown on each resample. Returns the 2.5 /
        97.5 percentiles for each metric so the UI can render
        `<point> [lo, hi]`.

        Drawdown is bankroll-based: peak starts at `bankroll`, drawdown
        is (peak - running_bank) / peak × 100. Same definition as the
        non-bootstrapped summary metric so the CI on max_drawdown_pct
        is in the same units the user sees on the card."""
        n = len(slips)
        if n == 0:
            return {}
        rng = random.Random(_BOOTSTRAP_SEED)

        rois:    list[float] = []
        wrs:     list[float] = []
        ddowns:  list[float] = []

        # Pre-extract for speed: bootstrap loops are hot.
        bet_arr    = [s["bet_size"] for s in slips]
        profit_arr = [s["profit"]   for s in slips]
        win_flag   = [1 if (s.get("payout", 0.0) > s.get("bet_size", 0.0)) else 0 for s in slips]

        for _ in range(n_resamples):
            idxs = [rng.randrange(n) for _ in range(n)]
            tot_bet = sum(bet_arr[i] for i in idxs)
            tot_prof = sum(profit_arr[i] for i in idxs)
            tot_wins = sum(win_flag[i] for i in idxs)
            roi = (tot_prof / tot_bet * 100.0) if tot_bet > 0 else 0.0
            wr  = (tot_wins / n * 100.0) if n > 0 else 0.0

            # Bankroll-based drawdown: same definition as the summary
            # metric. Resampled order approximates random ordering,
            # sufficient for tail-loss confidence.
            running = bankroll
            peak    = bankroll
            max_dd_pct = 0.0
            for i in idxs:
                running += profit_arr[i]
                if running > peak:
                    peak = running
                dd = ((peak - running) / peak * 100.0) if peak > 0 else 0.0
                if dd > max_dd_pct:
                    max_dd_pct = dd

            rois.append(roi)
            wrs.append(wr)
            ddowns.append(max_dd_pct)

        def _pct(arr, q):
            if not arr:
                return None
            arr_s = sorted(arr)
            k = int(round(q * (len(arr_s) - 1)))
            return arr_s[k]

        return {
            "roi_pct":          {"lo": round(_pct(rois, 0.025), 2),
                                 "hi": round(_pct(rois, 0.975), 2)},
            "win_rate_pct":     {"lo": round(_pct(wrs, 0.025), 2),
                                 "hi": round(_pct(wrs, 0.975), 2)},
            "max_drawdown_pct": {"lo": round(_pct(ddowns, 0.025), 2),
                                 "hi": round(_pct(ddowns, 0.975), 2)},
            "n_resamples":      n_resamples,
        }


    # ── Main entry ──────────────────────────────────────────────────────

    def run_simulation(self, config: StrategyConfig) -> Dict:
        if not self.db:
            return {"error": "Database not connected"}

        try:
            # Track the funnel — every filter step records its post-count
            # so the user can see exactly how many observations survived.
            funnel: dict[str, int] = {}

            # 1. Pull raw observatory rows.
            df = self._fetch_resolved_observatory(config.leagues)
            funnel["rows_pulled"] = len(df)
            if df.empty:
                return {"error": "No resolved data found matching league filter."}

            # 2. Date range.
            df["game_start_dt"] = pd.to_datetime(df["game_start"], errors="coerce", utc=True)
            df = df[df["game_start_dt"].notna()]
            funnel["after_game_start_parse"] = len(df)
            if config.start_date:
                start_dt = pd.to_datetime(config.start_date, utc=True)
                df = df[df["game_start_dt"] >= start_dt]
            if config.end_date:
                # End date is inclusive — bump to end-of-day.
                end_dt = pd.to_datetime(config.end_date, utc=True) + pd.Timedelta(days=1)
                df = df[df["game_start_dt"] < end_dt]
            funnel["after_date_filter"] = len(df)
            if df.empty:
                return {"error": "No rows in the selected date range.", "funnel": funnel}

            # 3. Prop includes/excludes.
            if config.excluded_props:
                df = df[~df["prop"].isin(config.excluded_props)]
            if config.included_props:
                df = df[df["prop"].isin(config.included_props)]
            funnel["after_prop_filter"] = len(df)
            if df.empty:
                return {"error": "Filters left no observations to evaluate.", "funnel": funnel}

            # 4. Apply current calibration to raw_true_prob.
            if config.use_calibration:
                df = self._apply_current_calibration(df)
            else:
                df = df.copy()
                df["calibrated_prob"] = pd.to_numeric(df["true_prob"], errors="coerce")
            df = df[df["calibrated_prob"].notna()]
            funnel["after_calibration"] = len(df)

            # 5. Min-prob filter (on calibrated).
            df = df[df["calibrated_prob"] >= config.min_prob]
            funnel["after_min_prob"] = len(df)
            if df.empty:
                return {"error": "No legs above the calibrated probability threshold.", "funnel": funnel}

            # 6. Group into slates by calendar day (game_start UTC).
            df["slate_id"] = df["game_start_dt"].dt.date.astype(str)
            slates = df.groupby("slate_id")
            funnel["distinct_slates"] = int(df["slate_id"].nunique())

            sim_slips: list[dict] = []
            cumulative_profit = 0.0
            total_bet = 0.0
            bankroll = config.bankroll
            push_legs_used = 0
            dnp_legs_used  = 0

            sorted_slate_ids = df.sort_values("game_start_dt")["slate_id"].unique()
            slates_with_qualifying = 0
            slates_without_full_slip = 0

            for sid in sorted_slate_ids:
                slate_df = slates.get_group(sid)
                if len(slate_df) < config.slip_size:
                    slates_without_full_slip += 1
                    continue
                slates_with_qualifying += 1

                slips_for_slate = self._build_slips(
                    slate_df, config.slip_size, config.slip_strategy,
                )
                if not slips_for_slate:
                    slates_without_full_slip += 1
                    continue

                for selected_legs in slips_for_slate:
                    if config.use_kelly:
                        probs = selected_legs["calibrated_prob"].tolist()
                        k_frac = self._calculate_kelly_fraction(
                            probs, config.slip_size, config.slip_type,
                        )
                        bet_size = bankroll * k_frac
                    else:
                        bet_size = config.bet_size

                    if bet_size <= 0:
                        # Kelly sized this slip out — record it with bet=0 so
                        # the win-rate denominator (which excludes bet=0) sees it.
                        sim_slips.append({
                            "timestamp": selected_legs["game_start"].iloc[0],
                            "league":    selected_legs["league"].iloc[0],
                            "hits":      0,
                            "n_eff":     0,
                            "n_legs":    config.slip_size,
                            "n_pushed":  0,
                            "payout":    0.0,
                            "bet_size":  0.0,
                            "profit":    0.0,
                            "legs":      selected_legs[
                                ["player", "prop", "calibrated_prob", "result"]
                            ].rename(columns={"calibrated_prob": "true_prob"}).to_dict("records"),
                        })
                        continue

                    results = selected_legs["result"].tolist()
                    payout_mult, n_eff, hits_eff, n_pushed = self._payout_with_push(
                        results, config.slip_type,
                    )
                    n_dnp = sum(1 for r in results if r == "dnp")
                    push_legs_used += sum(1 for r in results if r == "push")
                    dnp_legs_used  += n_dnp

                    profit = (bet_size * payout_mult) - bet_size
                    cumulative_profit += profit
                    total_bet += bet_size
                    bankroll += profit

                    sim_slips.append({
                        "timestamp":  selected_legs["game_start"].iloc[0],
                        "league":     selected_legs["league"].iloc[0],
                        "hits":       hits_eff,
                        "n_eff":      n_eff,
                        "n_legs":     config.slip_size,
                        "n_pushed":   n_pushed,
                        "payout":     bet_size * payout_mult,
                        "bet_size":   bet_size,
                        "profit":     profit,
                        "legs":       selected_legs[
                            ["player", "prop", "calibrated_prob", "result"]
                        ].rename(columns={"calibrated_prob": "true_prob"}).to_dict("records"),
                    })

            funnel["slates_with_qualifying"] = slates_with_qualifying
            funnel["slates_without_full_slip"] = slates_without_full_slip
            funnel["slips_formed"] = len(sim_slips)
            funnel["push_legs_in_slips"] = push_legs_used
            funnel["dnp_legs_in_slips"]  = dnp_legs_used

            if not sim_slips:
                return {
                    "error": f"Could not form any {config.slip_size}-leg slips from history.",
                    "funnel": funnel,
                }

            # Sort chronologically before any time-series rebuilding.
            def _ts(s):
                try:
                    return pd.to_datetime(s["timestamp"])
                except Exception:
                    return pd.Timestamp.max
            sim_slips.sort(key=_ts)

            # Equity / drawdown curves rebuilt in chronological order.
            equity_curve: list[dict] = []
            drawdown_curve: list[dict] = []
            running_cum = 0.0
            peak = config.bankroll
            running_bank = config.bankroll
            max_drawdown_pct = 0.0
            for s in sim_slips:
                running_cum += s["profit"]
                equity_curve.append({"x": s["timestamp"], "y": round(running_cum, 2)})
                running_bank += s["profit"]
                if running_bank > peak:
                    peak = running_bank
                dd_pct = ((peak - running_bank) / peak * 100.0) if peak > 0 else 0.0
                if dd_pct > max_drawdown_pct:
                    max_drawdown_pct = dd_pct
                drawdown_curve.append({"x": s["timestamp"], "y": round(dd_pct, 3)})

            # Aggregate metrics.
            roi = (cumulative_profit / total_bet) if total_bet > 0 else 0.0
            bet_slips = [s for s in sim_slips if s["bet_size"] > 0]
            wins_total = sum(1 for s in bet_slips if s["payout"] > s["bet_size"])
            win_rate = (wins_total / len(bet_slips)) if bet_slips else 0.0

            # Rolling ROI / win-rate (window sized to ~10% of slip count, clamped 20-100).
            window = max(20, min(100, len(sim_slips) // 10 or 20))
            rolling = []
            for i in range(len(sim_slips)):
                lo = max(0, i - window + 1)
                wnd = sim_slips[lo: i + 1]
                bet_sum    = sum(s["bet_size"] for s in wnd)
                profit_sum = sum(s["profit"] for s in wnd)
                wnd_bet    = [s for s in wnd if s["bet_size"] > 0]
                wins       = sum(1 for s in wnd_bet if s["payout"] > s["bet_size"])
                rolling.append({
                    "x":        wnd[-1]["timestamp"],
                    "roi":      round((profit_sum / bet_sum * 100.0) if bet_sum > 0 else 0.0, 2),
                    "win_rate": round((wins / len(wnd_bet) * 100.0) if wnd_bet else 0.0, 2),
                })

            # Monthly time-bucket: ROI per calendar month so users can see drift.
            monthly = self._monthly_buckets(sim_slips)

            # Bootstrap CIs on the aggregate metrics.
            ci = (
                self._bootstrap_metrics(bet_slips, bankroll=config.bankroll)
                if config.bootstrap and bet_slips else {}
            )

            # Breakdowns with bootstrap CI per row + thin-bucket flag.
            breakdowns = self._build_breakdowns(sim_slips, config.bootstrap)

            return {
                "summary": {
                    "total_slips":      len(sim_slips),
                    "bet_slips":        len(bet_slips),
                    "total_bet":        round(total_bet, 2),
                    "total_profit":     round(cumulative_profit, 2),
                    "roi_pct":          round(roi * 100, 2),
                    "win_rate_pct":     round(win_rate * 100, 2),
                    "max_drawdown_pct": round(max_drawdown_pct, 2),
                    "bankroll":         round(config.bankroll, 2),
                    "rolling_window":   window,
                    "ci":               ci,
                },
                "equity_curve":   equity_curve,
                "drawdown_curve": drawdown_curve,
                "rolling":        rolling,
                "monthly":        monthly,
                "breakdowns":     breakdowns,
                "funnel":         funnel,
                "slips":          sim_slips,
            }
        except Exception as e:
            logger.exception("Simulation failed")
            return {"error": str(e)}


    # ── Monthly bucketing & breakdowns ──────────────────────────────────

    @staticmethod
    def _monthly_buckets(slips: list[dict]) -> list[dict]:
        """ROI / win-rate per calendar month, oldest first. Useful for
        spotting performance decay over time."""
        if not slips:
            return []
        buckets: dict[str, dict] = {}
        for s in slips:
            try:
                dt = pd.to_datetime(s["timestamp"], utc=True)
            except Exception:
                continue
            key = dt.strftime("%Y-%m")
            b = buckets.setdefault(key, {
                "month": key, "slips": 0, "bet_slips": 0,
                "bet": 0.0, "profit": 0.0, "wins": 0,
            })
            b["slips"]  += 1
            b["bet"]    += s["bet_size"]
            b["profit"] += s["profit"]
            if s["bet_size"] > 0:
                b["bet_slips"] += 1
                if s["payout"] > s["bet_size"]:
                    b["wins"] += 1
        out = []
        for k in sorted(buckets):
            b = buckets[k]
            roi = (b["profit"] / b["bet"] * 100.0) if b["bet"] > 0 else 0.0
            wr  = (b["wins"] / b["bet_slips"] * 100.0) if b["bet_slips"] > 0 else 0.0
            out.append({
                "month":         k,
                "slips":         b["slips"],
                "bet":           round(b["bet"], 2),
                "profit":        round(b["profit"], 2),
                "roi_pct":       round(roi, 2),
                "win_rate_pct":  round(wr, 2),
            })
        return out

    @classmethod
    def _bucket_ci(cls, slips_subset: list[dict]) -> dict | None:
        """Bootstrap CI for one breakdown bucket. Returns None when the
        bucket is too thin to bootstrap meaningfully."""
        if len(slips_subset) < 2:
            return None
        return cls._bootstrap_metrics(slips_subset, n_resamples=BOOTSTRAP_RESAMPLES // 2)

    @classmethod
    def _build_breakdowns(cls, sim_slips: list[dict], with_ci: bool) -> dict:
        """Build by_league / by_hits / by_stat breakdowns with optional
        bootstrap CIs and an `is_thin` flag for low-n buckets."""
        def _empty():
            return {"slips": 0, "bet_slips": 0, "bet": 0.0, "profit": 0.0, "wins": 0,
                    "_slips": []}

        by_league: dict[str, dict] = {}
        by_hits:   dict[int, dict] = {}
        by_stat:   dict[str, dict] = {}

        for s in sim_slips:
            was_bet = s["bet_size"] > 0
            is_win  = was_bet and s["payout"] > s["bet_size"]

            lg = s["league"] or "Unknown"
            b = by_league.setdefault(lg, _empty())
            b["slips"] += 1; b["bet"] += s["bet_size"]; b["profit"] += s["profit"]
            if was_bet:
                b["bet_slips"] += 1; b["_slips"].append(s)
                if is_win: b["wins"] += 1

            h = int(s["hits"])
            b = by_hits.setdefault(h, _empty())
            b["slips"] += 1; b["bet"] += s["bet_size"]; b["profit"] += s["profit"]
            if was_bet:
                b["bet_slips"] += 1; b["_slips"].append(s)
                if is_win: b["wins"] += 1

            per_leg_bet = s["bet_size"] / s["n_legs"] if s["n_legs"] else 0.0
            per_leg_profit = s["profit"] / s["n_legs"] if s["n_legs"] else 0.0
            for leg in s.get("legs", []):
                prop = (leg.get("prop") or "Unknown").strip() or "Unknown"
                b = by_stat.setdefault(prop, {
                    "legs": 0, "bet": 0.0, "profit": 0.0, "hits": 0,
                })
                b["legs"]   += 1
                b["bet"]    += per_leg_bet
                b["profit"] += per_leg_profit
                if leg.get("result") == "hit":
                    b["hits"] += 1

        def _finalize_slip_group(d, key_label):
            out = []
            for k, v in d.items():
                bet = v.get("bet", 0.0)
                wr_num = v.get("wins", 0)
                wr_den = v.get("bet_slips", 0)
                slips_count = v.get("slips", 0)
                row = {
                    key_label: k,
                    "slips":   slips_count,
                    "bet":     round(bet, 2),
                    "profit":  round(v["profit"], 2),
                    "roi_pct": round((v["profit"] / bet * 100.0) if bet > 0 else 0.0, 2),
                    "win_rate_pct": round(
                        (wr_num / wr_den * 100.0) if wr_den else 0.0, 2,
                    ),
                    "is_thin": slips_count < MIN_BUCKET_SLIPS,
                }
                if with_ci and slips_count >= 2:
                    ci = cls._bucket_ci(v["_slips"])
                    if ci:
                        row["ci"] = ci
                out.append(row)
            return sorted(out, key=lambda r: r["profit"], reverse=True)

        def _finalize_stat_group(d):
            out = []
            for prop, v in d.items():
                bet  = v.get("bet", 0.0)
                legs = v.get("legs", 0)
                hits = v.get("hits", 0)
                out.append({
                    "stat_type": prop,
                    "slips":     legs,                # per-leg count, repurpose key
                    "bet":       round(bet, 2),
                    "profit":    round(v["profit"], 2),
                    "roi_pct":   round((v["profit"] / bet * 100.0) if bet > 0 else 0.0, 2),
                    "win_rate_pct": round(
                        (hits / legs * 100.0) if legs else 0.0, 2,
                    ),
                    "is_thin": legs < MIN_BUCKET_SLIPS,
                })
            return sorted(out, key=lambda r: r["profit"], reverse=True)

        return {
            "by_league": _finalize_slip_group(by_league, "league"),
            "by_hits":   _finalize_slip_group(by_hits,   "hits"),
            "by_stat":   _finalize_stat_group(by_stat),
        }


    # ── Optimizer ───────────────────────────────────────────────────────
    # Preserved from v1 but rewired to use the calibration-on-the-fly
    # data path so its output is consistent with run_simulation. Note:
    # this still has in-sample bias; the README and UI label it
    # accordingly. Use the held-out validation flow (future) for
    # generalizable threshold selection.

    _OPT_THRESHOLD_LO   = 0.53
    _OPT_THRESHOLD_HI   = 0.58
    _OPT_THRESHOLD_STEP = 0.001

    def _simulate_at_threshold(
        self, base_df: pd.DataFrame, threshold: float,
        slip_size: int, slip_type: str, bankroll: float, bet_size: float,
        use_kelly: bool, strategy: str,
    ) -> Optional[Dict]:
        df = base_df[base_df["calibrated_prob"] >= threshold]
        if df.empty or len(df) < slip_size:
            return None
        df = df.copy()
        df["slate_id"] = df["game_start_dt"].dt.date.astype(str)
        slates = df.groupby("slate_id")

        total_bet = 0.0
        total_profit = 0.0
        n_slips = 0
        n_zero_bets = 0
        running_bankroll = bankroll
        for sid in df.sort_values("game_start_dt")["slate_id"].unique():
            slate_df = slates.get_group(sid)
            if len(slate_df) < slip_size:
                continue
            for selected_legs in self._build_slips(slate_df, slip_size, strategy):
                if use_kelly:
                    probs = selected_legs["calibrated_prob"].tolist()
                    k_frac = self._calculate_kelly_fraction(probs, slip_size, slip_type)
                    leg_bet_size = running_bankroll * k_frac
                else:
                    leg_bet_size = bet_size
                if leg_bet_size <= 0:
                    n_zero_bets += 1
                    continue
                results = selected_legs["result"].tolist()
                payout_mult, _n_eff, _hits_eff, _n_pushed = self._payout_with_push(results, slip_type)
                profit = (leg_bet_size * payout_mult) - leg_bet_size
                total_profit += profit
                total_bet    += leg_bet_size
                running_bankroll += profit
                n_slips += 1
        if total_bet <= 0:
            return None
        roi = (total_profit / total_bet) * 100.0
        return {
            "threshold":    round(float(threshold), 4),
            "roi":          roi,
            "slips":        n_slips,
            "zero_bets":    n_zero_bets,
            "total_bet":    round(total_bet, 2),
            "total_profit": round(total_profit, 2),
        }

    def optimize_threshold(self, config: StrategyConfig) -> Dict:
        if not self.db:
            return {"error": "Database not connected"}
        if config.slip_size not in (2, 3, 4, 5, 6):
            return {"error": f"slip_size must be one of 2..6 (got {config.slip_size})."}
        if config.slip_type not in ("power", "flex"):
            return {"error": f"slip_type must be 'power' or 'flex' (got {config.slip_type!r})."}
        if config.bankroll is None or config.bankroll <= 0:
            return {"error": "bankroll must be positive."}
        if not config.use_kelly and (config.bet_size is None or config.bet_size <= 0):
            return {"error": "bet_size must be positive when Kelly is disabled."}
        if config.slip_type == "flex" and config.slip_size < 3:
            return {"error": "Flex slips require at least 3 legs."}

        try:
            base_df = self._fetch_resolved_observatory(config.leagues)
            if base_df.empty:
                return {"error": "No resolved data found matching filters."}
            base_df["game_start_dt"] = pd.to_datetime(base_df["game_start"], errors="coerce", utc=True)
            base_df = base_df[base_df["game_start_dt"].notna()]
            if config.start_date:
                start_dt = pd.to_datetime(config.start_date, utc=True)
                base_df = base_df[base_df["game_start_dt"] >= start_dt]
            if config.end_date:
                end_dt = pd.to_datetime(config.end_date, utc=True) + pd.Timedelta(days=1)
                base_df = base_df[base_df["game_start_dt"] < end_dt]
            if config.excluded_props:
                base_df = base_df[~base_df["prop"].isin(config.excluded_props)]
            if config.included_props:
                base_df = base_df[base_df["prop"].isin(config.included_props)]
            if base_df.empty:
                return {"error": "Filters left no observations to evaluate."}

            base_df = self._apply_current_calibration(base_df) if config.use_calibration else base_df.assign(
                calibrated_prob=pd.to_numeric(base_df["true_prob"], errors="coerce")
            )
            base_df = base_df[base_df["calibrated_prob"].notna()]
            if base_df.empty:
                return {"error": "No usable rows after calibration step."}

            best_roi = -float("inf")
            best_threshold: Optional[float] = None
            results: list[Dict] = []
            n_skipped_volume = 0
            n_skipped_zero_kelly = 0

            n_steps = int(round(
                (self._OPT_THRESHOLD_HI - self._OPT_THRESHOLD_LO) / self._OPT_THRESHOLD_STEP
            )) + 1
            for t in np.linspace(self._OPT_THRESHOLD_LO, self._OPT_THRESHOLD_HI, n_steps):
                t_val = float(t)
                summary = self._simulate_at_threshold(
                    base_df=base_df, threshold=t_val,
                    slip_size=config.slip_size, slip_type=config.slip_type,
                    bankroll=config.bankroll, bet_size=config.bet_size,
                    use_kelly=config.use_kelly,
                    strategy=config.slip_strategy,
                )
                if summary is None:
                    df_at_t = base_df[base_df["calibrated_prob"] >= t_val]
                    if df_at_t.empty or len(df_at_t) < config.slip_size:
                        n_skipped_volume += 1
                    else:
                        n_skipped_zero_kelly += 1
                    continue
                results.append({"threshold": summary["threshold"], "roi": summary["roi"], "slips": summary["slips"]})
                if summary["roi"] > best_roi:
                    best_roi = summary["roi"]
                    best_threshold = summary["threshold"]

            if not results:
                parts: list[str] = []
                if n_skipped_zero_kelly > 0:
                    parts.append(f"Kelly sized $0 for {n_skipped_zero_kelly} threshold(s)")
                if n_skipped_volume > 0:
                    parts.append(f"insufficient legs to form a {config.slip_size}-leg slip "
                                 f"at {n_skipped_volume} threshold(s)")
                fix = (
                    "Try disabling Kelly or widening filters."
                    if config.use_kelly and n_skipped_zero_kelly > 0
                    else "Try lowering slip size or widening filters."
                )
                msg = ("; ".join(parts) + ". " + fix) if parts else (
                    f"Not enough resolved legs above any tested threshold. {fix}"
                )
                return {"error": msg}

            return {
                "best_threshold": round(float(best_threshold), 4),
                "best_roi":       round(best_roi, 2),
                "all_results":    results,
                "warning":        ("In-sample optimization — the reported best ROI is biased "
                                   "upward by multiple-comparisons. Use the held-out "
                                   "validation flow before acting on this number."),
            }
        except Exception as e:
            logger.exception("Optimization failed")
            return {"error": str(e)}

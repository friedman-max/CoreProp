import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass, field

from engine.database import get_db
from engine.constants import POWER_PAYOUTS, FLEX_PAYOUTS

logger = logging.getLogger(__name__)

@dataclass
class StrategyConfig:
    leagues: List[str] = field(default_factory=list)
    min_prob: float = 0.5408  # Default to optimal break-even
    slip_size: int = 6        # 2, 3, 4, 5, 6
    slip_type: str = "flex"   # "power", "flex"
    bankroll: float = 100.0
    bet_size: float = 1.0     # Fixed bet size per slip
    excluded_props: List[str] = field(default_factory=list)
    included_props: List[str] = field(default_factory=list)  # Empty = all props
    use_calibration: bool = True
    use_kelly: bool = False

class StrategyTester:
    def __init__(self):
        self.db = get_db()

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
            ev += prob * net_profit
            ev_sq += prob * (net_profit ** 2)
            
        if ev <= 0:
            return 0.0
            
        variance = ev_sq - (ev ** 2)
        if variance <= 0:
            return 0.0
            
        # Quarter-Kelly is standard practice to manage drawdown risk
        kelly = (ev / variance) * 0.25 
        return max(0.0, min(kelly, 1.0))


    def run_simulation(self, config: StrategyConfig) -> Dict:
        """
        Runs a historical simulation based on the provided strategy configuration.
        """
        if not self.db:
            return {"error": "Database not connected"}

        try:
            # 1. Fetch resolved data
            query = self.db.table("market_observatory").select("*").neq("result", "pending")
            if config.leagues:
                query = query.in_("league", config.leagues)
            
            res = query.execute()
            df = pd.DataFrame(res.data)

            if df.empty:
                return {"error": "No resolved data found matching filters."}

            # 2. Pre-process outcomes
            df = df[df['result'].isin(['hit', 'miss'])].copy()
            df['outcome_bit'] = df['result'].map({'hit': 1, 'miss': 0})
            
            # Apply exclusion filters
            if config.excluded_props:
                df = df[~df['prop'].isin(config.excluded_props)]

            # Apply inclusion filters (stat-type specialization)
            if config.included_props:
                df = df[df['prop'].isin(config.included_props)]

            # Apply probability filter
            # Note: In a real simulation, we might want to apply calibration multipliers here
            # but for now we'll use the recorded true_prob.
            df = df[df['true_prob'] >= config.min_prob]

            if df.empty:
                return {"error": "No legs found above the probability threshold."}

            # 3. Group into Slates (By Day)
            # A slate represents all legs available on a given calendar day.
            df['game_start_dt'] = pd.to_datetime(df['game_start'])
            df['slate_id'] = df['game_start_dt'].dt.date.astype(str)
            slates = df.groupby('slate_id')

            sim_slips = []
            cumulative_profit = 0.0
            total_bet = 0.0
            bankroll = config.bankroll
            peak_bankroll = bankroll
            max_drawdown = 0.0
            equity_curve = []
            
            # Sort slates by time to simulate chronological betting
            sorted_slate_ids = df.sort_values('game_start')['slate_id'].unique()

            for sid in sorted_slate_ids:
                slate_df = slates.get_group(sid)
                
                # Sort legs by true_prob to pick the best ones first
                sorted_legs = slate_df.sort_values('true_prob', ascending=False)
                
                # Build as many slips of 'slip_size' as possible from this day's pool
                for i in range(0, len(sorted_legs) - config.slip_size + 1, config.slip_size):
                    selected_legs = sorted_legs.iloc[i : i + config.slip_size]
                    
                    if config.use_kelly:
                        probs = selected_legs['true_prob'].tolist()
                        k_frac = self._calculate_kelly_fraction(probs, config.slip_size, config.slip_type)
                        bet_size = bankroll * k_frac
                    else:
                        bet_size = config.bet_size
                        
                    # Calculate result
                    hits = int(selected_legs['outcome_bit'].sum())
                    payout_mult = 0.0
                    
                    if config.slip_type == "power":
                        if hits == config.slip_size:
                            payout_mult = POWER_PAYOUTS.get(config.slip_size, 0.0)
                    else: # flex
                        payout_mult = FLEX_PAYOUTS.get(config.slip_size, {}).get(hits, 0.0)

                    profit = (bet_size * payout_mult) - bet_size
                    bankroll += profit

                    # Track drawdown
                    if bankroll > peak_bankroll:
                        peak_bankroll = bankroll
                    else:
                        dd = (peak_bankroll - bankroll) / peak_bankroll if peak_bankroll > 0 else 0
                        if dd > max_drawdown:
                            max_drawdown = dd

                    cumulative_profit += profit
                    total_bet += bet_size
                    
                    sim_slips.append({
                        "timestamp": selected_legs['game_start'].iloc[0],
                        "league": selected_legs['league'].iloc[0],
                        "hits": hits,
                        "n_legs": config.slip_size,
                        "payout": bet_size * payout_mult,
                        "bet_size": bet_size,
                        "profit": profit,
                        "legs": selected_legs[['player', 'prop', 'true_prob', 'result']].to_dict('records')
                    })
                    equity_curve.append({
                        "x": selected_legs['game_start'].iloc[0],
                        "y": round(cumulative_profit, 2)
                    })

            if not sim_slips:
                return {"error": f"Could not form any {config.slip_size}-leg slips from history."}

            # 4. Aggregate Results
            roi = (cumulative_profit / total_bet) if total_bet > 0 else 0
            win_rate = sum(1 for s in sim_slips if s['profit'] > 0) / len(sim_slips)

            return {
                "summary": {
                    "total_slips": len(sim_slips),
                    "total_bet": round(total_bet, 2),
                    "total_profit": round(cumulative_profit, 2),
                    "roi_pct": round(roi * 100, 2),
                    "win_rate_pct": round(win_rate * 100, 2),
                    "max_drawdown_pct": round(max_drawdown * 100, 2),
                },
                "equity_curve": equity_curve,
                "slips": sim_slips[-50:] # Return last 50 for the UI log
            }
        except Exception as e:
            logger.exception("Simulation failed")
            return {"error": str(e)}

    # Threshold sweep range. Anything outside this band is too aggressive
    # (low) or too restrictive (high) to produce stable estimates from the
    # current observatory volumes.
    _OPT_THRESHOLD_LO  = 0.53
    _OPT_THRESHOLD_HI  = 0.58
    _OPT_THRESHOLD_STEP = 0.001

    def _simulate_at_threshold(
        self,
        base_df: pd.DataFrame,
        threshold: float,
        slip_size: int,
        slip_type: str,
        bankroll: float,
        bet_size: float,
        use_kelly: bool,
    ) -> Optional[Dict]:
        """Run one slate-by-slate simulation at a fixed `threshold`.

        Returns None if the threshold produced no slips (insufficient data
        OR every slip got a Kelly-zero bet size). Otherwise returns the
        threshold's summary dict.
        """
        df = base_df[base_df['true_prob'] >= threshold]
        if df.empty or len(df) < slip_size:
            return None

        df = df.copy()
        df['game_start_dt'] = pd.to_datetime(df['game_start'], errors='coerce')
        # Drop rows without a parseable game_start so they don't pollute
        # slate grouping with a fake "NaT" slate.
        df = df[df['game_start_dt'].notna()]
        if df.empty or len(df) < slip_size:
            return None
        df['slate_id'] = df['game_start_dt'].dt.date.astype(str)
        slates = df.groupby('slate_id')

        total_bet = 0.0
        total_profit = 0.0
        n_slips = 0
        n_zero_bets = 0
        running_bankroll = bankroll

        for sid in df.sort_values('game_start')['slate_id'].unique():
            slate_df = slates.get_group(sid)
            sorted_legs = slate_df.sort_values('true_prob', ascending=False)

            stride = slip_size
            for i in range(0, len(sorted_legs) - slip_size + 1, stride):
                selected_legs = sorted_legs.iloc[i: i + slip_size]

                if use_kelly:
                    probs = selected_legs['true_prob'].tolist()
                    k_frac = self._calculate_kelly_fraction(probs, slip_size, slip_type)
                    leg_bet_size = running_bankroll * k_frac
                else:
                    leg_bet_size = bet_size

                if leg_bet_size <= 0:
                    n_zero_bets += 1
                    continue

                hits = int(selected_legs['outcome_bit'].sum())
                payout_mult = 0.0
                if slip_type == "power":
                    if hits == slip_size:
                        payout_mult = POWER_PAYOUTS.get(slip_size, 0.0)
                else:
                    payout_mult = FLEX_PAYOUTS.get(slip_size, {}).get(hits, 0.0)

                profit = (leg_bet_size * payout_mult) - leg_bet_size
                total_profit += profit
                total_bet += leg_bet_size
                running_bankroll += profit
                n_slips += 1

        if total_bet <= 0:
            return None

        roi = (total_profit / total_bet) * 100.0
        return {
            "threshold":   round(float(threshold), 4),
            "roi":         roi,
            "slips":       n_slips,
            "zero_bets":   n_zero_bets,
            "total_bet":   round(total_bet, 2),
            "total_profit": round(total_profit, 2),
        }

    def optimize_threshold(self, config: StrategyConfig) -> Dict:
        """
        Sweeps through min_prob thresholds to find the one that maximizes ROI.
        """
        if not self.db:
            return {"error": "Database not connected"}

        # Validate inputs up-front so a bad request fails fast with a clear
        # message rather than a deep ValueError from range() or pandas.
        if config.slip_size not in (2, 3, 4, 5, 6):
            return {"error": f"slip_size must be one of 2..6 (got {config.slip_size})."}
        if config.slip_type not in ("power", "flex"):
            return {"error": f"slip_type must be 'power' or 'flex' (got {config.slip_type!r})."}
        if config.bankroll is None or config.bankroll <= 0:
            return {"error": "bankroll must be positive."}
        if not config.use_kelly and (config.bet_size is None or config.bet_size <= 0):
            return {"error": "bet_size must be positive when Kelly is disabled."}
        # Flex payouts only exist for slip_size ≥ 3; PrizePicks treats 2-leg
        # flex as a Power slip (see ev_calculator.calculate_slip).
        if config.slip_type == "flex" and config.slip_size < 3:
            return {"error": "Flex slips require at least 3 legs."}

        try:
            query = self.db.table("market_observatory").select("*").neq("result", "pending")
            if config.leagues:
                query = query.in_("league", config.leagues)
            res = query.execute()
            base_df = pd.DataFrame(res.data) if res and res.data is not None else pd.DataFrame()

            if base_df.empty:
                return {"error": "No resolved data found matching filters."}

            base_df = base_df[base_df['result'].isin(['hit', 'miss'])].copy()
            base_df['outcome_bit'] = base_df['result'].map({'hit': 1, 'miss': 0})
            if config.excluded_props:
                base_df = base_df[~base_df['prop'].isin(config.excluded_props)]
            if config.included_props:
                base_df = base_df[base_df['prop'].isin(config.included_props)]

            if base_df.empty:
                return {"error": "Filters left no observations to evaluate."}

            best_roi = -float('inf')
            best_threshold: Optional[float] = None
            results: list[Dict] = []
            n_skipped_volume = 0
            n_skipped_zero_kelly = 0

            # linspace is FP-stable (np.arange routinely drops the upper
            # endpoint by an ULP or two — caught by the test suite).
            n_steps = int(round(
                (self._OPT_THRESHOLD_HI - self._OPT_THRESHOLD_LO) / self._OPT_THRESHOLD_STEP
            )) + 1
            for t in np.linspace(self._OPT_THRESHOLD_LO, self._OPT_THRESHOLD_HI, n_steps):
                t_val = float(t)
                summary = self._simulate_at_threshold(
                    base_df=base_df,
                    threshold=t_val,
                    slip_size=config.slip_size,
                    slip_type=config.slip_type,
                    bankroll=config.bankroll,
                    bet_size=config.bet_size,
                    use_kelly=config.use_kelly,
                )
                if summary is None:
                    # Decide why so we can give a useful error if every threshold skips.
                    df_at_t = base_df[base_df['true_prob'] >= t_val]
                    if df_at_t.empty or len(df_at_t) < config.slip_size:
                        n_skipped_volume += 1
                    else:
                        # Volume was fine, so total_bet stayed 0 — Kelly
                        # zeroed every leg.
                        n_skipped_zero_kelly += 1
                    continue

                results.append({
                    "threshold": summary["threshold"],
                    "roi":       summary["roi"],
                    "slips":     summary["slips"],
                })
                if summary["roi"] > best_roi:
                    best_roi = summary["roi"]
                    best_threshold = summary["threshold"]

            if not results:
                # Construct a message that names the actual cause(s). Both
                # paths can fire across the sweep — Kelly-zero on low
                # thresholds, volume-skip on high ones — so we report
                # whichever is dominant and tailor the suggested fix.
                parts: list[str] = []
                if n_skipped_zero_kelly > 0:
                    parts.append(
                        f"Kelly sized $0 for {n_skipped_zero_kelly} threshold(s) "
                        "(every slip at those thresholds was -EV)"
                    )
                if n_skipped_volume > 0:
                    parts.append(
                        f"insufficient legs to form a {config.slip_size}-leg slip "
                        f"at {n_skipped_volume} threshold(s)"
                    )
                fix = (
                    "Try disabling Kelly or widening filters."
                    if config.use_kelly and n_skipped_zero_kelly > 0
                    else "Try lowering slip size or widening filters."
                )
                msg = ("; ".join(parts) + ". " + fix) if parts else (
                    f"Not enough resolved legs above any tested threshold to form "
                    f"a {config.slip_size}-leg slip. {fix}"
                )
                return {"error": msg}

            return {
                "best_threshold": round(float(best_threshold), 4),
                "best_roi":       round(best_roi, 2),
                "all_results":    results,
            }
        except Exception as e:
            logger.exception("Optimization failed")
            return {"error": str(e)}

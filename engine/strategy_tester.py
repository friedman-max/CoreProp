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
    use_calibration: bool = True

class StrategyTester:
    def __init__(self):
        self.db = get_db()

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

            # Apply probability filter
            # Note: In a real simulation, we might want to apply calibration multipliers here
            # but for now we'll use the recorded true_prob.
            df = df[df['true_prob'] >= config.min_prob]

            if df.empty:
                return {"error": "No legs found above the probability threshold."}

            # 3. Group into Slates (League + Game Start)
            # A slate represents a window of time where legs were available together.
            df['slate_id'] = df['league'] + "|" + df['game_start'].astype(str)
            slates = df.groupby('slate_id')

            sim_slips = []
            cumulative_profit = 0.0
            equity_curve = []
            
            # Sort slates by time to simulate chronological betting
            sorted_slate_ids = df.sort_values('game_start')['slate_id'].unique()

            for sid in sorted_slate_ids:
                slate_df = slates.get_group(sid)
                
                # We need at least 'slip_size' legs to form a slip from this slate
                if len(slate_df) < config.slip_size:
                    continue

                # PICK LOGIC: Pick the top N legs by true_prob
                # This simulates a user picking the "best" available plays at that time.
                selected_legs = slate_df.sort_values('true_prob', ascending=False).head(config.slip_size)
                
                # Calculate result
                hits = int(selected_legs['outcome_bit'].sum())
                payout_mult = 0.0
                
                if config.slip_type == "power":
                    if hits == config.slip_size:
                        payout_mult = POWER_PAYOUTS.get(config.slip_size, 0.0)
                else: # flex
                    payout_mult = FLEX_PAYOUTS.get(config.slip_size, {}).get(hits, 0.0)

                profit = (config.bet_size * payout_mult) - config.bet_size
                cumulative_profit += profit
                
                sim_slips.append({
                    "timestamp": selected_legs['game_start'].iloc[0],
                    "league": selected_legs['league'].iloc[0],
                    "hits": hits,
                    "n_legs": config.slip_size,
                    "payout": config.bet_size * payout_mult,
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
            total_bet = len(sim_slips) * config.bet_size
            roi = (cumulative_profit / total_bet) if total_bet > 0 else 0
            win_rate = sum(1 for s in sim_slips if s['profit'] > 0) / len(sim_slips)

            return {
                "summary": {
                    "total_slips": len(sim_slips),
                    "total_bet": round(total_bet, 2),
                    "total_profit": round(cumulative_profit, 2),
                    "roi_pct": round(roi * 100, 2),
                    "win_rate_pct": round(win_rate * 100, 2),
                },
                "equity_curve": equity_curve,
                "slips": sim_slips[-50:] # Return last 50 for the UI log
            }

        except Exception as e:
            logger.exception("Simulation failed")
            return {"error": str(e)}

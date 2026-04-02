import sys
import os
import logging

# Add root to path
sys.path.append(os.getcwd())

from engine.matcher import match_props, FanDuelProp, PrizePickLine, MatchedProp
from engine.ev_calculator import evaluate_match

def test_ev_calc_relaxed():
    print("Testing EV calculation with partial book data...")
    
    # LeBron match - both books
    match_both = MatchedProp(
        pp=PrizePickLine(league="NBA", player_name="LeBron James", stat_type="Points", line_score=25.5, player_id="1"),
        fd=FanDuelProp(league="NBA", player_name="LeBron James", prop_type="Points", line=25.5, over_odds=-110, under_odds=-110, both_sided=True),
        dk=FanDuelProp(league="NBA", player_name="LeBron James", prop_type="Points", line=25.5, over_odds=-115, under_odds=-115, both_sided=True),
        name_score=100
    )
    
    # Connor match - FD only
    match_fd = MatchedProp(
        pp=PrizePickLine(league="NHL", player_name="Connor McDavid", stat_type="Goals", line_score=0.5, player_id="2"),
        fd=FanDuelProp(league="NHL", player_name="Connor McDavid", prop_type="Goals", line=0.5, over_odds=150, under_odds=None, both_sided=False),
        dk=None,
        name_score=100
    )
    
    # Shohei match - DK only
    match_dk = MatchedProp(
        pp=PrizePickLine(league="MLB", player_name="Shohei Ohtani", stat_type="Home Runs", line_score=0.5, player_id="3"),
        fd=None,
        dk=FanDuelProp(league="MLB", player_name="Shohei Ohtani", prop_type="Home Runs", line=0.5, over_odds=200, under_odds=None, both_sided=False),
        name_score=100
    )
    
    for m in [match_both, match_fd, match_dk]:
        results = evaluate_match(m, min_ev_pct=-10.0)
        print(f"Match [{m.pp.player_name}] -> {len(results)} bets found.")
        for r in results:
            print(f"  - {r.side.upper()} {r.pp_line} (True Prob: {r.true_prob:.4f}, EV: {r.individual_ev_pct:.4f})")
            
    print("SUCCESS: EV calculated for all scenarios.")

if __name__ == "__main__":
    test_ev_calc_relaxed()

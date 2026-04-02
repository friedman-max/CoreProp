import sys
import os
import logging

# Add root to path
sys.path.append(os.getcwd())

from engine.matcher import match_props, FanDuelProp, PrizePickLine, MatchedProp

def test_static_matching():
    print("Testing static matching with partial data...")
    
    pp = [
        PrizePickLine(league="NBA", player_name="LeBron James", stat_type="Points", line_score=25.5, player_id="1"),
        PrizePickLine(league="NHL", player_name="Connor McDavid", stat_type="Goals", line_score=0.5, player_id="2"),
        PrizePickLine(league="MLB", player_name="Shohei Ohtani", stat_type="Home Runs", line_score=0.5, player_id="3")
    ]
    
    fd = [
        FanDuelProp(league="NBA", player_name="LeBron James", prop_type="Points", line=25.5, over_odds=-110, under_odds=-110, both_sided=True),
        FanDuelProp(league="NHL", player_name="Connor McDavid", prop_type="Goals", line=0.5, over_odds=150, under_odds=None, both_sided=False)
    ]
    
    dk = [
        FanDuelProp(league="NBA", player_name="LeBron James", prop_type="Points", line=25.5, over_odds=-115, under_odds=-115, both_sided=True),
        FanDuelProp(league="MLB", player_name="Shohei Ohtani", prop_type="Home Runs", line=0.5, over_odds=200, under_odds=None, both_sided=False)
    ]
    
    matches = match_props(fd, dk, pp)
    print(f"Total Matches: {len(matches)}")
    for m in matches:
        fd_str = "FD" if m.fd else "--"
        dk_str = "DK" if m.dk else "--"
        print(f"  - [{fd_str}][{dk_str}] {m.pp.player_name}: {m.pp.stat_type}")
    
    assert len(matches) == 3
    print("SUCCESS: Identified matches with partial book coverage.")

if __name__ == "__main__":
    test_static_matching()

"""Regression tests for ESPNResultsChecker._compute_stat league routing.

The generic basketball "Points" branch had no `league != "NHL"` guard (unlike
Assists and Blocked Shots), so it shadowed the NHL-specific Points handler
(goals + assists). NHL Points legs graded off a nonexistent basketball `pts`
key from the boxscore -> None -> force-marked DNP, dropping a real hit/miss.
"""
from __future__ import annotations

from engine.results_checker import ESPNResultsChecker


def test_nhl_points_is_goals_plus_assists():
    # NHL boxscore has no `pts`; Points must be goals + assists.
    assert ESPNResultsChecker._compute_stat({"goals": 1, "assists": 2}, "Points", "NHL") == 3.0


def test_nhl_points_none_when_components_missing():
    # If a component is missing, return None (pending), not a wrong number.
    assert ESPNResultsChecker._compute_stat({"goals": 1}, "Points", "NHL") is None


def test_nba_points_still_reads_pts():
    # The non-NHL path is unchanged: NBA Points reads the pts column.
    assert ESPNResultsChecker._compute_stat({"pts": 27}, "Points", "NBA") == 27.0

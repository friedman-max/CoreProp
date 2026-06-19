"""Soccer (World Cup) scoring path in results_checker.

Verifies the rosters-shaped box-score parse and the soccer prop mapping,
including that ESPN-unavailable props (Tackles, Shots Assisted) return None
so those legs stay pending rather than grade on a missing stat.
"""
from engine.results_checker import ESPNResultsChecker, grade_leg


# A trimmed ESPN soccer summary in the real shape: player stats live under
# rosters[].roster[].stats[] as {name, abbreviation, displayValue}.
SOCCER_SUMMARY = {
    "boxscore": {"teams": [], "form": []},   # no boxscore.players (soccer)
    "rosters": [
        {
            "homeAway": "home",
            "roster": [
                {
                    "athlete": {"displayName": "Viktor Gyökeres", "id": "1"},
                    "stats": [
                        {"name": "totalShots",    "abbreviation": "SHOT", "displayValue": "5"},
                        {"name": "shotsOnTarget",  "abbreviation": "SOG",  "displayValue": "2"},
                        {"name": "foulsCommitted", "abbreviation": "FC",   "displayValue": "1"},
                        {"name": "goalAssists",    "abbreviation": "A",    "displayValue": "0"},
                        {"name": "totalGoals",     "abbreviation": "G",    "displayValue": "1"},
                    ],
                },
                {
                    "athlete": {"displayName": "Alireza Beiranvand", "id": "2"},
                    "stats": [
                        {"name": "saves",          "abbreviation": "SV", "displayValue": "6"},
                        {"name": "totalShots",     "abbreviation": "SHOT", "displayValue": "0"},
                    ],
                },
            ],
        }
    ],
}


def _stats(summary, name):
    parsed = ESPNResultsChecker._parse_box_score(summary)
    return parsed[name.lower()]


def test_parse_indexes_by_long_name_and_abbreviation():
    parsed = ESPNResultsChecker._parse_box_score(SOCCER_SUMMARY)
    assert "viktor gyökeres" in parsed
    s = parsed["viktor gyökeres"]
    assert s["totalshots"] == "5" and s["shot"] == "5"
    assert s["shotsontarget"] == "2" and s["sog"] == "2"


def test_compute_soccer_props():
    chk = ESPNResultsChecker.__new__(ESPNResultsChecker)
    g = _stats(SOCCER_SUMMARY, "Viktor Gyökeres")
    assert chk._compute_stat(g, "Shots", "SOCCER") == 5
    assert chk._compute_stat(g, "Shots On Target", "SOCCER") == 2
    assert chk._compute_stat(g, "Fouls", "SOCCER") == 1
    assert chk._compute_stat(g, "Goals", "SOCCER") == 1
    assert chk._compute_stat(g, "Assists", "SOCCER") == 0
    gk = _stats(SOCCER_SUMMARY, "Alireza Beiranvand")
    assert chk._compute_stat(gk, "Goalie Saves", "SOCCER") == 6


def test_unavailable_soccer_props_return_none():
    """Tackles + Shots Assisted aren't in ESPN's WC feed -> None (stay pending),
    never graded against a stat we don't have."""
    chk = ESPNResultsChecker.__new__(ESPNResultsChecker)
    g = _stats(SOCCER_SUMMARY, "Viktor Gyökeres")
    assert chk._compute_stat(g, "Tackles", "SOCCER") is None
    assert chk._compute_stat(g, "Shots Assisted", "SOCCER") is None


def test_soccer_grading_end_to_end():
    chk = ESPNResultsChecker.__new__(ESPNResultsChecker)
    g = _stats(SOCCER_SUMMARY, "Viktor Gyökeres")
    shots = chk._compute_stat(g, "Shots", "SOCCER")
    assert grade_leg(shots, 2.0, "over") == "hit"     # 5 > 2
    assert grade_leg(shots, 5.0, "over") == "push"    # 5 == 5
    assert grade_leg(shots, 6.5, "over") == "miss"    # 5 < 6.5


def test_soccer_does_not_leak_into_other_leagues():
    """The soccer block is league-gated; an NBA 'Assists' must not hit the
    soccer goalAssists path."""
    chk = ESPNResultsChecker.__new__(ESPNResultsChecker)
    nba = {"ast": "7", "pts": "20"}
    assert chk._compute_stat(nba, "Assists", "NBA") == 7

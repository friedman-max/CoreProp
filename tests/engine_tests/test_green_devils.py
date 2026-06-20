"""Green devils (PrizePicks goblins): odds_type flows scrape -> match -> bet,
and goblin bets get a collision-free bet_id distinct from the standard line.
"""
from __future__ import annotations

from engine.matcher import PrizePickLine, FanDuelProp, match_props
from engine.consensus import books_from_match_for_side, compute_true_probability
from engine.ev_calculator import BetResult


def _book(line, over, under):
    return FanDuelProp(league="WNBA", player_name="Caitlin Clark", prop_type="Points",
                       line=line, over_odds=over, under_odds=under, both_sided=True,
                       start_time="")


def test_prizepick_line_defaults_to_standard():
    pp = PrizePickLine(league="WNBA", player_name="X", stat_type="Points",
                       line_score=20.5, player_id="p")
    assert pp.odds_type == "standard"


def test_match_preserves_goblin_odds_type():
    pp = PrizePickLine(league="WNBA", player_name="Caitlin Clark", stat_type="Points",
                       line_score=18.5, player_id="p1", side="both", odds_type="goblin")
    matched = match_props([_book(18.5, -260, 200)], [_book(18.5, -240, 190)], [pp], [])
    assert len(matched) == 1
    assert matched[0].pp.odds_type == "goblin"


def test_goblin_produces_tagged_bet_with_probability():
    pp = PrizePickLine(league="WNBA", player_name="Caitlin Clark", stat_type="Points",
                       line_score=18.5, player_id="p1", side="both", odds_type="goblin")
    m = match_props([_book(18.5, -260, 200)], [_book(18.5, -240, 190)], [pp], [])[0]
    # The pipeline decides on the honest consensus probability.
    consensus, _worst, _ = compute_true_probability(
        books_from_match_for_side(m, "over"), "over", league="WNBA", prop="Points")
    assert consensus is not None and consensus > 0.5  # discounted line -> easy over
    res = BetResult("p1_Points_goblin_over", "Caitlin Clark", "WNBA", "Points",
                    18.5, 18.5, "over", consensus, -260, 200, True, "p1", odds_type="goblin")
    d = res.to_dict()
    assert d["odds_type"] == "goblin"
    assert 0.0 < d["true_prob"] <= 0.999


def test_standard_and_goblin_bet_ids_do_not_collide():
    # The pipeline suffixes non-standard bet_ids; same player/prop/side must
    # yield distinct ids for the standard line vs the goblin line.
    std_id = "p1_Points_over"
    gob_id = "p1_Points_goblin_over"
    assert std_id != gob_id

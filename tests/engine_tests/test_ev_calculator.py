"""Pin the BetResult invariants.

BetResult is the value object that flows from the matching loop to the
client. The incoming `true_prob` is the raw no-vig market consensus:

  * raw_true_prob preserves it untouched (clamped to [0.001, 0.999]) — CLV
    and the observatory training corpus measure against the raw number.
  * true_prob (the DECISION number) additionally gets the FINDINGS.md
    side-bias correction (config.SIDE_BIAS) for standard lines; green
    devils are exempt.
  * EV math: individual_ev_pct == score_leg(true_prob, 6, "power")
  * true_prob is clipped to 0.999 (downstream Kelly math depends on
    log(1 - p) being finite).
"""
from __future__ import annotations

import pytest

import config as cfg
from engine.ev_calculator import BetResult
from engine.constants import score_leg, per_leg_break_even


def _bet(prob: float, side: str = "over", league: str = "NBA",
         odds_type: str = "standard") -> BetResult:
    return BetResult(
        bet_id="x",
        player_name="Test Player",
        league=league,
        prop_type="Points",
        pp_line=20.5,
        fd_line=20.5,
        side=side,
        true_prob=prob,
        over_odds=-110,
        under_odds=-110,
        both_sided=True,
        pp_player_id="123",
        odds_type=odds_type,
    )


def _expected_decision(prob: float, league: str, side: str) -> float:
    """Mirror of the BetResult bias application for assertions."""
    raw = max(0.001, min(0.999, prob))
    if cfg.SIDE_BIAS_ENABLED:
        raw = raw + cfg.SIDE_BIAS.get((league.upper(), side.lower()), 0.0)
    return max(0.001, min(0.999, raw))


def test_ev_formula_matches_true_prob():
    """individual_ev_pct is the context-aware per-leg EV vs the 6-Power
    break-even, derived from the (bias-corrected) decision prob."""
    b = _bet(0.60)
    expected = round(score_leg(b.true_prob, slip_n=6, slip_type="power"), 6)
    assert b.individual_ev_pct == pytest.approx(expected, abs=1e-6)


def test_true_prob_clipped_below_one():
    """Kelly math needs log(1 - p) finite. Clip ceiling is 0.999."""
    b = _bet(0.9999)
    assert b.true_prob <= 0.999


def test_raw_true_prob_preserved_and_decision_biased():
    """raw_true_prob is the untouched consensus; true_prob carries the
    FINDINGS side-bias (NBA over: -0.031)."""
    b = _bet(0.55, side="over", league="NBA")
    assert b.raw_true_prob == pytest.approx(0.55)
    assert b.true_prob == pytest.approx(_expected_decision(0.55, "NBA", "over"))
    assert b.true_prob <= 0.999


def test_side_bias_boosts_unders_docks_overs():
    """The correction moves UNDERs up and (most) OVERs down, per league."""
    if not cfg.SIDE_BIAS_ENABLED:
        pytest.skip("side bias disabled via env")
    under = _bet(0.55, side="under", league="NBA")
    over = _bet(0.55, side="over", league="NBA")
    assert under.true_prob > under.raw_true_prob
    assert over.true_prob < over.raw_true_prob


def test_side_bias_unknown_league_neutral():
    """Leagues without a fitted bias entry (e.g. SOCCER) pass through raw."""
    b = _bet(0.55, side="over", league="SOCCER")
    assert b.true_prob == pytest.approx(0.55)


def test_green_devils_exempt_from_bias():
    """The bias table was fit on standard lines; goblins pass through raw."""
    b = _bet(0.55, side="under", league="NBA", odds_type="goblin")
    assert b.true_prob == pytest.approx(0.55)


def test_to_dict_carries_true_and_raw():
    """The wire format serializes both probabilities."""
    b = _bet(0.55)
    d = b.to_dict()
    assert "true_prob" in d
    assert "raw_true_prob" in d
    assert d["raw_true_prob"] == pytest.approx(0.55)


def test_edge_relative_to_break_even():
    """edge = decision_prob - per_leg_break_even(6, 'power')."""
    b = _bet(0.50, side="over", league="NBA")
    expected_p = _expected_decision(0.50, "NBA", "over")
    assert b.edge == pytest.approx(expected_p - per_leg_break_even(6, "power"), abs=1e-6)
    assert -0.15 < b.edge < 0.1

"""Pin the BetResult invariants.

BetResult is the value object that flows from the matching loop to the
client. In simplify-v1 there is NO calibration: the incoming `true_prob` is
already the most-conservative devigged probability across books, and BetResult
only clamps it to [0.001, 0.999] for the downstream log/EV math.

  * EV math: individual_ev_pct == score_leg(true_prob, 6, "power")
  * true_prob is clipped to 0.999 (downstream Kelly math depends on
    log(1 - p) being finite).
  * raw_true_prob == true_prob (kept for the wire format).
"""
from __future__ import annotations

import pytest

from engine.ev_calculator import BetResult
from engine.constants import score_leg, per_leg_break_even


def _bet(prob: float, side: str = "over") -> BetResult:
    return BetResult(
        bet_id="x",
        player_name="Test Player",
        league="NBA",
        prop_type="Points",
        pp_line=20.5,
        fd_line=20.5,
        side=side,
        true_prob=prob,
        over_odds=-110,
        under_odds=-110,
        both_sided=True,
        pp_player_id="123",
    )


def test_ev_formula_matches_true_prob():
    """individual_ev_pct is the context-aware per-leg EV vs the 6-Power
    break-even, derived from true_prob (no calibration in simplify-v1)."""
    b = _bet(0.60)
    expected = round(score_leg(b.true_prob, slip_n=6, slip_type="power"), 6)
    assert b.individual_ev_pct == pytest.approx(expected, abs=1e-6)


def test_true_prob_clipped_below_one():
    """Kelly math needs log(1 - p) finite. Clip ceiling is 0.999."""
    b = _bet(0.9999)
    assert b.true_prob <= 0.999


def test_raw_true_prob_equals_true_prob():
    """simplify-v1: with no calibration, raw_true_prob == true_prob (both the
    clamped conservative devig)."""
    b = _bet(0.55)
    assert b.raw_true_prob == pytest.approx(0.55)
    assert b.true_prob == pytest.approx(b.raw_true_prob)
    assert b.true_prob <= 0.999


def test_to_dict_carries_true_and_raw():
    """The wire format serializes both probabilities."""
    b = _bet(0.55)
    d = b.to_dict()
    assert "true_prob" in d
    assert "raw_true_prob" in d
    assert d["raw_true_prob"] == pytest.approx(0.55)


def test_edge_relative_to_break_even():
    """edge = true_prob - per_leg_break_even(6, 'power'). Sanity-bound at 50%."""
    b = _bet(0.50)
    assert b.edge == pytest.approx(0.50 - per_leg_break_even(6, "power"), abs=1e-6)
    assert -0.1 < b.edge < 0.1

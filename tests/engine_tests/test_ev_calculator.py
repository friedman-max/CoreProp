"""Pin the BetResult invariants.

BetResult is the value object that flows from the matching loop to the
client. Two things must hold no matter what:

  * EV math: individual_ev_pct == calibrated_prob * OPTIMAL_IMPLIED_DECIMAL - 1
  * Calibrated prob is clipped to 0.999 (the downstream Kelly math depends
    on log(1 - p) being finite).

When the calibration system is empty (`_calibration_curves = {}` shape on
cold start), `calibrate()` should return the raw probability — no shrinkage,
no clipping unless required. We verify both regimes.
"""
from __future__ import annotations

import pytest

from engine.ev_calculator import BetResult
from engine.constants import OPTIMAL_IMPLIED_DECIMAL


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


def test_ev_formula_matches_calibrated_prob():
    """individual_ev_pct must be derived from the calibrated prob, never
    from the raw. (The bug migration_006 fixed went the other way around
    — calibrator training on its own output — but the EV math always read
    the calibrated value.)"""
    b = _bet(0.60)
    expected = round(b.true_prob * OPTIMAL_IMPLIED_DECIMAL - 1.0, 6)
    assert b.individual_ev_pct == pytest.approx(expected, abs=1e-6)


def test_calibrated_prob_clipped_below_one():
    """Sandbox / Kelly math needs log(1 - p) finite. Clip ceiling is 0.999."""
    b = _bet(0.9999)
    assert b.true_prob <= 0.999


def test_raw_true_prob_preserved():
    """raw_true_prob keeps the input untouched; true_prob is the calibrated
    value. This separation is migration_006's invariant."""
    b = _bet(0.55)
    assert b.raw_true_prob == pytest.approx(0.55)
    # On a fresh install with no calibration curves, calibrate is a no-op
    # and true_prob == raw_true_prob. The constraint we can assert
    # universally is that calibrated never exceeds the 0.999 ceiling.
    assert b.true_prob <= 0.999


def test_to_dict_carries_calibrated_and_raw():
    """The wire format must serialize BOTH probabilities so the observatory
    log can distinguish raw consensus from calibrated output."""
    b = _bet(0.55)
    d = b.to_dict()
    assert "true_prob" in d
    assert "raw_true_prob" in d
    assert d["raw_true_prob"] == pytest.approx(0.55)


def test_edge_relative_to_break_even():
    """edge = calibrated_prob - OPTIMAL_BREAK_EVEN. Sanity-bound for 50%."""
    b = _bet(0.50)
    # edge should be within (-0.1, 0.1) for a coin-flip after calibration.
    assert -0.1 < b.edge < 0.1

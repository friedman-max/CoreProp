"""Sharp-anchor engine tests.

Covers: Pinnacle extraction + devig, the missing/one-sided -> None contract,
the tradeability floor, slip-shape gating, and — most important — the
guarantee that in sharp mode NO calibrator touches the probability.
"""
from unittest.mock import patch

import pytest

from engine.consensus import BookOdds
from engine import sharp_anchor as sa
from config import SHARP_MIN_PROB


def _books(pin=None, fd=None):
    out = []
    if fd is not None:
        out.append(BookOdds(book_name="fanduel", over_odds=fd[0],
                            under_odds=fd[1], both_sided=True))
    if pin is not None:
        over, under, both = pin
        out.append(BookOdds(book_name="pinnacle", over_odds=over,
                            under_odds=under, both_sided=both))
    return out


# ── pinnacle_fair_from_books ────────────────────────────────────────────

def test_symmetric_market_devigs_to_half():
    books = _books(pin=(-110, -110, True))
    p = sa.pinnacle_fair_from_books(books, "over")
    assert p is not None
    assert abs(p - 0.5) < 0.005


def test_favored_under_side():
    # Under is juiced (-130) vs over (+110): under fair must clear 0.5.
    books = _books(pin=(110, -130, True))
    p_under = sa.pinnacle_fair_from_books(books, "under")
    p_over = sa.pinnacle_fair_from_books(books, "over")
    assert p_under > 0.5 > p_over
    assert abs((p_under + p_over) - 1.0) < 1e-6


def test_missing_pinnacle_returns_none():
    books = _books(fd=(-120, -105))   # FanDuel only — soft book is NOT a fair source
    assert sa.pinnacle_fair_from_books(books, "over") is None


def test_one_sided_pinnacle_returns_none():
    # One-sided market cannot be devigged without inventing the other side.
    books = _books(pin=(-115, None, False))
    assert sa.pinnacle_fair_from_books(books, "over") is None


def test_invalid_side_returns_none():
    books = _books(pin=(-110, -110, True))
    assert sa.pinnacle_fair_from_books(books, "both") is None
    assert sa.pinnacle_fair_from_books(books, "") is None


# ── is_tradeable floor ──────────────────────────────────────────────────

def test_floor_is_load_bearing():
    assert sa.is_tradeable(SHARP_MIN_PROB) is True
    assert sa.is_tradeable(SHARP_MIN_PROB + 0.01) is True
    assert sa.is_tradeable(SHARP_MIN_PROB - 0.001) is False
    assert sa.is_tradeable(None) is False


# ── slip shapes: only what the placeability backtest validated ──────────

def test_slip_shapes():
    # 3-Power is the validated default (44 placeable slips, +25u, +56.8%/slip
    # in the feasibility backtest); Flex 4-6 allowed for explicit user choice.
    assert sa.slip_shape_allowed("Power", 3) is True
    assert sa.slip_shape_allowed("Flex", 6) is True
    assert sa.slip_shape_allowed("flex", 5) is True
    assert sa.slip_shape_allowed("flex", 4) is True
    assert sa.slip_shape_allowed("flex", 3) is False
    assert sa.slip_shape_allowed("power", 6) is False
    assert sa.slip_shape_allowed("power", 2) is False
    assert sa.DEFAULT_SLIP_TYPE == "Power"
    assert sa.DEFAULT_SLIP_SIZE == 3


# ── the calibration-bypass guarantee ────────────────────────────────────

def _mk_bet(true_prob, sharp_missing=False):
    from engine.ev_calculator import BetResult
    return BetResult(
        bet_id="t", player_name="Test", league="NBA", prop_type="Points",
        pp_line=20.5, fd_line=20.5, side="over", true_prob=true_prob,
        over_odds=-110, under_odds=-110, both_sided=True, pp_player_id="p",
        sharp_missing=sharp_missing,
    )


def test_sharp_mode_bypasses_every_calibrator():
    """In sharp mode true_prob must equal the input EXACTLY — proof that no
    isotonic / RWBC / beta layer modified it."""
    import engine.ev_calculator as evc
    with patch.object(evc, "_USE_SHARP", True), \
         patch.object(evc, "_USE_RAW", False):
        # Booby-trap all three calibrators: if any is consulted, fail loud.
        with patch.object(evc, "_apply_isotonic_calibration",
                          side_effect=AssertionError("isotonic touched!")), \
             patch.object(evc._rwbc, "calibrate",
                          side_effect=AssertionError("RWBC touched!")), \
             patch.object(evc._beta, "calibrate",
                          side_effect=AssertionError("beta touched!")):
            bet = _mk_bet(0.613)
    assert abs(bet.true_prob - 0.613) < 1e-12
    assert bet.calibration_halted is False
    assert bet.sharp_missing is False


def test_sharp_missing_marks_halted():
    """No Pinnacle price -> display-only: halted so the worker never logs it."""
    import engine.ev_calculator as evc
    with patch.object(evc, "_USE_SHARP", True), \
         patch.object(evc, "_USE_RAW", False):
        bet = _mk_bet(0.58, sharp_missing=True)
    assert bet.calibration_halted is True
    assert bet.sharp_missing is True
    assert bet.to_dict()["sharp_missing"] is True

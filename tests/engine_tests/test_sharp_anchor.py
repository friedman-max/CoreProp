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


# ── worst-case fair + mode dispatcher ───────────────────────────────────

def test_worst_case_is_min_across_books():
    # Two books price the same under: worst case must be the LOWER fair.
    books = _books(pin=(-110, -110, True), fd=(-150, 120))
    wc_under = sa.worst_case_fair_from_books(books, "under")
    pin_under = sa.pinnacle_fair_from_books(books, "under")
    assert wc_under is not None
    assert wc_under <= pin_under + 1e-9


def test_worst_case_at_most_shin_per_book():
    # Single book: worst-case (min of 4 methods) <= Shin devig of same odds.
    books = _books(pin=(110, -130, True))
    wc = sa.worst_case_fair_from_books(books, "under")
    shin = sa.pinnacle_fair_from_books(books, "under")
    assert wc is not None and wc <= shin + 1e-9


def test_worst_case_skips_one_sided():
    books = _books(pin=(-115, None, False))
    assert sa.worst_case_fair_from_books(books, "over") is None


def test_mode_pinnacle_ignores_soft_books():
    books = _books(fd=(-140, 110))  # FanDuel only
    with patch.object(sa, "ANCHOR_MODE", "pinnacle"):
        assert sa.fair_from_books(books, "under") is None


def test_mode_hybrid_falls_back_to_worst_case_under_only():
    books = _books(fd=(-140, 110))  # no Pinnacle
    with patch.object(sa, "ANCHOR_MODE", "hybrid"), \
         patch.object(sa, "WORST_CASE_UNDER_ONLY", True):
        assert sa.fair_from_books(books, "under") is not None   # falls back
        assert sa.fair_from_books(books, "over") is None        # OVER gated


def test_mode_hybrid_prefers_pinnacle_when_present():
    books = _books(pin=(-110, -110, True), fd=(-150, 120))
    with patch.object(sa, "ANCHOR_MODE", "hybrid"):
        fair = sa.fair_from_books(books, "under")
        pin = sa.pinnacle_fair_from_books(books, "under")
        assert abs(fair - pin) < 1e-12


def test_mode_worst_case_any_side_when_flag_off():
    books = _books(fd=(-140, 110))
    with patch.object(sa, "ANCHOR_MODE", "worst_case"), \
         patch.object(sa, "WORST_CASE_UNDER_ONLY", False):
        assert sa.fair_from_books(books, "over") is not None


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

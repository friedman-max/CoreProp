"""Soccer fair-value fixes (2026-06-19): favorite-aware single-sided devig,
DraftKings-preferred book selection, and the scoreable-prop gate.
"""
from unittest.mock import patch

from engine.devig import (
    devig_single_sided_scaled,
    devig_single_sided_favorite_aware,
    american_to_implied,
)
from engine.consensus import BookOdds
from engine import sharp_anchor as sa
from engine.results_checker import soccer_prop_scoreable


# ── favorite-aware devig (audit fix #1) ─────────────────────────────────

def test_favorite_is_pulled_below_old_scaled():
    """A heavy milestone OVER favorite must devig LOWER than the old model,
    which left it biased high (the audit's -500 -> 0.79 example)."""
    old = devig_single_sided_scaled(-500)
    new = devig_single_sided_favorite_aware(-500)
    assert new < old
    assert new < 0.76   # was ~0.794


def test_longshot_side_unchanged():
    """Underdog side (implied < 0.5) is identical to the old model — we only
    fixed the favorite under-correction."""
    for od in (120, 200, 300, 500):
        assert abs(devig_single_sided_favorite_aware(od) - devig_single_sided_scaled(od)) < 1e-9


def test_even_money_minimal_hold():
    p = devig_single_sided_favorite_aware(100)  # implied 0.5
    assert abs(p - 0.5 / 1.05) < 1e-6   # only base vig at 0.5


# ── DraftKings-preferred selection (audit fix #2) ───────────────────────

def _book(name, over, under=None, both=False):
    return BookOdds(book_name=name, over_odds=over, under_odds=under, both_sided=both)


def test_prefers_draftkings_over_fanduel():
    """When both books price the over, use clean DraftKings, not corrupt
    FanDuel — even if FanDuel's number would be lower."""
    books = [_book("fanduel", -200), _book("draftkings", -500)]
    fair = sa.single_sided_fair_from_books(books, "over")
    assert abs(fair - devig_single_sided_favorite_aware(-500)) < 1e-9   # DK value


def test_falls_back_to_fanduel_when_no_dk():
    books = [_book("fanduel", -300)]
    fair = sa.single_sided_fair_from_books(books, "over")
    assert abs(fair - devig_single_sided_favorite_aware(-300)) < 1e-9


def test_uses_favorite_aware_not_scaled():
    books = [_book("draftkings", -500)]
    fair = sa.single_sided_fair_from_books(books, "over")
    assert abs(fair - devig_single_sided_favorite_aware(-500)) < 1e-9
    assert fair < devig_single_sided_scaled(-500)   # proves new path, not old


# ── scoreable-prop gate ─────────────────────────────────────────────────

def test_espn_props_always_scoreable():
    for p in ("Shots", "Shots On Target", "Fouls", "Goalie Saves", "Goals", "Assists"):
        assert soccer_prop_scoreable(p) is True


def test_tackles_and_shots_assisted_need_api_football():
    with patch("config.API_FOOTBALL_KEY", ""):
        import importlib, engine.results_checker as rc
        # soccer_prop_scoreable imports the key lazily inside the function
        assert rc.soccer_prop_scoreable("Tackles") is False
        assert rc.soccer_prop_scoreable("Shots Assisted") is False
    with patch("config.API_FOOTBALL_KEY", "test-key-123"):
        import engine.results_checker as rc
        assert rc.soccer_prop_scoreable("Tackles") is True
        assert rc.soccer_prop_scoreable("Shots Assisted") is True


def test_unknown_prop_not_scoreable():
    assert soccer_prop_scoreable("Corner Kicks") is False

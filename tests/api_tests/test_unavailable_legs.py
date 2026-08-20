"""The unavailable-leg pool: a leg the extension could not stage must leave the
candidate pool, and must come back on its own once the TTL lapses."""
import time

import pytest

from web import app as app_mod


@pytest.fixture(autouse=True)
def _clean_pool():
    with app_mod._unavailable_lock:
        app_mod._unavailable_legs.clear()
    yield
    with app_mod._unavailable_lock:
        app_mod._unavailable_legs.clear()


def _bet(player="Shohei Ohtani", prop="Total Bases", line=1.5, side="over"):
    return {"player": player, "prop": prop, "line": line, "side": side}


def test_unmarked_leg_is_available():
    assert app_mod.is_leg_unavailable(_bet()) is False


def test_marked_leg_leaves_the_pool():
    app_mod.mark_legs_unavailable([_bet()])
    assert app_mod.is_leg_unavailable(_bet()) is True


def test_marking_is_per_leg_not_per_player():
    """Blocking one line must not block the player's other props — that would
    throw away good candidates every time a single line moved."""
    app_mod.mark_legs_unavailable([_bet(prop="Total Bases")])
    assert app_mod.is_leg_unavailable(_bet(prop="Total Bases")) is True
    assert app_mod.is_leg_unavailable(_bet(prop="Hits")) is False
    assert app_mod.is_leg_unavailable(_bet(line=2.5)) is False
    assert app_mod.is_leg_unavailable(_bet(side="under")) is False


def test_key_normalises_case_whitespace_and_line_format():
    """The extension and the scraper format these differently; 1.5 and "1.50"
    are the same bet and must not both be blockable independently."""
    app_mod.mark_legs_unavailable([_bet(player="  SHOHEI Ohtani ", line="1.50")])
    assert app_mod.is_leg_unavailable(_bet(player="shohei ohtani", line=1.5)) is True


def test_accepts_scraper_field_names_too():
    """is_leg_unavailable is called with rows from the bets pool, which use
    player_name / prop_type / pp_line rather than the extension's names."""
    app_mod.mark_legs_unavailable([_bet()])
    assert app_mod.is_leg_unavailable({
        "player_name": "Shohei Ohtani", "prop_type": "Total Bases",
        "pp_line": 1.5, "side": "over",
    }) is True


def test_leg_returns_to_the_pool_after_ttl():
    """PrizePicks reinstates pulled lines routinely, so the block must expire
    by itself rather than needing a restart."""
    app_mod.mark_legs_unavailable([_bet()])
    assert app_mod.is_leg_unavailable(_bet()) is True
    with app_mod._unavailable_lock:
        for k in app_mod._unavailable_legs:
            app_mod._unavailable_legs[k] = time.monotonic() - 1   # expire it
    assert app_mod.is_leg_unavailable(_bet()) is False


def test_expired_entries_are_pruned_not_merely_ignored():
    app_mod.mark_legs_unavailable([_bet()])
    with app_mod._unavailable_lock:
        for k in app_mod._unavailable_legs:
            app_mod._unavailable_legs[k] = time.monotonic() - 1
    app_mod.is_leg_unavailable(_bet())
    with app_mod._unavailable_lock:
        assert app_mod._unavailable_legs == {}


def test_blank_legs_are_ignored():
    """A malformed failure report must not poison the pool with an empty key
    that would then match other malformed rows."""
    assert app_mod.mark_legs_unavailable([{}, {"player": "", "prop": ""}]) == 0
    with app_mod._unavailable_lock:
        assert app_mod._unavailable_legs == {}

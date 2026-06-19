"""API-Football fallback scorer — disabled-without-key contract + parsing."""
from datetime import datetime, timezone
from unittest.mock import patch

from engine.apifootball import APIFootballClient


def test_disabled_without_key_returns_none():
    c = APIFootballClient(api_key="")
    assert c.enabled is False
    assert c.get_player_stat("Anyone", datetime(2026, 6, 15, tzinfo=timezone.utc), "Tackles") is None


def test_unsupported_prop_returns_none():
    c = APIFootballClient(api_key="k")
    assert c.get_player_stat("X", datetime(2026, 6, 15, tzinfo=timezone.utc), "Shots") is None


# Mocked two-call flow: fixtures-for-date then players-for-fixture.
_FIXTURES = {"response": [
    {"fixture": {"id": 999, "date": "2026-06-15T19:00:00+00:00"}},
]}
_PLAYERS = {"response": [
    {"players": [
        {"player": {"name": "Viktor Gyökeres"},
         "statistics": [{"tackles": {"total": 3}, "passes": {"key": 2}}]},
    ]},
]}


def _fake_get(self, path, params):
    return _FIXTURES if path == "/fixtures" else _PLAYERS


def test_tackles_and_key_passes_parse():
    c = APIFootballClient(api_key="k")
    gs = datetime(2026, 6, 15, 19, 0, tzinfo=timezone.utc)
    with patch.object(APIFootballClient, "_get", _fake_get):
        assert c.get_player_stat("Viktor Gyokeres", gs, "Tackles") == 3
        assert c.get_player_stat("Viktor Gyokeres", gs, "Shots Assisted") == 2


def test_player_not_in_fixture_returns_none():
    c = APIFootballClient(api_key="k")
    gs = datetime(2026, 6, 15, 19, 0, tzinfo=timezone.utc)
    with patch.object(APIFootballClient, "_get", _fake_get):
        assert c.get_player_stat("Nobody Here", gs, "Tackles") is None


def test_kickoff_far_from_game_start_skipped():
    """A fixture >6h from the leg's game_start isn't matched."""
    c = APIFootballClient(api_key="k")
    gs = datetime(2026, 6, 15, 3, 0, tzinfo=timezone.utc)  # 16h before the fixture
    with patch.object(APIFootballClient, "_get", _fake_get):
        assert c.get_player_stat("Viktor Gyökeres", gs, "Tackles") is None

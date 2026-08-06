"""The landing minigame's public API contract.

The load-bearing property: GET /api/public/daily-pick must not contain the
answer in any form (no probabilities, no favored side, no book odds) — the
Network tab must be useless before a pick. The reveal then returns numbers
that are re-derivable from the books array it ships.

Selection math lives in tests/engine_tests/test_minigame_selector.py; these
tests exercise the HTTP layer with a mocked match snapshot and no network.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import config as cfg
from web import minigame
from web.state import _lock, _state

_FUTURE = "2099-01-01T00:00:00Z"


def _rows(player, over_am=-150, under_am=120, *, line=23.5, trending=1000, image=None):
    base = {
        "player_name": player, "league": "WNBA", "stat_type": "Points",
        "pp_line": line, "start_time": _FUTURE,
        "pp_player_id": f"pid-{player}", "odds_type": "standard",
        "team": "IND", "opponent": "LVA", "position": "G",
        "trending_count": trending,
    }
    p = minigame.devig_books([
        {"book": "FanDuel", "side": "more", "american": over_am},
        {"book": "FanDuel", "side": "less", "american": under_am},
    ])
    over = {**base, "side": "over", "fd_odds": over_am, "dk_odds": -138,
            "pin_odds": None, "nv_odds": None, "true_prob": round(p[0], 4)}
    under = {**base, "side": "under", "fd_odds": under_am, "dk_odds": 115,
             "pin_odds": None, "nv_odds": None, "true_prob": round(p[1], 4)}
    return [over, under]


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _seed(monkeypatch):
    """Fresh frozen cache, no Supabase, a 5-candidate snapshot (one with an
    image URL), and an empty rate-limiter table for every test."""
    from web.routers import public as public_router

    minigame.reset_for_tests()
    monkeypatch.setattr(minigame, "load_state_from_supabase", lambda key: (None, None))
    monkeypatch.setattr(minigame, "sync_state_to_supabase", lambda key, data: True)
    public_router._rate_hits.clear()
    public_router._image_cache.clear()

    rows = []
    for i in range(5):
        rows += _rows(f"Player {i:02d}", trending=100 + i)
    with _lock:
        _state["matches"] = rows
        _state["pp_player_images"] = {
            f"pid-Player {i:02d}": f"https://images.prizepicks.invalid/p{i}.png"
            for i in range(5)
        }
    yield
    minigame.reset_for_tests()
    public_router._rate_hits.clear()
    public_router._image_cache.clear()
    with _lock:
        _state["matches"] = []
        _state["pp_player_images"] = {}


# ── daily-pick ─────────────────────────────────────────────────────────────

def test_daily_pick_shape(client):
    r = client.get("/api/public/daily-pick")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body["day_index"], int)
    assert len(body["picks"]) == 3
    for p in body["picks"]:
        assert len(p["id"]) == 12
        assert p["league"] == "WNBA"
        assert p["prop"] == "Points"
        assert p["line"] == 23.5
        assert p["team"] == "IND" and p["opponent"] == "LVA"
        assert p["position"] == "G"
        assert isinstance(p["trending_count"], int)
        assert p["image_url"] == f"/api/public/player-image?pick={p['id']}"


def test_daily_pick_leaks_no_answer(client):
    """The whole game: nothing in the pre-pick payload may reveal the favored
    side. Checked by key-name scan of the raw body, not just the parsed shape,
    so a helpfully-added field fails loudly."""
    r = client.get("/api/public/daily-pick")
    raw = r.text.lower()
    for forbidden in ("p_more", "p_less", "favored", "vig", "books", "american",
                      "odds", "prob", "image_source_url"):
        assert forbidden not in raw, f"daily-pick payload leaks '{forbidden}'"


def test_daily_pick_is_frozen_for_the_day(client):
    ids1 = [p["id"] for p in client.get("/api/public/daily-pick").json()["picks"]]
    # Snapshot churns mid-day; the board must not.
    with _lock:
        _state["matches"] = []
    ids2 = [p["id"] for p in client.get("/api/public/daily-pick").json()["picks"]]
    assert ids1 == ids2


# ── reveal ─────────────────────────────────────────────────────────────────

def test_reveal_unknown_id_404(client):
    assert client.get("/api/public/daily-pick/reveal", params={"id": "nope00000000"}).status_code == 404


def test_reveal_falls_back_to_recent_frozen_days(client):
    """A pick id from a previous frozen day still grades: the visitor's board
    can outlive the blob that served it (tab open across the 8am ET boundary,
    or the Render-wake unfrozen board replaced by the real freeze), and the
    reveal must not 404 those ids into a permanent retry loop. The frozen day
    stores the full pick dict, so grading is exact."""
    old_pick = {
        "id": "oldboard0001", "player": "Yesterday Player", "team": "IND",
        "opponent": "LVA", "position": "G", "league": "WNBA", "prop": "Points",
        "line": 20.5, "game_start": "2020-01-01T00:00:00Z", "trending_count": 5,
        "image_source_url": None, "favored": "more", "p_more": 0.57,
        "p_less": 0.43, "vig_pct": 4.5,
        "books": [{"book": "FanDuel", "side": "more", "american": -150},
                  {"book": "FanDuel", "side": "less", "american": 120}],
    }
    idx = minigame.day_index()
    with minigame._frozen_lock:
        minigame._frozen_cache[idx - 1] = {"day_index": idx - 1, "picks": [old_pick]}

    # Today's board freezes from the 5-candidate snapshot and does NOT contain
    # the old id — the hit below can only come from the lookback.
    today_ids = {p["id"] for p in client.get("/api/public/daily-pick").json()["picks"]}
    assert "oldboard0001" not in today_ids

    r = client.get("/api/public/daily-pick/reveal", params={"id": "oldboard0001"})
    assert r.status_code == 200
    body = r.json()
    assert body["favored"] == "more"
    assert body["p_more"] == 0.57


def test_reveal_consistent_with_returned_books(client):
    """p_more/p_less/vig_pct must be re-derivable from exactly the books
    array in the same response (mean of per-book implied, normalized)."""
    from engine.devig import american_to_implied

    picks = client.get("/api/public/daily-pick").json()["picks"]
    for p in picks:
        r = client.get("/api/public/daily-pick/reveal", params={"id": p["id"]})
        assert r.status_code == 200
        body = r.json()
        assert body["id"] == p["id"]
        assert body["favored"] in ("more", "less")

        more = [american_to_implied(b["american"]) for b in body["books"] if b["side"] == "more"]
        less = [american_to_implied(b["american"]) for b in body["books"] if b["side"] == "less"]
        assert more and less
        avg_more = sum(more) / len(more)
        avg_less = sum(less) / len(less)
        total = avg_more + avg_less
        assert body["p_more"] == pytest.approx(avg_more / total, abs=1e-3)
        assert body["p_less"] == pytest.approx(avg_less / total, abs=1e-3)
        assert body["vig_pct"] == pytest.approx((total - 1.0) * 100.0, abs=0.05)
        favored_p = body["p_more"] if body["favored"] == "more" else body["p_less"]
        assert favored_p >= 0.5


# ── player-image ───────────────────────────────────────────────────────────

def test_player_image_rejects_unknown_id(client):
    """The id gate is what keeps this from being an open proxy: only ids in
    today's frozen selection resolve."""
    assert client.get("/api/public/player-image", params={"pick": "deadbeef0000"}).status_code == 404


def test_player_image_404_when_headshots_disabled(client, monkeypatch):
    monkeypatch.setattr(cfg, "HEADSHOTS_ENABLED", False)
    pick_id = None
    # Freeze first (daily-pick still works with headshots off — image_url null).
    body = client.get("/api/public/daily-pick").json()
    pick_id = body["picks"][0]["id"]
    assert all(p["image_url"] is None for p in body["picks"])
    assert client.get("/api/public/player-image", params={"pick": pick_id}).status_code == 404


def test_player_image_404_when_upstream_unreachable(client):
    # The seeded image URLs point at an .invalid TLD, so the server-side fetch
    # fails; the contract is 404 (frontend falls back), never a 5xx.
    pick_id = client.get("/api/public/daily-pick").json()["picks"][0]["id"]
    assert client.get("/api/public/player-image", params={"pick": pick_id}).status_code == 404


# ── event ──────────────────────────────────────────────────────────────────

def test_event_accepts_known_enum(client):
    for event in ("game_viewed", "pick_made", "revealed", "plays_exhausted", "cta_clicked"):
        r = client.post("/api/public/event", json={"event": event, "day_index": 20666})
        assert r.status_code == 204, event


def test_event_rejects_unknown_event(client):
    r = client.post("/api/public/event", json={"event": "drop_tables", "day_index": 20666})
    assert r.status_code == 422


def test_event_rejects_missing_day_index(client):
    r = client.post("/api/public/event", json={"event": "game_viewed"})
    assert r.status_code == 422


def test_event_rejects_oversized_meta(client):
    """meta lands in an uncapped jsonb column; the endpoint bounds it so an
    unauthenticated caller can't use telemetry as a storage-flood primitive."""
    r = client.post("/api/public/event", json={
        "event": "game_viewed", "day_index": 20666,
        "meta": {"blob": "x" * 5000},
    })
    assert r.status_code == 422


# ── rate limiter ───────────────────────────────────────────────────────────

def test_rate_limiter_kicks_in(client):
    from web.routers import public as public_router

    # Exhaust the window quickly instead of issuing 60 real requests.
    ok = client.get("/api/public/daily-pick")
    assert ok.status_code == 200
    with public_router._rate_lock:
        (hits,) = public_router._rate_hits.values()
        while len(hits) < public_router._RATE_LIMIT:
            hits.append(hits[-1])
    assert client.get("/api/public/daily-pick").status_code == 429

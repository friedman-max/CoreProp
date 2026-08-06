"""web/minigame.py — the landing minigame's daily selector.

Covers the contract's load-bearing math with no network and no TestClient:

  * day_index flips at 8am America/New_York, not midnight (7:59 vs 8:01).
  * Pair building applies each filter: goblins out (a goblin square has no
    Less button), one-sided markets out, favored-probability band, and the
    |p_over + p_under - 1| <= 0.02 complement sanity check.
  * Same day -> same picks (deterministic seeded shuffle, recompute-safe).
  * The reveal numbers are re-derivable from the stored books array — the
    whole point of the receipt.
  * Backfill pulls from the most recent previous day's frozen selection and
    never invents cards.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from web import minigame
from web.state import _lock, _state

_ET = ZoneInfo("America/New_York")

# A ~57% over market: implied(-150)=0.600, implied(+120)=0.4545 -> p_more≈0.569.
_FAV_OVER = (-150, 120)
# A coin flip: -110/-110 -> p_more=0.5, outside every band.
_COIN = (-110, -110)
# A heavy favorite: implied(-250)=0.714, implied(+190)=0.345 -> p_more≈0.674 —
# outside the strict [0.55, 0.60] band, inside the widened >=0.55 band.
_HEAVY = (-250, 190)

_FUTURE = "2099-01-01T00:00:00Z"


def _rows(player, over_under=_FAV_OVER, *, league="WNBA", prop="Points", line=23.5,
          start=_FUTURE, odds_type="standard", trending=1000,
          true_over=None, true_under=None, pid=None):
    """One over row + one under row, shaped like web/app.py's serialized
    match rows (base fields + per-book display odds + true_prob)."""
    over_am, under_am = over_under
    if true_over is None:
        # Default consensus agrees with the quotes closely enough to pass the
        # complement check.
        p = minigame.devig_books([
            {"book": "FanDuel", "side": "more", "american": over_am},
            {"book": "FanDuel", "side": "less", "american": under_am},
        ])
        true_over, true_under = round(p[0], 4), round(p[1], 4)
    base = {
        "player_name": player, "league": league, "stat_type": prop,
        "pp_line": line, "start_time": start,
        "pp_player_id": pid or f"pid-{player}",
        "odds_type": odds_type, "team": "IND", "opponent": "LVA",
        "position": "G", "trending_count": trending,
    }
    over = {**base, "side": "over", "fd_odds": over_am, "dk_odds": None,
            "pin_odds": None, "nv_odds": None, "true_prob": true_over}
    under = {**base, "side": "under", "fd_odds": under_am, "dk_odds": None,
             "pin_odds": None, "nv_odds": None, "true_prob": true_under}
    return [over, under]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """No Supabase, empty caches, empty snapshot — each test builds its own."""
    minigame.reset_for_tests()
    monkeypatch.setattr(minigame, "load_state_from_supabase", lambda key: (None, None))
    monkeypatch.setattr(minigame, "sync_state_to_supabase", lambda key, data: True)
    with _lock:
        _state["matches"] = []
        _state["pp_player_images"] = {}
    yield
    minigame.reset_for_tests()
    with _lock:
        _state["matches"] = []
        _state["pp_player_images"] = {}


# ── day_index ──────────────────────────────────────────────────────────────

def test_day_boundary_is_8am_eastern():
    before = datetime(2026, 8, 5, 7, 59, tzinfo=_ET)
    after = datetime(2026, 8, 5, 8, 1, tzinfo=_ET)
    assert minigame.day_index(after) == minigame.day_index(before) + 1


def test_day_index_same_instant_any_timezone():
    # 12:30 UTC on Aug 5 == 8:30am ET (EDT): same instant, same bucket.
    utc = datetime(2026, 8, 5, 12, 30, tzinfo=timezone.utc)
    et = datetime(2026, 8, 5, 8, 30, tzinfo=_ET)
    assert minigame.day_index(utc) == minigame.day_index(et)


def test_day_index_stable_within_a_day():
    a = datetime(2026, 8, 5, 9, 0, tzinfo=_ET)
    b = datetime(2026, 8, 6, 7, 59, tzinfo=_ET)
    assert minigame.day_index(a) == minigame.day_index(b)


# ── pair building + filters ────────────────────────────────────────────────

def _candidates(rows):
    return minigame._build_candidates(rows, {})


def test_pairs_over_and_under_for_same_market():
    cands = _candidates(_rows("Kelsey Mitchell"))
    assert len(cands) == 1
    c = cands[0]
    assert c["player"] == "Kelsey Mitchell"
    assert c["favored"] == "more"
    assert len(c["books"]) == 2  # one book, both sides


def test_unpaired_side_is_dropped():
    over_only = _rows("Kelsey Mitchell")[:1]
    assert _candidates(over_only) == []


def test_goblin_is_excluded():
    # Layout requirement, not preference: a goblin square has no Less button.
    assert _candidates(_rows("Goblin Player", odds_type="goblin")) == []


def test_one_sided_books_are_excluded():
    rows = _rows("One Sided")
    rows[1]["fd_odds"] = None  # book quotes over but not under
    assert _candidates(rows) == []


def test_derived_display_odds_do_not_count_as_posted():
    """fd_odds existing on both rows is not enough: web/app.py's display odds
    can be DERIVED from the complement (_display_odds re-vigs the opposite
    side, and the matcher reuses the exact-line prop for both equivalents).
    The per-side *_posted flags web/app.py stamps are the evidence the book
    actually quoted the side; a False flag must fail the both-sides gate, or
    the receipt would show a price the book never posted and the devig would
    run on the book's own mirrored complement."""
    rows = _rows("Derived Under")
    rows[0]["fd_posted"] = True
    rows[1]["fd_posted"] = False  # the under-side fd_odds is a re-vigged invention
    assert _candidates(rows) == []


def test_rows_without_posted_flags_default_to_posted():
    """Legacy rows serialized before the *_posted flags existed keep working
    (the fixture rows above carry no flags at all); the gate defaults to True
    and self-heals once the next pipeline cycle stamps real flags."""
    assert len(_candidates(_rows("Legacy Row"))) == 1


def test_complement_violation_is_excluded():
    # Consensus sides that don't sum to ~1 would make the reveal contradict
    # the +EV board (|0.60 + 0.36 - 1| = 0.04 > 0.02).
    rows = _rows("Torn Market", true_over=0.60, true_under=0.36)
    assert _candidates(rows) == []


def test_complement_within_tolerance_is_kept():
    rows = _rows("Slightly Torn", true_over=0.575, true_under=0.44)  # off by 0.015
    assert len(_candidates(rows)) == 1


def test_band_excludes_coin_flips_and_heavy_favorites():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    cands = _candidates(
        _rows("In Band") + _rows("Coin Flip", _COIN) + _rows("Heavy Fav", _HEAVY)
    )
    pool = minigame._pool(cands, minigame._BAND_LO, minigame._BAND_HI, now)
    assert [c["player"] for c in pool] == ["In Band"]


def test_widened_band_admits_heavy_favorites_but_never_coin_flips():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    cands = _candidates(_rows("Coin Flip", _COIN) + _rows("Heavy Fav", _HEAVY))
    pool = minigame._pool(cands, minigame._BAND_LO, None, now)
    assert [c["player"] for c in pool] == ["Heavy Fav"]


def test_future_games_preferred_when_enough_qualify():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(3):
        rows += _rows(f"Future {i}", trending=100 + i)
    rows += _rows("Started", start="2020-01-01T00:00:00Z")
    picks = minigame._select_picks(_candidates(rows), idx=123, now_utc=now)
    assert len(picks) == 3
    assert all(p["player"].startswith("Future") for p in picks)


def test_started_games_allowed_when_future_pool_is_short():
    # "in the future at freeze time if possible" — with only 2 future
    # qualifiers the started game fills the board rather than leaving a hole.
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = _rows("Future A") + _rows("Future B", line=24.5) \
        + _rows("Started", start="2020-01-01T00:00:00Z", line=25.5)
    picks = minigame._select_picks(_candidates(rows), idx=123, now_utc=now)
    assert {p["player"] for p in picks} == {"Future A", "Future B", "Started"}


def test_selection_ranks_by_trending_and_caps_pool_at_top8():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(12):
        rows += _rows(f"Player {i:02d}", trending=i)
    cands = _candidates(rows)
    picks = minigame._select_picks(cands, idx=7, now_utc=now)
    # The 4 least-trending candidates (00-03) can never appear: the shuffle
    # draws only from the top-8 trending pool.
    assert len(picks) == 3
    assert all(int(p["player"].split()[-1]) >= 4 for p in picks)


def test_one_card_per_player():
    # Trending concentrates on stars: without dedup the live board froze
    # "A'ja Wilson, A'ja Wilson, Caitlin Clark". One card per player unless
    # the pool has fewer than 3 distinct players.
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = _rows("A'ja Wilson", prop="Points", trending=3000) \
        + _rows("A'ja Wilson", prop="Assists", line=3.5, trending=2900) \
        + _rows("A'ja Wilson", prop="Rebounds", line=9.5, trending=2800) \
        + _rows("Caitlin Clark", line=21.5, trending=1600) \
        + _rows("Kelsey Mitchell", line=23.5, trending=1500)
    picks = minigame._select_picks(_candidates(rows), idx=123, now_utc=now)
    assert len(picks) == 3
    assert {p["player"] for p in picks} == {"A'ja Wilson", "Caitlin Clark",
                                           "Kelsey Mitchell"}


def test_duplicate_players_fill_when_pool_is_short():
    # A repeat card beats an empty slot: 2 distinct players, 3 markets.
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = _rows("A'ja Wilson", prop="Points", trending=3000) \
        + _rows("A'ja Wilson", prop="Assists", line=3.5, trending=2900) \
        + _rows("Caitlin Clark", line=21.5, trending=1600)
    picks = minigame._select_picks(_candidates(rows), idx=123, now_utc=now)
    assert len(picks) == 3
    assert sum(1 for p in picks if p["player"] == "A'ja Wilson") == 2


def test_floor_tier_fires_only_when_band_and_backfill_are_empty():
    """Launch-day 2am case observed live: 72 real candidates, zero in band,
    zero frozen history — the hero's game must not vanish. The floor tier
    takes the strongest sub-band candidates (>=0.52) rather than nothing."""
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    minigame.reset_for_tests()
    # -125/+101 devigs to .5276 and -130/+105 to .5367: floor-eligible
    # (>=0.52) but below the .55 band, mirroring the observed live slate.
    rows = _rows("Mike Trout", (-125, 101), prop="Hits+Runs+RBIs") \
        + _rows("Elly De La Cruz", (-130, 105), prop="Total Bases", line=1.5) \
        + _rows("Dustin May", (-110, -110), prop="Pitcher Strikeouts", line=4.5)
    with _lock:
        _state["matches"] = rows
    _idx, blob = minigame.get_or_freeze_today(now)
    picks = blob["picks"]
    # -110/-110 devigs to exactly 0.5/0.5 — below the 0.52 floor, excluded.
    assert {p["player"] for p in picks} == {"Mike Trout", "Elly De La Cruz"}
    # Strongest lesson first.
    favs = [max(p["p_more"], p["p_less"]) for p in picks]
    assert favs == sorted(favs, reverse=True)
    minigame.reset_for_tests()


def test_backfilled_board_is_served_but_not_frozen():
    """Observed live at the 2026-08-06 8:04am rollover: the scrape succeeded
    but nothing qualified in band, so all three slots came from yesterday's
    blob. Freezing that would pin stale cards for 24h — the board must stay
    provisional so the evening slate can still claim the day."""
    minigame.reset_for_tests()
    # Yesterday: a real in-band board that freezes.
    yesterday = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    with _lock:
        _state["matches"] = _rows("Kelsey Mitchell") + _rows("Caitlin Clark", line=21.5) \
            + _rows("A'ja Wilson", line=19.5)
    y_idx, y_blob = minigame.get_or_freeze_today(yesterday)
    assert len(y_blob["picks"]) == 3

    # Today, 8:04am ET: scrape is non-empty but holds nothing in band.
    morning = datetime(2026, 8, 6, 12, 4, tzinfo=timezone.utc)
    assert minigame.day_index(morning) == y_idx + 1
    with _lock:
        _state["matches"] = _rows("Coin Flip", _COIN)
    blob, freezable = minigame._compute_blob(minigame.day_index(morning), morning)
    assert [p["id"] for p in blob["picks"]] == [p["id"] for p in y_blob["picks"]]
    assert freezable is False, "backfilled morning board must not freeze"
    minigame.reset_for_tests()


def test_thin_day_settles_after_the_deadline():
    """The provisional state can't last all day — a board that reshuffles
    every scrape is worse than a settled marginal one."""
    minigame.reset_for_tests()
    with _lock:
        _state["matches"] = _rows("Mike Trout", (-125, 101)) \
            + _rows("Elly De La Cruz", (-130, 105), line=1.5) \
            + _rows("Trea Turner", (-128, 103), line=2.5)
    # 9am ET — inside the settle window, sub-band board stays provisional.
    early = datetime(2026, 8, 6, 13, 0, tzinfo=timezone.utc)
    _blob, freezable = minigame._compute_blob(minigame.day_index(early), early)
    assert freezable is False
    # 1pm ET — past the deadline, the same board settles.
    late = datetime(2026, 8, 6, 17, 0, tzinfo=timezone.utc)
    blob, freezable = minigame._compute_blob(minigame.day_index(late), late)
    assert freezable is True and len(blob["picks"]) == 3
    minigame.reset_for_tests()


def test_hours_since_boundary_handles_pre_8am():
    # 2am ET belongs to the board that started 8am the previous day: 18h in.
    pre = datetime(2026, 8, 6, 6, 0, tzinfo=timezone.utc)   # 02:00 ET
    assert minigame._hours_since_boundary(pre) == pytest.approx(18.0, abs=0.1)
    post = datetime(2026, 8, 6, 13, 0, tzinfo=timezone.utc)  # 09:00 ET
    assert minigame._hours_since_boundary(post) == pytest.approx(1.0, abs=0.1)


# ── determinism ────────────────────────────────────────────────────────────

def test_same_day_same_picks():
    rows = []
    for i in range(10):
        rows += _rows(f"Player {i:02d}", trending=100 + i)
    with _lock:
        _state["matches"] = rows

    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    idx1, blob1 = minigame.get_or_freeze_today(now)
    minigame.reset_for_tests()  # simulate a restart: recompute from scratch
    idx2, blob2 = minigame.get_or_freeze_today(now)
    assert idx1 == idx2
    assert [p["id"] for p in blob1["picks"]] == [p["id"] for p in blob2["picks"]]


def test_different_days_can_reorder():
    # The seed is day_index; two days over the same snapshot must not be
    # forced into the same order (shuffle actually depends on the seed).
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(8):
        rows += _rows(f"Player {i:02d}", trending=100 + i)
    cands = _candidates(rows)
    orders = {
        tuple(p["id"] for p in minigame._select_picks(cands, idx=idx, now_utc=now))
        for idx in range(10)
    }
    assert len(orders) > 1


def test_frozen_blob_is_served_not_recomputed():
    rows = []
    for i in range(5):
        rows += _rows(f"Player {i:02d}", trending=100 + i)
    with _lock:
        _state["matches"] = rows
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    _idx, blob1 = minigame.get_or_freeze_today(now)

    # The snapshot changes mid-day (new scrape); the frozen board must not.
    with _lock:
        _state["matches"] = []
    _idx, blob2 = minigame.get_or_freeze_today(now)
    assert blob1 is blob2


# ── reveal consistency ─────────────────────────────────────────────────────

def test_reveal_rederivable_from_books():
    """The receipt contract: mean-of-implied per side over the returned books,
    normalized, must reproduce p_more/p_less; the pre-normalization overround
    must reproduce vig_pct."""
    from engine.devig import american_to_implied

    rows = _rows("Kelsey Mitchell")
    # Add a second book so the mean is actually a mean.
    rows[0]["dk_odds"] = -138
    rows[1]["dk_odds"] = 115
    (c,) = _candidates(rows)

    more = [american_to_implied(b["american"]) for b in c["books"] if b["side"] == "more"]
    less = [american_to_implied(b["american"]) for b in c["books"] if b["side"] == "less"]
    avg_more = sum(more) / len(more)
    avg_less = sum(less) / len(less)
    total = avg_more + avg_less

    assert c["p_more"] == pytest.approx(avg_more / total, abs=1e-3)
    assert c["p_less"] == pytest.approx(avg_less / total, abs=1e-3)
    assert c["p_more"] + c["p_less"] == pytest.approx(1.0, abs=1e-3)
    assert c["vig_pct"] == pytest.approx((total - 1.0) * 100.0, abs=0.05)
    assert c["favored"] == ("more" if c["p_more"] >= c["p_less"] else "less")


# ── backfill ───────────────────────────────────────────────────────────────

def _fake_pick(pick_id: str) -> dict:
    return {
        "id": pick_id, "player": f"Yesterday {pick_id}", "team": "IND",
        "opponent": "LVA", "position": "G", "league": "WNBA", "prop": "Points",
        "line": 20.5, "game_start": "2020-01-01T00:00:00Z", "trending_count": 5,
        "image_source_url": None, "favored": "more", "p_more": 0.57,
        "p_less": 0.43, "vig_pct": 4.5,
        "books": [{"book": "FanDuel", "side": "more", "american": -150},
                  {"book": "FanDuel", "side": "less", "american": 120}],
    }


def test_backfill_fills_from_most_recent_frozen_day():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    idx = minigame.day_index(now)
    with minigame._frozen_lock:
        minigame._frozen_cache[idx - 1] = {
            "day_index": idx - 1,
            "picks": [_fake_pick("aaa"), _fake_pick("bbb"), _fake_pick("ccc")],
        }
    # Today only one market qualifies -> two slots backfill from yesterday.
    with _lock:
        _state["matches"] = _rows("Only Today")
    _i, blob = minigame.get_or_freeze_today(now)
    players = [p["player"] for p in blob["picks"]]
    assert len(blob["picks"]) == 3
    assert "Only Today" in players
    assert sum(p.startswith("Yesterday") for p in players) == 2


def test_backfill_exhausted_returns_short_board_not_inventions():
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    with _lock:
        _state["matches"] = _rows("Only Today")
    _i, blob = minigame.get_or_freeze_today(now)
    assert [p["player"] for p in blob["picks"]] == ["Only Today"]


def test_empty_snapshot_is_not_frozen():
    # Render-just-woke-up: no matches yet. Serving nothing is fine; freezing
    # nothing for the whole day is not.
    now = datetime(2026, 8, 5, 15, 0, tzinfo=timezone.utc)
    idx, blob = minigame.get_or_freeze_today(now)
    assert blob["picks"] == []
    with minigame._frozen_lock:
        assert idx not in minigame._frozen_cache

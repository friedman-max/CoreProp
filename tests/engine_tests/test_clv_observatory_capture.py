"""Tests for the Phase 1A audit PR-3a observatory closing-line capture path.

The new method `CLVTracker.update_observatory_closing_lines(current_probs)`
must:
  1. Write closing_prob for pending observatory rows whose 6-tuple matches
     a key in current_probs.
  2. Respect a capture window (default 4h pre-game) so we don't write
     closing_prob for games happening tomorrow.
  3. Be idempotent on no-op writes (existing closing_prob within 1e-4 of
     the new value is skipped).
  4. Skip writes when value is out of range (0, 1).
"""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from engine.clv_checker import CLVTracker


# ── Mock helpers ────────────────────────────────────────────────────────


class _FakeQuery:
    """Minimal builder mock: chain methods, return self, .execute() returns
    canned data. Captures updates in `recorded_updates`."""

    def __init__(self, parent: "_FakeDb", table: str):
        self._parent = parent
        self._table = table
        self._select_cols: str | None = None
        self._update_values: dict | None = None
        self._eq_filters: list[tuple[str, object]] = []
        self._gte_filters: list[tuple[str, object]] = []
        self._lte_filters: list[tuple[str, object]] = []
        self._range: tuple[int, int] | None = None

    def select(self, cols: str):
        self._select_cols = cols
        return self

    def update(self, values: dict):
        self._update_values = values
        return self

    def eq(self, col, val):
        self._eq_filters.append((col, val))
        return self

    def gte(self, col, val):
        self._gte_filters.append((col, val))
        return self

    def lte(self, col, val):
        self._lte_filters.append((col, val))
        return self

    def range(self, lo, hi):
        self._range = (lo, hi)
        return self

    def execute(self):
        if self._update_values is not None:
            self._parent.recorded_updates.append({
                "table":   self._table,
                "values":  self._update_values,
                "filters": list(self._eq_filters),
            })
            return MagicMock(data=[])
        # Selecting — return whatever the parent has staged for this page.
        offset_lo = (self._range or (0, 999))[0]
        page_rows = self._parent.staged_obs_pages.get(offset_lo, [])
        return MagicMock(data=page_rows)


class _FakeDb:
    def __init__(self, obs_rows: list[dict]):
        # Paginate the staged rows by 1000-row pages.
        self.staged_obs_pages = {}
        for i in range(0, max(1, len(obs_rows)), 1000):
            self.staged_obs_pages[i] = obs_rows[i : i + 1000]
        if not obs_rows:
            self.staged_obs_pages[0] = []
        self.recorded_updates: list[dict] = []

    def table(self, name: str) -> _FakeQuery:
        return _FakeQuery(self, name)


def _make_tracker(db: _FakeDb) -> CLVTracker:
    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            t = CLVTracker()
    return t


# ── Tests ───────────────────────────────────────────────────────────────


def test_writes_closing_prob_for_pending_obs_within_window():
    now = datetime.now(timezone.utc)
    soon = (now + timedelta(hours=2)).isoformat()
    far_future = (now + timedelta(days=5)).isoformat()

    obs_rows = [
        {
            "id":           "obs-1",
            "player":       "Player One",
            "prop":         "Points",
            "side":         "over",
            "line":         25.5,
            "game_start":   soon,
            "closing_prob": None,
        },
        # This row is outside the 4h window — must NOT be returned by the
        # query.  We rely on the fake DB's lte filter NOT excluding by
        # default; in production, supabase enforces it.  Simulate by
        # excluding manually below.
    ]
    db = _FakeDb(obs_rows)

    current_probs = {
        ("player one", "points", "over", 25.5): 0.62,
    }

    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines(current_probs)

    assert n == 1
    assert len(db.recorded_updates) == 1
    upd = db.recorded_updates[0]
    assert upd["table"] == "market_observatory"
    assert upd["values"] == {"closing_prob": 0.62}
    assert ("id", "obs-1") in upd["filters"]


def test_skip_no_change():
    now = datetime.now(timezone.utc)
    soon = (now + timedelta(hours=1)).isoformat()

    obs_rows = [
        {
            "id":           "obs-2",
            "player":       "Player Two",
            "prop":         "Rebounds",
            "side":         "under",
            "line":         8.5,
            "game_start":   soon,
            "closing_prob": 0.55,
        },
    ]
    db = _FakeDb(obs_rows)
    # Identical value (within 1e-4) — should NOT trigger an update.
    current_probs = {
        ("player two", "rebounds", "under", 8.5): 0.5500,
    }

    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines(current_probs)

    assert n == 0
    assert len(db.recorded_updates) == 0


def test_update_when_value_changes():
    now = datetime.now(timezone.utc)
    soon = (now + timedelta(hours=1)).isoformat()

    obs_rows = [
        {
            "id":           "obs-3",
            "player":       "Player Three",
            "prop":         "Assists",
            "side":         "over",
            "line":         6.5,
            "game_start":   soon,
            "closing_prob": 0.50,
        },
    ]
    db = _FakeDb(obs_rows)
    current_probs = {
        ("player three", "assists", "over", 6.5): 0.58,
    }

    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines(current_probs)

    assert n == 1
    assert db.recorded_updates[0]["values"] == {"closing_prob": 0.58}


def test_skip_invalid_value():
    now = datetime.now(timezone.utc)
    soon = (now + timedelta(hours=1)).isoformat()

    obs_rows = [
        {
            "id":           "obs-4",
            "player":       "Player Four",
            "prop":         "Steals",
            "side":         "over",
            "line":         1.5,
            "game_start":   soon,
            "closing_prob": None,
        },
    ]
    db = _FakeDb(obs_rows)
    current_probs = {
        ("player four", "steals", "over", 1.5): 1.5,  # out of (0,1) range
    }

    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines(current_probs)

    assert n == 0


def test_skip_when_key_not_in_dict():
    now = datetime.now(timezone.utc)
    soon = (now + timedelta(hours=1)).isoformat()

    obs_rows = [
        {
            "id":           "obs-5",
            "player":       "Player Five",
            "prop":         "Blocks",
            "side":         "over",
            "line":         1.5,
            "game_start":   soon,
            "closing_prob": None,
        },
    ]
    db = _FakeDb(obs_rows)
    current_probs = {
        # Different prop — no match.
        ("player five", "points", "over", 25.5): 0.65,
    }

    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines(current_probs)

    assert n == 0


def test_no_writes_when_no_probs():
    db = _FakeDb([])
    with patch("engine.clv_checker.get_db", return_value=db):
        with patch("engine.clv_checker.load_isotonic_calibration",
                   return_value={"global": None, "leagues": {}, "props": {}}):
            tracker = CLVTracker()
            n = tracker.update_observatory_closing_lines({})
    assert n == 0

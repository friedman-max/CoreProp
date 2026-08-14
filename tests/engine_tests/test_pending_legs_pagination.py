"""Result grading and CLV must paginate their pending-legs reads.

PostgREST silently caps an unbounded `select(...).execute()` at 1000 rows. The
repo already knows this: five call sites paginate with an explicit comment
saying so (engine/calibration.py, web/app.py's backtest read), and
tests/engine_tests/test_analytics_pagination.py exists to pin it.

Three readers on the WRITE path were never migrated:

  * engine/results_checker.check_pending_results  — grades legs from ESPN
  * engine/clv_checker.update_closing_lines_from_probs — captures closing lines
  * engine/clv_checker.finalize_missed — scans the whole legs table

All three do `.select(...).eq("result","pending").execute()` with no `.range()`,
no `.limit()` and — critically — no `.order()`. Once a user's pending set
exceeds 1000, an arbitrary and *unstable* subset becomes invisible to both
graders, so those legs never resolve. They then stay `result='pending'`, which
keeps the pending set above 1000: the failure is SELF-REINFORCING, not a
one-time truncation.

The cost is not just cosmetic. Unresolved legs are excluded from analytics, and
`analysis/12_side_bias_refit.py` refits SIDE_BIAS from settled rows — so a
silently biased sample propagates into the betting thresholds themselves.

`_CappedQuery` below emulates the cap: an unpaged read returns only the first
1000 rows, a `.range()` read honours its window. A leg past the cap must still
be graded.
"""
from __future__ import annotations


class _Res:
    def __init__(self, data):
        self.data = data


class _CappedQuery:
    """PostgREST builder enforcing the silent 1000-row cap on unpaged reads."""

    HARD_CAP = 1000

    def __init__(self, rows):
        self._rows = rows
        self._eq = []
        self._is_null = []
        self._lo = None
        self._hi = None
        self._limit = None
        self.ordered = False

    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        self._eq.append((col, val))
        return self

    def is_(self, col, val):
        # emulate PostgREST `<col>=is.null` (only null is used here)
        self._is_null.append(("is", col))
        return self

    @property
    def not_(self):
        # `<col>=not.is.null`: `.not_` returns a negating proxy that records the
        # next `.is_` and hands control back to this builder for chaining.
        parent = self

        class _Neg:
            def is_(self, col, val):
                parent._is_null.append(("not_is", col))
                return parent

        return _Neg()

    def in_(self, *a, **k):
        return self

    def order(self, *a, **k):
        self.ordered = True
        return self

    def limit(self, n):
        self._limit = n
        return self

    def range(self, lo, hi):
        self._lo, self._hi = lo, hi
        return self

    def update(self, *a, **k):
        return self

    def delete(self, *a, **k):
        return self

    def insert(self, *a, **k):
        return self

    def execute(self):
        rows = [r for r in self._rows
                if all(r.get(c) == v for c, v in self._eq)]
        for kind, col in self._is_null:
            if kind == "is":
                rows = [r for r in rows if r.get(col) is None]
            else:  # not_is: column IS NOT NULL
                rows = [r for r in rows if r.get(col) is not None]
        if self._lo is not None:
            return _Res(rows[self._lo : self._hi + 1])
        return _Res(rows[: self.HARD_CAP])


class _CappedDB:
    def __init__(self, legs):
        self.legs = legs
        self.seen_unpaged = False

    def table(self, name):
        q = _CappedQuery(self.legs if name == "legs" else [])
        self._last = q
        return q


def _pending_legs(n: int, *, marker_player: str) -> list[dict]:
    """n pending legs; the LAST one is the marker, so it sits past the cap."""
    rows = []
    for i in range(n - 1):
        rows.append({
            "id": f"L{i:05d}", "slip_id": f"S{i:05d}", "leg_num": 1,
            "player": f"Filler {i}", "league": "MLB", "prop": "Hits",
            "line": 1.5, "side": "over", "true_prob": 0.6,
            "result": "pending", "game_start": "2026-07-01T23:00:00Z",
            "closing_prob": None, "clv_pct": None, "raw_true_prob": 0.6,
        })
    rows.append({
        "id": "MARKER", "slip_id": "SMARKER", "leg_num": 1,
        "player": marker_player, "league": "MLB", "prop": "Hits",
        "line": 1.5, "side": "over", "true_prob": 0.6,
        "result": "pending", "game_start": "2026-07-01T23:00:00Z",
        "closing_prob": None, "clv_pct": None, "raw_true_prob": 0.6,
    })
    return rows


def _collect_read_rows(monkeypatch, module, call, legs=None) -> list[dict]:
    """Run `call` against a capped DB and return every leg row the code saw."""
    if legs is None:
        legs = _pending_legs(1300, marker_player="Marker Player")
    db = _CappedDB(legs)
    seen: list[dict] = []

    orig = _CappedQuery.execute

    def spy(self):
        res = orig(self)
        if isinstance(res.data, list):
            seen.extend(r for r in res.data if isinstance(r, dict) and "player" in r)
        return res

    monkeypatch.setattr(_CappedQuery, "execute", spy)
    monkeypatch.setattr(module, "get_db", lambda: db)
    try:
        call()
    except Exception:
        # We assert on what was READ, not on downstream grading (which would
        # need live ESPN). A failure past the read is fine.
        pass
    return seen


def test_results_checker_reads_past_the_1000_row_cap(monkeypatch):
    import engine.results_checker as rc

    checker = rc.ESPNResultsChecker()
    # Keep it OFFLINE by cutting the HTTP session, not individual methods. There
    # are two independent ESPN paths (`_fetch_all_stats` for box scores and
    # `_fetch_gamelog_stats`, a per-player search fallback), so patching one by
    # name still left 1300 live requests — 186s, and an earlier attempt patched a
    # method that didn't even exist, which `raising=False` silently allowed.
    # Killing `_session` is exhaustive and can't drift as methods are renamed.
    class _NoNetwork:
        def get(self, *a, **k):
            raise AssertionError(
                "test attempted a live HTTP request: " + str(a[:1])
            )

    monkeypatch.setattr(checker, "_session", _NoNetwork())
    assert hasattr(checker, "_fetch_all_stats"), "ESPN fetch method renamed"
    monkeypatch.setattr(checker, "_fetch_all_stats", lambda *a, **k: {})

    seen = _collect_read_rows(monkeypatch, rc, checker.check_pending_results)
    players = {r["player"] for r in seen}

    assert players, "no legs were read at all — test wiring is wrong"
    assert "Marker Player" in players, (
        f"the leg past the 1000-row cap was never read ({len(players)} legs seen). "
        "check_pending_results does an unpaginated select, so legs beyond the cap "
        "never resolve — permanently."
    )


def test_clv_closing_line_read_paginates(monkeypatch):
    import engine.clv_checker as cc

    tracker = cc.CLVTracker()
    seen = _collect_read_rows(
        monkeypatch, cc,
        lambda: tracker.update_closing_lines_from_probs({}, {}),
    )
    players = {r["player"] for r in seen}

    assert players, "no legs were read at all — test wiring is wrong"
    assert "Marker Player" in players, (
        f"the leg past the 1000-row cap was never read ({len(players)} legs seen). "
        "update_closing_lines_from_probs does an unpaginated select, so those "
        "legs never get a closing line and are excluded from CLV."
    )


def test_finalize_missed_filters_to_partial_writes(monkeypatch):
    """finalize_missed must push its candidate predicate to the server.

    It only ever acts on *partial-write* rows — `closing_prob` captured but
    `clv_pct` still null (the client-side skip at the top of its loop). Before
    the fix it fetched the WHOLE `legs` table every cycle (settled + pending,
    all columns in `cols`) and filtered in Python — the largest remaining
    Supabase egress line item, and on a live board it returns finalized=0
    almost every time. Pushing `closing_prob IS NOT NULL AND clv_pct IS NULL`
    down to PostgREST means only the handful of real candidates ever transfer.

    This pins BOTH properties: (1) the two server-side null filters exclude
    non-candidate rows from the read, and (2) the read still paginates, so a
    candidate sitting past the 1000-row cap is still finalized.
    """
    import engine.clv_checker as cc

    def _candidate(sid, player):
        return {
            "slip_id": sid, "leg_num": 1,
            "closing_prob": 0.55, "clv_pct": None,
            "true_prob": 0.6, "raw_true_prob": 0.6,
            "game_start": "2026-07-01T23:00:00Z", "player": player,
        }

    # 1300 partial-write candidates; the marker is last so it lands past the cap.
    legs = [_candidate(f"S{i:05d}", f"Cand {i}") for i in range(1299)]
    legs.append(_candidate("SMARKER", "Marker Player"))
    # Two rows the server-side filter MUST exclude, placed on the first page:
    legs.insert(3, {  # closing line was never captured -> not a candidate
        "slip_id": "SNOCLOSE", "leg_num": 1,
        "closing_prob": None, "clv_pct": None,
        "true_prob": 0.6, "raw_true_prob": 0.6,
        "game_start": "2026-07-01T23:00:00Z", "player": "NoClose Skip",
    })
    legs.insert(4, {  # already finalized -> not a candidate
        "slip_id": "SDONE", "leg_num": 1,
        "closing_prob": 0.55, "clv_pct": 0.02,
        "true_prob": 0.6, "raw_true_prob": 0.6,
        "game_start": "2026-07-01T23:00:00Z", "player": "Already Done",
    })

    tracker = cc.CLVTracker()
    seen = _collect_read_rows(monkeypatch, cc, tracker.finalize_missed, legs=legs)
    players = {r["player"] for r in seen}

    assert players, "no legs were read at all — test wiring is wrong"
    assert "Marker Player" in players, (
        "the partial-write candidate past the 1000-row cap was never read — "
        "finalize_missed must still paginate its (now filtered) read."
    )
    assert "NoClose Skip" not in players, (
        "finalize_missed read a row with closing_prob IS NULL — the server-side "
        "`.not_.is_('closing_prob','null')` filter is missing, so it is still "
        "transferring non-candidate rows (the egress waste this fix removes)."
    )
    assert "Already Done" not in players, (
        "finalize_missed read an already-finalized row (clv_pct not null) — the "
        "server-side `.is_('clv_pct','null')` filter is missing."
    )

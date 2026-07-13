"""Regression tests for auto-backtest slip logging.

Covers two bugs where slips silently failed to reach the backtest:
  1. BREAK_EVEN.get((n, type)) returned None for combos not in the table
     (e.g. ("2","flex")), and the code bailed on None -> the slip was never
     logged. Now falls back to OPTIMAL_BREAK_EVEN.
  2. The auto path writes user_id on the slip header and every leg (required
     so the RLS-scoped read path can return them). Guard it doesn't regress,
     and that the returned n_legs reflects the real leg count (was hardcoded 6).
"""
from __future__ import annotations


class _FakeQuery:
    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.inserts = []
    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def eq(self, *a, **k): return self
    def in_(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def execute(self):
        class _Res:
            def __init__(self, data): self.data = data
        return _Res(self._rows)
    def insert(self, rows):
        self.inserts.append(rows if isinstance(rows, list) else [rows])
        return self


class _FakeDB:
    def __init__(self):
        self._slips_q = _FakeQuery()
        self._legs_q = _FakeQuery()
    def table(self, name):
        if name == "slips": return self._slips_q
        if name == "legs": return self._legs_q
        raise KeyError(name)


def _bet(i, *, ev=0.06, prob=0.62, team=None):
    return {
        "player_name": f"P{i}", "prop_type": "Points", "pp_line": 20.5,
        "side": "over", "start_time": "2025-05-13T19:00:00Z",
        "team": team or ("ATL" if i < 2 else "BOS"),
        "true_prob": prob, "raw_true_prob": prob, "individual_ev_pct": ev,
    }


def test_flex_two_leg_slip_is_not_silently_dropped():
    # ("2","flex") is absent from BREAK_EVEN; before the fix, slip_be was None
    # and try_log_slip returned None (slip never logged). With the fallback it
    # gates on OPTIMAL_BREAK_EVEN and logs a +EV Flex-2 slip.
    from engine.backtest import BacktestLogger
    # Two high-prob legs on different teams so the 2-team rule passes.
    bets = [_bet(0, prob=0.70, team="ATL"), _bet(1, prob=0.70, team="BOS")]
    db = _FakeDB()
    bl = BacktestLogger(user_id="u1", db_client=db)
    result = bl.try_log_slip(bets, slip_type="Flex", n_legs=2)
    assert result is not None, "Flex-2 slip was silently dropped"
    assert len(db._slips_q.inserts) == 1
    assert len(db._legs_q.inserts) == 1


def test_logged_slip_carries_user_id_and_correct_n_legs():
    from engine.backtest import BacktestLogger
    bets = [_bet(i, ev=0.05 + 0.001 * i) for i in range(3)]
    db = _FakeDB()
    bl = BacktestLogger(user_id="user-42", db_client=db)
    result = bl.try_log_slip(bets, slip_type="Power", n_legs=3)
    assert result is not None
    # n_legs must reflect the real leg count, not a hardcoded 6.
    assert result["n_legs"] == 3
    # Header carries user_id (RLS ownership).
    header = db._slips_q.inserts[0][0]
    assert header.get("user_id") == "user-42"
    assert header.get("n_legs") == 3
    # Every leg carries user_id too.
    legs = db._legs_q.inserts[0]
    assert legs and all(l.get("user_id") == "user-42" for l in legs)


# ── Orphaned-header rollback ─────────────────────────────────────────────────
# The slip header and its legs are two separate inserts (PostgREST has no
# cross-table transaction). If the legs insert fails for a NON-duplicate reason
# and the optional-column retries are exhausted, the already-committed header
# must be rolled back — otherwise it shows up as an "N-leg" slip with zero legs.

class _RollbackQuery:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self._del_id = None
    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def eq(self, col, val): self._del_id = val; return self
    def in_(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def insert(self, rows):
        self._pending = rows if isinstance(rows, list) else [rows]
        self._op = "insert"
        return self
    def delete(self):
        self._op = "delete"
        return self
    def execute(self):
        class _Res:
            def __init__(self, data): self.data = data
        if self.name == "slips":
            if getattr(self, "_op", None) == "insert":
                self.db.slip_ids.update(r["id"] for r in self._pending)
            elif getattr(self, "_op", None) == "delete":
                self.db.slip_ids.discard(self._del_id)
                self.db.deleted.append(self._del_id)
            return _Res([])
        # legs table: every insert fails with a non-duplicate error
        if getattr(self, "_op", None) == "insert":
            raise RuntimeError("legs insert boom (not a duplicate)")
        return _Res([])


class _RollbackDB:
    def __init__(self):
        self.slip_ids = set()
        self.deleted = []
    def table(self, name):
        return _RollbackQuery(self, name)


def test_non_duplicate_legs_failure_rolls_back_header():
    # Reproduces the "6-leg slip with no legs" bug: legs insert fails for a
    # reason other than a 23505 duplicate, exhausting the optional-column
    # retries. The header written first must be deleted so no orphaned,
    # legless slip survives.
    from engine.backtest import BacktestLogger
    bets = [_bet(i, ev=0.05 + 0.001 * i) for i in range(6)]
    db = _RollbackDB()
    bl = BacktestLogger(user_id="u1", db_client=db)
    result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)
    assert result is None, "a failed legs insert must not report success"
    # The header must have been rolled back — no orphaned slip left behind.
    assert db.slip_ids == set(), f"orphaned slip header(s) survived: {db.slip_ids}"
    assert db.deleted, "header rollback delete was never issued"


# ── Multi-slip-per-cycle support ────────────────────────────────────────────
# The refresh worker loops try_log_slip until the pool can't build another
# distinct slip. That only works if try_log_slip's DB-driven dedup sees the
# slips committed by earlier iterations. This stateful fake feeds inserted
# legs back into the dedup select, mirroring Supabase, so we can prove the
# loop drains a multi-slip pool and then terminates.

class _StatefulDB:
    """Minimal Supabase stand-in that remembers inserted slips + legs and
    replays them through the same select chain BacktestLogger.\
_fetch_recent_slips_with_legs uses."""
    def __init__(self):
        self.slips = []   # list of {id, user_id, timestamp}
        self.legs = []    # list of leg dicts (must carry slip_id, player, ...)

    def table(self, name):
        return _StatefulQuery(self, name)


class _StatefulQuery:
    def __init__(self, db, name):
        self.db = db
        self.name = name
        self._filter_ids = None
        self._eq_slip_id = None   # from .eq("slip_id", X) — the idempotent readback
    def select(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def eq(self, col=None, val=None, *a, **k):
        # insert_legs_idempotent reads back with .eq("slip_id", slip_id); honor
        # it so the fake filters like real PostgREST (a no-op eq would return
        # every slip's legs and make the helper think legs already exist).
        if col == "slip_id":
            self._eq_slip_id = val
        return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def in_(self, col, vals):
        self._filter_ids = set(vals)
        return self
    def insert(self, rows):
        rows = rows if isinstance(rows, list) else [rows]
        target = self.db.slips if self.name == "slips" else self.db.legs
        target.extend(rows)
        return self
    def delete(self, *a, **k): return self
    def execute(self):
        class _Res:
            def __init__(self, data): self.data = data
        if self.name == "slips":
            return _Res([{"id": s["id"], "timestamp": s.get("timestamp", "")}
                         for s in self.db.slips])
        # legs
        rows = self.db.legs
        if self._filter_ids is not None:
            rows = [l for l in rows if l.get("slip_id") in self._filter_ids]
        if self._eq_slip_id is not None:
            rows = [l for l in rows if l.get("slip_id") == self._eq_slip_id]
        return _Res(rows)


def test_worker_loop_logs_multiple_distinct_slips_then_stops():
    from engine.backtest import BacktestLogger
    # Pool big enough for two 3-leg Power slips (6 distinct players, 2 teams).
    bets = [_bet(i, ev=0.06, prob=0.66, team=("ATL" if i % 2 == 0 else "BOS"))
            for i in range(6)]
    db = _StatefulDB()
    bl = BacktestLogger(user_id="u1", db_client=db)

    # Replicate the worker's _log_pool loop (bounded).
    logged = 0
    for _ in range(10):
        if bl.try_log_slip(bets, slip_type="Power", n_legs=3) is None:
            break
        logged += 1

    # Two full slips fit; the third call finds every player already used and
    # returns None, so the loop terminates rather than spinning to the cap.
    assert logged == 2, f"expected 2 distinct slips, logged {logged}"
    assert len(db.slips) == 2
    # 2 slips × 3 legs, all distinct players.
    assert len(db.legs) == 6
    assert len({l["player"] for l in db.legs}) == 6


# ── Lost-response double-insert (the "12-leg slip" bug) ──────────────────────
# A PostgREST batch legs-insert is atomic server-side, but the CLIENT can't
# tell a genuine failure from a LOST RESPONSE (read timeout / reset / 5xx from
# the edge AFTER Postgres committed the rows). The old code retried the same
# batch on any non-23505 error, double-inserting every leg → a 6-leg config
# produced a 12-leg slip. insert_legs_idempotent must re-read what landed and
# insert only the missing legs, so the slip ends up with exactly n_legs.

class _LostResponseLegsQuery:
    """legs table whose FIRST insert commits the rows and THEN raises a
    lost-response error (as httpx would on a read timeout). Subsequent reads
    see the committed rows; subsequent inserts behave normally."""
    def __init__(self, db):
        self.db = db
        self._filter_slip = None
        self._op = None
        self._pending = None
    def select(self, *a, **k): return self
    def eq(self, col, val):
        if col == "slip_id":
            self._filter_slip = val
        return self
    def in_(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def insert(self, rows):
        self._op = "insert"
        self._pending = rows if isinstance(rows, list) else [rows]
        return self
    def delete(self):
        self._op = "delete"
        return self
    def execute(self):
        class _Res:
            def __init__(self, data): self.data = data
        if self._op == "insert":
            # Commit server-side FIRST...
            self.db.legs.extend(self._pending)
            self.db.insert_calls += 1
            # ...then simulate a lost response on the very first insert only.
            if self.db.insert_calls == 1:
                import httpx
                raise httpx.ReadTimeout("response lost after commit")
            return _Res([])
        if self._op == "delete":
            return _Res([])
        # select readback: rows for this slip_id
        rows = [l for l in self.db.legs if l.get("slip_id") == self._filter_slip]
        return _Res(rows)


class _LostResponseDB:
    def __init__(self):
        self.slips = []
        self.legs = []
        self.insert_calls = 0
    def table(self, name):
        if name == "slips":
            # Reuse the simple stateful slips query (insert/select/delete).
            return _LostResponseSlipsQuery(self)
        return _LostResponseLegsQuery(self)


class _LostResponseSlipsQuery:
    def __init__(self, db):
        self.db = db
        self._op = None
        self._pending = None
        self._del_id = None
    def select(self, *a, **k): return self
    def eq(self, col, val): self._del_id = val; return self
    def in_(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def gte(self, *a, **k): return self
    def insert(self, rows):
        self._op = "insert"
        self._pending = rows if isinstance(rows, list) else [rows]
        return self
    def delete(self):
        self._op = "delete"
        return self
    def execute(self):
        class _Res:
            def __init__(self, data): self.data = data
        if self._op == "insert":
            self.db.slips.extend(self._pending)
        elif self._op == "delete":
            self.db.slips = [s for s in self.db.slips if s.get("id") != self._del_id]
        return _Res([{"id": s["id"], "timestamp": s.get("timestamp", "")}
                     for s in self.db.slips])


def test_lost_response_retry_does_not_double_insert_legs():
    from engine.backtest import BacktestLogger
    bets = [_bet(i, ev=0.05 + 0.001 * i) for i in range(6)]
    db = _LostResponseDB()
    bl = BacktestLogger(user_id="u1", db_client=db)
    result = bl.try_log_slip(bets, slip_type="Power", n_legs=6)

    # The slip is logged successfully despite the lost-response error...
    assert result is not None, "slip should still log after a recovered lost response"
    # ...and has EXACTLY 6 legs, not 12. Each leg_num appears once.
    assert len(db.legs) == 6, f"expected 6 legs, got {len(db.legs)} (double-insert regression)"
    leg_nums = sorted(l["leg_num"] for l in db.legs)
    assert leg_nums == [1, 2, 3, 4, 5, 6], f"leg_num set corrupted: {leg_nums}"


def test_insert_legs_idempotent_helper_is_a_noop_when_all_present():
    # If every leg already landed (a lost response AFTER a full commit), the
    # helper's readback sees all leg_nums present and inserts nothing more.
    from engine.backtest import insert_legs_idempotent
    db = _LostResponseDB()
    legs = [{"slip_id": "S1", "leg_num": i, "player": f"P{i}"} for i in range(1, 7)]
    # Pre-seed as if the first (lost-response) insert already committed them.
    db.legs.extend(legs)
    db.insert_calls = 1  # so no further insert would raise
    insert_legs_idempotent(db, "S1", list(legs))
    assert len(db.legs) == 6, "helper must not re-insert already-present legs"

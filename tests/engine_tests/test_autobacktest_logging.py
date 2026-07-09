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

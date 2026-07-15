"""
Regression: analytics must paginate the legs/slips fetches.

PostgREST silently caps an unbounded `select(...).execute()` at 1000 rows. The
analytics P&L timeline builds `legs_by_slip` from that fetch and skips any slip
whose legs are absent — so once a user has >1000 legs, a real winning slip
whose legs fall past the cap silently vanishes from analytics (and its win-day
shows no winning bets). evaluate_analytics must page through every row.
"""
from __future__ import annotations

import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


class _Res:
    def __init__(self, data):
        self.data = data


class _Query:
    """Emulates the PostgREST builder for one table, enforcing the 1000-row
    hard cap on any select that doesn't page with .range()."""
    HARD_CAP = 1000

    def __init__(self, table_rows):
        self._rows = table_rows
        self._lo = None
        self._hi = None

    def select(self, *a, **k): return self
    def in_(self, *a, **k): return self
    def eq(self, *a, **k): return self
    def order(self, *a, **k): return self

    def range(self, lo, hi):
        self._lo, self._hi = lo, hi
        return self

    def execute(self):
        if self._lo is not None:
            # Paged read: honor the requested window.
            return _Res(self._rows[self._lo:self._hi + 1])
        # Unpaged read: PostgREST silently truncates to the hard cap.
        return _Res(self._rows[: self.HARD_CAP])


class _PagedDB:
    def __init__(self, slips, legs):
        self._slips = slips
        self._legs = legs
    def table(self, name):
        if name == "slips":
            return _Query(self._slips)
        if name == "legs":
            return _Query(self._legs)
        raise KeyError(name)


class AnalyticsPaginationTests(unittest.TestCase):
    def test_winning_slip_past_1000_legs_still_in_pnl(self):
        import engine.calibration as cal

        # Build 200 six-leg slips = 1200 legs, all losers, plus ONE winning
        # 6-leg Power slip placed LAST (its legs sit at rows 1200-1205, well
        # past the 1000-row cap). slip_id ordering puts the winner last.
        slips = []
        legs = []
        for i in range(200):
            sid = f"S{i:04d}"
            slips.append({"id": sid, "timestamp": f"2026-06-{(i % 28) + 1:02d}T20:00:00Z", "slip_type": "power"})
            for k in range(6):
                # Loser slips: one miss so payout=0.
                legs.append({
                    "result": "miss" if k == 0 else "hit",
                    "true_prob": 0.6, "player": f"P{i}_{k}", "prop": "Points",
                    "side": "over", "league": "MLB", "slip_id": sid,
                    "closing_prob": None, "clv_pct": None,
                })
        win_sid = "ZWIN0001"  # sorts after all "S..." ids
        slips.append({"id": win_sid, "timestamp": "2026-07-09T20:00:00Z", "slip_type": "power"})
        for k in range(6):
            legs.append({
                "result": "hit",
                "true_prob": 0.62, "player": f"W{k}", "prop": "Points",
                "side": "over", "league": "MLB", "slip_id": win_sid,
                "closing_prob": None, "clv_pct": None,
            })

        self.assertGreater(len(legs), _Query.HARD_CAP, "test needs >1000 legs to exercise the cap")

        db = _PagedDB(slips, legs)
        orig = cal.get_user_db
        cal.get_user_db = lambda jwt: db
        try:
            out = cal.evaluate_analytics(user_jwt="x")
        finally:
            cal.get_user_db = orig

        timeline = out.get("pnl_timeline", [])
        win_points = [p for p in timeline if p.get("slip_id") == win_sid]
        self.assertTrue(win_points, "winning slip past the 1000-row cap is missing from the P&L timeline")
        # 6-leg Power all-hit pays 37.5x (PrizePicks' current table) → pnl =
        # 37.5 - 1 = 36.5 on a 1-unit stake.
        self.assertAlmostEqual(win_points[0]["pnl"], 36.5, places=4)
        self.assertGreaterEqual(out.get("won_slips", 0), 1)


if __name__ == "__main__":
    unittest.main()

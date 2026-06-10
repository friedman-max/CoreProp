"""Breakdown + monthly-bucket aggregation:
`StrategyTester._build_breakdowns` and `._monthly_buckets`.

These power the by-league / by-hits / by-stat tables and the monthly
drift chart in the sandbox. They aggregate the same `sim_slips` list
`run_simulation` builds. The contracts worth pinning:
  * win-rate denominators exclude bet=0 (Kelly-sized-out) slips,
  * thin buckets (< MIN_BUCKET_SLIPS) are flagged, not hidden,
  * by_stat is per-LEG (not per-slip) and splits stake/profit across legs,
  * rows sort by profit descending,
  * monthly buckets key by calendar month, oldest first.
"""
from __future__ import annotations

import unittest

from engine.strategy_tester import StrategyTester, MIN_BUCKET_SLIPS
from tests.engine_tests._sandbox_fixtures import sim_slip


def _leg(prop, result):
    return {"player": "P", "prop": prop, "result": result}


class BuildBreakdownsTests(unittest.TestCase):
    def test_by_league_splits_and_computes_roi(self):
        slips = [
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", league="NBA",
                     payout=6.0, bet_size=1.0),   # +5
            sim_slip(timestamp="2026-05-02T00:00:00+00:00", league="NBA",
                     payout=0.0, bet_size=1.0, hits=2),  # -1
            sim_slip(timestamp="2026-05-03T00:00:00+00:00", league="MLB",
                     payout=3.0, bet_size=1.0),   # +2
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        by_league = {r["league"]: r for r in out["by_league"]}
        self.assertEqual(by_league["NBA"]["slips"], 2)
        self.assertEqual(by_league["NBA"]["profit"], 4.0)   # +5 -1
        self.assertEqual(by_league["NBA"]["bet"], 2.0)
        self.assertEqual(by_league["NBA"]["roi_pct"], 200.0)  # 4/2
        self.assertEqual(by_league["NBA"]["win_rate_pct"], 50.0)  # 1 of 2
        self.assertEqual(by_league["MLB"]["profit"], 2.0)

    def test_rows_sorted_by_profit_descending(self):
        slips = [
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", league="NBA",
                     payout=0.0, bet_size=1.0),   # -1
            sim_slip(timestamp="2026-05-02T00:00:00+00:00", league="MLB",
                     payout=11.0, bet_size=1.0),  # +10
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        leagues = [r["league"] for r in out["by_league"]]
        self.assertEqual(leagues, ["MLB", "NBA"], "should sort by profit desc")

    def test_zero_bet_slips_excluded_from_win_rate_denominator(self):
        # A Kelly-sized-out slip (bet=0) counts in `slips` but not in the
        # win-rate denominator (which is bet_slips).
        slips = [
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", league="NBA",
                     payout=6.0, bet_size=1.0),            # bet win
            sim_slip(timestamp="2026-05-02T00:00:00+00:00", league="NBA",
                     payout=0.0, bet_size=0.0, profit=0.0),  # sized out
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        nba = next(r for r in out["by_league"] if r["league"] == "NBA")
        self.assertEqual(nba["slips"], 2)
        self.assertEqual(nba["win_rate_pct"], 100.0,
            "the sized-out slip must not dilute win rate")

    def test_thin_bucket_flagged(self):
        slips = [sim_slip(timestamp="2026-05-01T00:00:00+00:00", league="NBA")]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        nba = next(r for r in out["by_league"] if r["league"] == "NBA")
        self.assertTrue(nba["is_thin"], "1 slip is below MIN_BUCKET_SLIPS")

    def test_fat_bucket_not_flagged(self):
        slips = [
            sim_slip(timestamp=f"2026-05-01T00:00:{i:02d}+00:00", league="NBA")
            for i in range(MIN_BUCKET_SLIPS)
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        nba = next(r for r in out["by_league"] if r["league"] == "NBA")
        self.assertFalse(nba["is_thin"])

    def test_by_hits_groups_on_effective_hits(self):
        slips = [
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", hits=3),
            sim_slip(timestamp="2026-05-02T00:00:00+00:00", hits=3),
            sim_slip(timestamp="2026-05-03T00:00:00+00:00", hits=2, payout=0.0),
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        by_hits = {r["hits"]: r for r in out["by_hits"]}
        self.assertEqual(by_hits[3]["slips"], 2)
        self.assertEqual(by_hits[2]["slips"], 1)

    def test_by_stat_is_per_leg_and_splits_stake(self):
        # One 2-leg slip, stake 2.0, profit 10. Each leg gets stake 1.0
        # and profit 5.0. Points hit, Rebounds missed.
        slips = [sim_slip(
            timestamp="2026-05-01T00:00:00+00:00", n_legs=2, hits=1,
            payout=12.0, bet_size=2.0,
            legs=[_leg("Points", "hit"), _leg("Rebounds", "miss")],
        )]
        out = StrategyTester._build_breakdowns(slips, with_ci=False)
        by_stat = {r["stat_type"]: r for r in out["by_stat"]}
        self.assertAlmostEqual(by_stat["Points"]["bet"], 1.0)
        self.assertAlmostEqual(by_stat["Points"]["profit"], 5.0)
        self.assertEqual(by_stat["Points"]["win_rate_pct"], 100.0)   # 1/1 hit
        self.assertEqual(by_stat["Rebounds"]["win_rate_pct"], 0.0)   # 0/1 hit

    def test_with_ci_attaches_band_when_enough_slips(self):
        slips = [
            sim_slip(timestamp=f"2026-05-01T00:00:{i:02d}+00:00", league="NBA",
                     payout=(6.0 if i % 2 == 0 else 0.0), bet_size=1.0)
            for i in range(10)
        ]
        out = StrategyTester._build_breakdowns(slips, with_ci=True)
        nba = next(r for r in out["by_league"] if r["league"] == "NBA")
        self.assertIn("ci", nba)
        self.assertIn("roi_pct", nba["ci"])

    def test_empty_input_yields_empty_groups(self):
        out = StrategyTester._build_breakdowns([], with_ci=True)
        self.assertEqual(out["by_league"], [])
        self.assertEqual(out["by_hits"], [])
        self.assertEqual(out["by_stat"], [])


class MonthlyBucketTests(unittest.TestCase):
    def test_buckets_keyed_by_month_oldest_first(self):
        slips = [
            sim_slip(timestamp="2026-04-15T00:00:00+00:00", payout=6.0, bet_size=1.0),
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", payout=0.0, bet_size=1.0),
            sim_slip(timestamp="2026-05-20T00:00:00+00:00", payout=6.0, bet_size=1.0),
        ]
        out = StrategyTester._monthly_buckets(slips)
        self.assertEqual([b["month"] for b in out], ["2026-04", "2026-05"])
        may = next(b for b in out if b["month"] == "2026-05")
        self.assertEqual(may["slips"], 2)
        self.assertEqual(may["profit"], 4.0)   # +5 -1
        self.assertEqual(may["roi_pct"], 200.0)
        self.assertEqual(may["win_rate_pct"], 50.0)

    def test_empty_input_returns_empty_list(self):
        self.assertEqual(StrategyTester._monthly_buckets([]), [])

    def test_zero_bet_slip_counted_in_slips_not_win_rate(self):
        slips = [
            sim_slip(timestamp="2026-05-01T00:00:00+00:00", payout=6.0, bet_size=1.0),
            sim_slip(timestamp="2026-05-02T00:00:00+00:00", payout=0.0,
                     bet_size=0.0, profit=0.0),
        ]
        out = StrategyTester._monthly_buckets(slips)
        may = out[0]
        self.assertEqual(may["slips"], 2)
        self.assertEqual(may["win_rate_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()

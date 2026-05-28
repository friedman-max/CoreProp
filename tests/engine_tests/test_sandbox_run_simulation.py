"""End-to-end `StrategyTester.run_simulation` over patched data.

These drive the whole pipeline — fetch → date/prop filter → calibration →
min-prob → live_replay slip building → payout → equity/drawdown/breakdown
assembly — with the DB and calibration step patched out so the test
controls exactly which legs exist. They pin the response *shape* the
frontend (`sbMapResult`) consumes and the headline *math* on a couple of
hand-computable scenarios.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tests.engine_tests._sandbox_fixtures import (
    obs_row, run_replay_simulation, StrategyConfig,
)

UTC = timezone.utc


def _one_tick_winning_slip():
    """3 legs, one scrape tick, 3 distinct teams, all hit at p=0.65."""
    t0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    game = datetime(2026, 5, 1, 19, 0, tzinfo=UTC)
    return [
        obs_row(player=f"P{i}", team=f"T{i}",
                first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                game_start=game, true_prob=0.65, result="hit")
        for i in range(3)
    ]


class RunSimulationHappyPathTests(unittest.TestCase):
    def setUp(self):
        self.cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.578, slip_size=3, slip_type="power",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=False,
        )

    def test_single_winning_slip_math(self):
        res = run_replay_simulation(_one_tick_winning_slip(), self.cfg)
        self.assertNotIn("error", res, res.get("error"))
        s = res["summary"]
        self.assertEqual(s["total_slips"], 1)
        self.assertEqual(s["bet_slips"], 1)
        self.assertEqual(s["total_bet"], 1.0)
        self.assertEqual(s["total_profit"], 5.0)   # 1 * 6 - 1
        self.assertEqual(s["roi_pct"], 500.0)
        self.assertEqual(s["win_rate_pct"], 100.0)
        self.assertEqual(s["max_drawdown_pct"], 0.0)
        self.assertEqual(s["bankroll"], 100.0)

    def test_response_carries_every_section_the_ui_reads(self):
        res = run_replay_simulation(_one_tick_winning_slip(), self.cfg)
        for key in ("summary", "equity_curve", "drawdown_curve", "rolling",
                    "monthly", "breakdowns", "funnel", "slips"):
            self.assertIn(key, res, f"missing top-level key {key!r}")
        # Equity curve carries CUMULATIVE PROFIT (sbMapResult adds the
        # starting bankroll on the client).
        self.assertEqual(len(res["equity_curve"]), 1)
        self.assertEqual(res["equity_curve"][0]["y"], 5.0)
        # One slip with the expected leg/hit counts.
        self.assertEqual(len(res["slips"]), 1)
        self.assertEqual(res["slips"][0]["n_legs"], 3)
        self.assertEqual(res["slips"][0]["hits"], 3)

    def test_bootstrap_off_yields_empty_ci(self):
        res = run_replay_simulation(_one_tick_winning_slip(), self.cfg)
        self.assertEqual(res["summary"]["ci"], {})

    def test_bootstrap_on_attaches_ci_when_enough_bet_slips(self):
        # Need >=2 bet slips for a non-degenerate CI. Two non-overlapping
        # ticks → two slips.
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=UTC)
        rows = []
        for tick in range(2):
            start = t0 + timedelta(minutes=10 * tick)
            for i in range(3):
                rows.append(obs_row(
                    player=f"P{tick}_{i}", team=f"T{i}",
                    first_seen=start, last_seen=start + timedelta(minutes=5),
                    game_start=game, true_prob=0.65, result="hit"))
        cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.578, slip_size=3, slip_type="power",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=True,
        )
        res = run_replay_simulation(rows, cfg)
        self.assertEqual(res["summary"]["total_slips"], 2)
        self.assertIn("roi_pct", res["summary"]["ci"])


class RunSimulationDrawdownTests(unittest.TestCase):
    """A win followed by a loss must produce a non-zero drawdown and a
    50% win rate — the full equity-rebuild path."""

    def test_win_then_loss(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=UTC)
        rows = []
        # Tick 1: all hit (+5). Tick 2 (10 min later, non-overlapping
        # window, fresh players): all miss (-1).
        for i in range(3):
            rows.append(obs_row(player=f"W{i}", team=f"T{i}",
                                 first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                                 game_start=game, true_prob=0.65, result="hit"))
        t1 = t0 + timedelta(minutes=10)
        for i in range(3):
            rows.append(obs_row(player=f"L{i}", team=f"T{i}",
                                 first_seen=t1, last_seen=t1 + timedelta(minutes=5),
                                 game_start=game, true_prob=0.65, result="miss"))
        cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.578, slip_size=3, slip_type="power",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=False,
        )
        res = run_replay_simulation(rows, cfg)
        s = res["summary"]
        self.assertEqual(s["total_slips"], 2)
        self.assertEqual(s["total_profit"], 4.0)   # +5 -1
        self.assertEqual(s["roi_pct"], 200.0)       # 4 / 2
        self.assertEqual(s["win_rate_pct"], 50.0)
        # Bank: 100 → 105 (peak) → 104. dd = (105-104)/105*100 ≈ 0.95%.
        self.assertGreater(s["max_drawdown_pct"], 0.0)
        self.assertAlmostEqual(s["max_drawdown_pct"], 0.95, places=2)
        self.assertEqual([p["y"] for p in res["equity_curve"]], [5.0, 4.0])


class RunSimulationAllSlipsTests(unittest.TestCase):
    """The sandbox must surface EVERY disjoint valid slip in the window,
    not just the single best one per scrape tick. A pool of 2*slip_size
    co-available qualifying legs at one tick yields two slips."""

    def _pool_at_one_tick(self, n_legs):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=UTC)
        # Distinct players AND distinct teams so dedup / two-team rules
        # never block a leg — the only limit is the partition into slips.
        return [
            obs_row(player=f"P{i}", team=f"T{i}",
                    first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                    game_start=game, true_prob=0.65, result="hit")
            for i in range(n_legs)
        ]

    def _cfg(self, **kw):
        base = dict(
            leagues=["NBA"], min_prob=0.578, slip_size=3, slip_type="power",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=False,
        )
        base.update(kw)
        return StrategyConfig(**base)

    def test_six_legs_one_tick_yields_two_three_leg_slips(self):
        res = run_replay_simulation(self._pool_at_one_tick(6), self._cfg())
        self.assertNotIn("error", res, res.get("error"))
        self.assertEqual(res["summary"]["total_slips"], 2,
            "6 co-available legs / 3 per slip must form 2 disjoint slips")
        # Both win (all hit) → profit 5 each = 10 total over 2u staked.
        self.assertEqual(res["summary"]["total_profit"], 10.0)
        self.assertEqual(res["summary"]["roi_pct"], 500.0)

    def test_no_leg_is_reused_across_slips_in_a_tick(self):
        res = run_replay_simulation(self._pool_at_one_tick(6), self._cfg())
        players = [leg["player"] for s in res["slips"] for leg in s["legs"]]
        self.assertEqual(len(players), len(set(players)),
            "a leg must not appear in two slips at the same tick")

    def test_leftover_legs_below_one_slip_are_dropped(self):
        # 7 legs / 3 per slip → 2 full slips, 1 leftover (can't fill a 3rd).
        res = run_replay_simulation(self._pool_at_one_tick(7), self._cfg())
        self.assertEqual(res["summary"]["total_slips"], 2)

    def test_analytics_aggregate_over_all_slips(self):
        # Breakdowns / equity must reflect every emitted slip, not one.
        res = run_replay_simulation(self._pool_at_one_tick(6), self._cfg())
        self.assertEqual(len(res["equity_curve"]), 2)
        nba = next(r for r in res["breakdowns"]["by_league"]
                   if r["league"] == "NBA")
        self.assertEqual(nba["slips"], 2)
        self.assertEqual(res["funnel"]["ticks_with_slip"], 1)
        self.assertEqual(res["funnel"]["slips_formed"], 2)


class RunSimulationRejectionTests(unittest.TestCase):
    def test_two_leg_flex_is_rejected_end_to_end(self):
        # No BREAK_EVEN[("2","flex")] entry → every candidate slip is
        # rejected → the run returns an actionable error, not a payload.
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=UTC)
        rows = [
            obs_row(player=f"P{i}", team=f"T{i}",
                    first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                    game_start=game, true_prob=0.70, result="hit")
            for i in range(2)
        ]
        cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.0, slip_size=2, slip_type="flex",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=False,
        )
        res = run_replay_simulation(rows, cfg)
        self.assertIn("error", res)

    def test_min_prob_above_all_legs_returns_filter_error(self):
        cfg = StrategyConfig(
            leagues=["NBA"], min_prob=0.95, slip_size=3, slip_type="power",
            bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
            bootstrap=False,
        )
        res = run_replay_simulation(_one_tick_winning_slip(), cfg)
        self.assertIn("error", res)
        self.assertIn("Min True Prob", res["error"])


class RunSimulationFunnelTests(unittest.TestCase):
    def test_funnel_counts_ticks_and_formed_slips(self):
        res = run_replay_simulation(
            _one_tick_winning_slip(),
            StrategyConfig(
                leagues=["NBA"], min_prob=0.578, slip_size=3, slip_type="power",
                bet_size=1.0, use_kelly=False, slip_strategy="live_replay",
                bootstrap=False,
            ),
        )
        f = res["funnel"]
        self.assertEqual(f["rows_pulled"], 3)
        self.assertEqual(f["distinct_scrape_ticks"], 1)
        self.assertEqual(f["ticks_with_slip"], 1)
        self.assertEqual(f["slips_formed"], 1)


class RunSimulationNoDbTests(unittest.TestCase):
    def test_no_db_returns_error(self):
        from tests.engine_tests._sandbox_fixtures import make_tester
        res = make_tester(db=None).run_simulation(StrategyConfig(leagues=["NBA"]))
        self.assertEqual(res, {"error": "Database not connected"})


if __name__ == "__main__":
    unittest.main()

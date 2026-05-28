"""Tests for `StrategyTester._replay_live_auto_builder`.

The contract: at every distinct scrape tick (first_seen_at floored to the
minute), the replay must produce the slip that engine/backtest.BacktestLogger
would have produced given the legs that were live on the board at that
moment, plus the dedup state accumulated from earlier-tick replay slips.

Key invariants validated here:
  * Only legs whose [first_seen, last_seen] window contains the tick are
    considered. A leg that disappears before another arrives can't end
    up in the same simulated slip.
  * Cross-tick dedup: the same (player, game_date) can't appear in two
    replay slips within 48h. After the window decays it becomes
    eligible again.
  * Two-distinct-teams rule fires even when EV ranking would prefer a
    same-team stack.
  * The break-even probability gate rejects slips whose average true_prob
    falls below BREAK_EVEN[(n_legs, slip_type)].
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from engine.strategy_tester import StrategyTester, StrategyConfig


def _row(
    *,
    player: str,
    team: str,
    league: str = "NBA",
    prop: str = "Points",
    line: float = 20.5,
    side: str = "over",
    true_prob: float = 0.65,
    first_seen: datetime,
    last_seen: datetime,
    game_start: datetime,
    result: str = "hit",
) -> dict:
    """Build one observatory-shaped row. `calibrated_prob` is set equal to
    true_prob because the replay path consumes already-calibrated values
    (the upstream `_apply_current_calibration` runs before it)."""
    return {
        "player":           player,
        "team":             team,
        "league":           league,
        "prop":             prop,
        "line":             line,
        "side":             side,
        "true_prob":        true_prob,
        "raw_true_prob":    true_prob,
        "calibrated_prob":  true_prob,
        "game_start":       game_start.isoformat(),
        "game_start_dt":    pd.Timestamp(game_start),
        "first_seen_at":    first_seen.isoformat(),
        "last_seen_at":     last_seen.isoformat(),
        "result":           result,
        "market_width":     0.02,
    }


def _tester() -> StrategyTester:
    """Construct a tester without touching the database. We bypass __init__
    because it tries to load isotonic calibration from disk."""
    t = StrategyTester.__new__(StrategyTester)
    t.db = None
    t._curves = {}
    return t


class ReplayLivePoolTests(unittest.TestCase):
    """The most-emphatic guarantee: legs whose availability windows don't
    overlap can't both end up in the same simulated slip."""

    def test_non_overlapping_legs_never_share_slip(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)

        # Two waves: legs A,B,C visible 12:00-12:05, legs D,E,F visible
        # 12:10-12:15. No instant ever has all six on the board. A 3-leg
        # slip can form from each wave but no slip should ever mix waves.
        wave1 = [
            _row(player=f"P{i}", team=f"T{i}",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                 game_start=game, true_prob=0.65)
            for i in range(3)
        ]
        wave2 = [
            _row(player=f"Q{i}", team=f"U{i}",
                 first_seen=t0 + timedelta(minutes=10),
                 last_seen=t0 + timedelta(minutes=15),
                 game_start=game, true_prob=0.65)
            for i in range(3)
        ]
        df = pd.DataFrame(wave1 + wave2)

        cfg = StrategyConfig(
            slip_size=3, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _funnel = _tester()._replay_live_auto_builder(df, cfg)

        # Every slip's leg players must come exclusively from one wave.
        wave1_players = {f"P{i}" for i in range(3)}
        wave2_players = {f"Q{i}" for i in range(3)}
        for s in slips:
            players = {leg["player"] for leg in s["legs"]}
            mixed = (players & wave1_players) and (players & wave2_players)
            self.assertFalse(
                mixed,
                f"Slip mixed legs from two non-overlapping waves: {players}",
            )

    def test_overlapping_legs_can_form_slip(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        rows = [
            _row(player=f"P{i}", team=f"T{i}",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=30),
                 game_start=game, true_prob=0.70)
            for i in range(3)
        ]
        df = pd.DataFrame(rows)
        cfg = StrategyConfig(
            slip_size=3, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _funnel = _tester()._replay_live_auto_builder(df, cfg)
        self.assertGreaterEqual(len(slips), 1)
        self.assertEqual(len(slips[0]["legs"]), 3)


class ReplayDedupTests(unittest.TestCase):
    """The 48 h rolling dedup window: the same (player, sports_day) can
    appear in two replay slips only if the second tick is > 48 h after
    the first."""

    def test_same_player_skipped_within_48h(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        # First wave at t0: 3 legs, one of them is "Star".
        first = [
            _row(player="Star", team="T0",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=3),
                 game_start=game, true_prob=0.80),
            _row(player="P1", team="T1",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=3),
                 game_start=game, true_prob=0.70),
            _row(player="P2", team="T2",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=3),
                 game_start=game, true_prob=0.70),
        ]
        # Second wave 10 min later — Star reappears but it's still the
        # same (player, sports_day) within the 48 h window, so it must
        # be excluded from the second slip. Wave includes 3 fresh legs
        # so a valid 3-leg slip can still form after Star is dropped.
        t1 = t0 + timedelta(minutes=10)
        second = [
            _row(player="Star", team="T0",
                 first_seen=t1, last_seen=t1 + timedelta(minutes=3),
                 game_start=game, true_prob=0.80),
            _row(player="Q1", team="U1",
                 first_seen=t1, last_seen=t1 + timedelta(minutes=3),
                 game_start=game, true_prob=0.70),
            _row(player="Q2", team="U2",
                 first_seen=t1, last_seen=t1 + timedelta(minutes=3),
                 game_start=game, true_prob=0.70),
            _row(player="Q3", team="U3",
                 first_seen=t1, last_seen=t1 + timedelta(minutes=3),
                 game_start=game, true_prob=0.70),
        ]
        df = pd.DataFrame(first + second)
        cfg = StrategyConfig(
            slip_size=3, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _ = _tester()._replay_live_auto_builder(df, cfg)
        self.assertEqual(len(slips), 2)
        # Star appears in slip 1 only.
        slip0_players = {l["player"] for l in slips[0]["legs"]}
        slip1_players = {l["player"] for l in slips[1]["legs"]}
        self.assertIn("Star", slip0_players)
        self.assertNotIn("Star", slip1_players)


class ReplayTwoTeamTests(unittest.TestCase):
    """PrizePicks rule: ≥2 distinct teams per slip. EV ranking might prefer
    a same-team stack; replay must swap in a different-team leg or reject."""

    def test_single_team_pool_produces_no_slip(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        # 3 legs all on team "LAL" — no second-team candidate available.
        rows = [
            _row(player=f"P{i}", team="LAL",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                 game_start=game, true_prob=0.70)
            for i in range(3)
        ]
        df = pd.DataFrame(rows)
        cfg = StrategyConfig(
            slip_size=3, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _ = _tester()._replay_live_auto_builder(df, cfg)
        self.assertEqual(slips, [])


class ReplayBalancedPartitionTests(unittest.TestCase):
    """Regression for the "only one slip" bug. The break-even gate is on the
    slip *average*, so greedily stacking the best legs makes the first slip
    clear BE while every later slip falls below it — the user saw 1 slip.
    The builder must instead spread strong and marginal legs across slips so
    more of them clear break-even."""

    def test_marginal_pool_yields_multiple_balanced_slips(self):
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        # 2-Power BE = 57.74%. Four co-available legs, all distinct teams:
        #   greedy:   [.65,.62]=.635 ✓ then [.58,.56]=.570 ✗ → 1 slip.
        #   balanced: [.65,.58]=.615 ✓ and [.62,.56]=.590 ✓  → 2 slips.
        probs = [0.65, 0.62, 0.58, 0.56]
        rows = [
            _row(player=f"P{i}", team=f"T{i}",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                 game_start=game, true_prob=p)
            for i, p in enumerate(probs)
        ]
        df = pd.DataFrame(rows)
        cfg = StrategyConfig(
            slip_size=2, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _ = _tester()._replay_live_auto_builder(df, cfg)
        self.assertEqual(len(slips), 2,
            "balanced partition must lift the marginal pair over break-even")
        # No leg is reused across the two slips.
        players = [l["player"] for s in slips for l in s["legs"]]
        self.assertEqual(len(players), len(set(players)))


class ReplayTickFloorTests(unittest.TestCase):
    """Regression: the replay floors each tick to the minute, then asked
    `first_seen <= tick <= last_seen`. A row whose first_seen had any
    non-zero seconds (12:00:23) failed the comparison against its own
    floored tick (12:00:00), leaving every tick with an empty live pool
    and the user staring at "Could not form any N-leg slips from
    history" no matter how much data was available."""

    def test_rows_with_nonzero_seconds_are_still_available_at_their_tick(self):
        # Pick a timestamp deliberately offset by some seconds — this is
        # the realistic shape of Postgres NOW() timestamps stamped at
        # scrape time (microsecond-precision, never aligned to a minute).
        t0 = datetime(2026, 5, 22, 12, 0, 23, tzinfo=timezone.utc)
        game = datetime(2026, 5, 22, 19, 0, tzinfo=timezone.utc)
        rows = [
            _row(player=f"P{i}", team=f"T{i}",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=2),
                 game_start=game, true_prob=0.65)
            for i in range(3)
        ]
        df = pd.DataFrame(rows)
        cfg = StrategyConfig(
            slip_size=2, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, funnel = _tester()._replay_live_auto_builder(df, cfg)
        self.assertGreaterEqual(
            len(slips), 1,
            f"Replay produced 0 slips despite 3 live legs at tick — "
            f"funnel: {funnel}",
        )
        self.assertEqual(
            funnel["ticks_without_pool"], 0,
            f"Every tick should have had legs available; funnel: {funnel}",
        )


class ReplayBreakEvenTests(unittest.TestCase):
    """The BREAK_EVEN gate rejects slips whose avg true_prob falls below the
    payout-implied break-even, even if every leg passed the EV-floor."""

    def test_below_break_even_average_rejected(self):
        from engine.constants import BREAK_EVEN
        t0 = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
        game = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        be = BREAK_EVEN[("3", "power")]
        # Set average true_prob comfortably below break-even.
        low = max(0.40, be - 0.05)
        rows = [
            _row(player=f"P{i}", team=f"T{i}",
                 first_seen=t0, last_seen=t0 + timedelta(minutes=5),
                 game_start=game, true_prob=low)
            for i in range(3)
        ]
        df = pd.DataFrame(rows)
        cfg = StrategyConfig(
            slip_size=3, slip_type="power", bet_size=1.0,
            slip_strategy="live_replay", min_prob=0.0,
        )
        slips, _ = _tester()._replay_live_auto_builder(df, cfg)
        self.assertEqual(slips, [])


class ReplayErrorDiagnosticsTests(unittest.TestCase):
    """Regression: the "Could not form any 2-leg slips from history" error
    was a one-liner with no diagnosis, even when the funnel made the cause
    obvious. In production the dominant failure mode is the BREAK_EVEN
    gate — user picks min_prob=54.1% expecting slips, but 2-Power
    requires avg prob >= 57.74%, so every formed slip is silently
    rejected. The error now names the gate and the threshold to raise to."""

    def _run_at_prob(self, true_prob, min_prob, slip_size=2, slip_type="power"):
        import os
        from unittest.mock import patch
        import pandas as pd
        from engine.strategy_tester import StrategyConfig, StrategyTester
        os.environ.setdefault("SUPABASE_URL", "http://localhost")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test")
        os.environ.setdefault("SUPABASE_ANON_KEY", "test")
        t0 = datetime(2026, 5, 22, 12, 0, 23, tzinfo=timezone.utc)
        game = datetime(2026, 5, 22, 19, 0, tzinfo=timezone.utc)
        rows = []
        # 40 ticks × 6 legs each, all at the same true_prob so we can
        # control whether the BE gate fires.
        for i in range(40):
            t = t0 + timedelta(hours=i)
            for j in range(6):
                rows.append({
                    "result": "hit" if (i + j) % 3 != 2 else "miss",
                    "prop": "Points", "line": 20.5,
                    "true_prob": true_prob, "raw_true_prob": true_prob,
                    "side": "over" if j % 2 == 0 else "under",
                    "game_start": game.isoformat(),
                    "game_start_dt": pd.Timestamp(game),
                    "league": "NBA", "player": f"P{i}_{j}",
                    "market_width": 4.0, "team": f"T{j % 4}",
                    "first_seen_at": t.isoformat(),
                    "last_seen_at":  (t + timedelta(minutes=3)).isoformat(),
                })
        df = pd.DataFrame(rows)
        tester = StrategyTester.__new__(StrategyTester)
        tester.db = object()
        tester._curves = {}
        with patch.object(StrategyTester, "_fetch_resolved_observatory",
                          return_value=df), \
             patch.object(tester, "_apply_current_calibration",
                          side_effect=lambda d: d.assign(
                              calibrated_prob=pd.to_numeric(d["true_prob"])
                          )):
            return tester.run_simulation(StrategyConfig(
                leagues=["NBA"], min_prob=min_prob,
                slip_size=slip_size, slip_type=slip_type,
                bet_size=1.0, use_kelly=False,
                slip_strategy="live_replay", bootstrap=False,
            ))

    def test_below_break_even_error_names_the_gate(self):
        # min_prob 54.1% but 2-Power BE = 57.74%; legs at calibrated 0.55
        # pass the filter, every formed slip's avg = 0.55 < 0.5774 → all rejected.
        res = self._run_at_prob(true_prob=0.55, min_prob=0.541)
        self.assertIn("error", res)
        err = res["error"]
        self.assertIn("57.74%", err,
            f"Error must name the actual break-even threshold; got: {err}")
        self.assertIn("Raise Min True Prob", err,
            f"Error must suggest the user action; got: {err}")
        self.assertIn("rejected", err.lower(),
            f"Error must say slips were rejected (not just absent); got: {err}")

    def test_at_break_even_with_qualifying_legs_recovers(self):
        # Legs at 0.60 > BE 0.5774 → slips should actually form.
        res = self._run_at_prob(true_prob=0.60, min_prob=0.578)
        self.assertNotIn("error", res,
            f"Expected slips at min_prob=0.578 with 0.60 legs; got: {res.get('error')}")
        self.assertGreater(res["summary"]["total_slips"], 0)

    def test_min_prob_above_data_returns_filter_error(self):
        # All legs at 0.55 but min_prob 0.60 → after_min_prob = 0
        res = self._run_at_prob(true_prob=0.55, min_prob=0.60)
        self.assertIn("error", res)
        self.assertIn("Min True Prob", res["error"])
        self.assertIn("lower", res["error"].lower())


if __name__ == "__main__":
    unittest.main()

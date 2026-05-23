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


if __name__ == "__main__":
    unittest.main()

"""Shared builders for the sandbox test suite.

The sandbox tests all need the same two things: observatory-shaped rows
(the dicts `StrategyTester` consumes after calibration) and a tester
instance that doesn't touch the database or load calibration from disk.
Centralizing them here keeps each test file focused on the behavior it
pins rather than on plumbing, and means a change to the row shape only
has to be made in one place.

This module name starts with `_` so pytest doesn't collect it as a test
module. Import it as:

    from tests.engine_tests._sandbox_fixtures import obs_row, make_tester
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from engine.strategy_tester import StrategyTester, StrategyConfig


def obs_row(
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
    """One observatory-shaped row, post-calibration.

    `calibrated_prob` is set equal to `true_prob` because every code path
    under test runs after `_apply_current_calibration` (we patch that out
    in the end-to-end tests, so the rows must already carry the calibrated
    value)."""
    return {
        "player":          player,
        "team":            team,
        "league":          league,
        "prop":            prop,
        "line":            line,
        "side":            side,
        "true_prob":       true_prob,
        "raw_true_prob":   true_prob,
        "calibrated_prob": true_prob,
        "game_start":      game_start.isoformat(),
        "game_start_dt":   pd.Timestamp(game_start),
        "first_seen_at":   first_seen.isoformat(),
        "last_seen_at":    last_seen.isoformat(),
        "result":          result,
        "market_width":    0.02,
    }


def make_tester(*, db=None) -> StrategyTester:
    """A tester with no DB and no on-disk calibration.

    Bypasses `__init__` (which calls `get_db()` and reads the isotonic
    calibration JSON). Pass `db=object()` when the code under test only
    checks `if not self.db` as a gate — the truthy sentinel is enough."""
    t = StrategyTester.__new__(StrategyTester)
    t.db = db
    t._curves = {}
    return t


def sim_slip(
    *,
    timestamp: str,
    league: str = "NBA",
    hits: int = 3,
    n_eff: int = 3,
    n_legs: int = 3,
    n_pushed: int = 0,
    payout: float = 6.0,
    bet_size: float = 1.0,
    profit: float | None = None,
    legs: list[dict] | None = None,
) -> dict:
    """One entry in the `sim_slips` list that `run_simulation` builds and
    `_build_breakdowns` / `_monthly_buckets` / `_bootstrap_metrics`
    consume. `profit` defaults to payout - bet_size so the row is
    internally consistent unless a test overrides it."""
    if profit is None:
        profit = payout - bet_size
    return {
        "timestamp": timestamp,
        "league":    league,
        "hits":      hits,
        "n_eff":     n_eff,
        "n_legs":    n_legs,
        "n_pushed":  n_pushed,
        "payout":    payout,
        "bet_size":  bet_size,
        "profit":    profit,
        "legs":      legs if legs is not None else [
            {"player": f"P{i}", "prop": "Points", "result": "hit"}
            for i in range(n_legs)
        ],
    }


def run_replay_simulation(rows: list[dict], config: StrategyConfig) -> dict:
    """Drive `run_simulation` end-to-end over `rows` with the DB and
    calibration step patched out. `rows` are already-calibrated
    observatory dicts (see `obs_row`); calibration is a pass-through that
    copies `true_prob` into `calibrated_prob`."""
    df = pd.DataFrame(rows)
    tester = make_tester(db=object())
    with patch.object(
        StrategyTester, "_fetch_resolved_observatory", return_value=df,
    ), patch.object(
        tester, "_apply_current_calibration",
        side_effect=lambda d: d.assign(
            calibrated_prob=pd.to_numeric(d["true_prob"]),
        ),
    ):
        return tester.run_simulation(config)


# Re-export so test files can `from _sandbox_fixtures import StrategyConfig`.
__all__ = [
    "StrategyConfig", "StrategyTester",
    "obs_row", "make_tester", "sim_slip", "run_replay_simulation",
    "datetime", "timezone",
]

"""Pin the isotonic recalibration map (engine.calibration_map) and its wiring
into BetResult.

Invariants under test:
  * PAVA produces a monotone non-decreasing fit and pools violators into
    their weighted mean.
  * A 2x-overconfident raw signal is bent back toward realized: raw 0.90 on a
    (true = 0.5 + 0.5*(raw-0.5)) generator calibrates near 0.70.
  * apply_calibration returns None when there is no trusted fit, so callers
    fall through to the SIDE_BIAS path.
  * BetResult wiring:
      - flag OFF  -> identical to the SIDE_BIAS-only behavior (one ruler),
      - flag ON, no trusted fit -> SIDE_BIAS fallback,
      - flag ON, trusted cell fit -> map REPLACES SIDE_BIAS (no double-correct),
      - raw_true_prob is never mutated on any path,
      - green devils are exempt.
"""
from __future__ import annotations

import random

import pytest

import config as cfg
import engine.calibration_map as cm
from engine.calibration_map import _pava, _fit_cell, _interp, apply_calibration
from engine.ev_calculator import BetResult


@pytest.fixture(autouse=True)
def _restore_state():
    """Snapshot + restore the module map and the config flags each test mutates
    so tests can't leak state into the rest of the suite."""
    saved_map = cm._map
    saved_flag = cfg.CALIBRATION_MAP_ENABLED
    saved_bias = cfg.SIDE_BIAS_ENABLED
    yield
    cm._map = saved_map
    cfg.CALIBRATION_MAP_ENABLED = saved_flag
    cfg.SIDE_BIAS_ENABLED = saved_bias


def _bet(prob, side="under", league="MLB", odds_type="standard"):
    return BetResult(
        bet_id="x", player_name="P", league=league, prop_type="K",
        pp_line=5.5, fd_line=5.5, side=side, true_prob=prob,
        over_odds=-110, under_odds=-110, both_sided=True,
        pp_player_id="1", odds_type=odds_type,
    )


def test_pava_monotone_and_pools():
    out = _pava([0.6, 0.2, 0.3, 0.9], [10, 10, 10, 10])
    assert out == sorted(out)                 # non-decreasing
    # the first three (0.6, 0.2, 0.3) pool to their mean 0.3667
    assert out[0] == pytest.approx(0.3667, abs=1e-3)
    assert out[-1] == pytest.approx(0.9)


def test_fit_bends_overconfident_signal_back():
    random.seed(7)
    pairs = []
    for _ in range(6000):
        raw = random.uniform(0.05, 0.95)
        true = 0.5 + 0.5 * (raw - 0.5)       # model claims 2x its real edge
        pairs.append((raw, 1 if random.random() < true else 0))
    fit = _fit_cell(pairs)
    assert fit["trusted"]
    cal = _interp(fit["knots"], 0.90)
    # target 0.70; calibrated must be much closer to 0.70 than raw 0.90 is
    assert abs(cal - 0.70) < abs(0.90 - 0.70)
    assert cal == pytest.approx(0.70, abs=0.05)


def test_apply_none_without_trusted_fit():
    cm._map = {}
    assert apply_calibration(0.9, "NBA", "over") is None
    # untrusted cell also yields None
    cm._map = {"cells": {"NBA|over": {"trusted": False, "knots": [[0.5, 0.5]]}},
               "global": None}
    assert apply_calibration(0.9, "NBA", "over") is None


def test_betresult_flag_off_matches_side_bias():
    cfg.CALIBRATION_MAP_ENABLED = False
    cfg.SIDE_BIAS_ENABLED = True
    b = _bet(0.62, side="under", league="MLB")   # MLB under bias = +0.025
    assert b.true_prob == pytest.approx(0.645, abs=1e-9)
    assert b.raw_true_prob == pytest.approx(0.62)


def test_betresult_flag_on_no_fit_falls_back_to_side_bias():
    cfg.CALIBRATION_MAP_ENABLED = True
    cfg.SIDE_BIAS_ENABLED = True
    cm._map = {}
    b = _bet(0.62, side="under", league="MLB")
    assert b.true_prob == pytest.approx(0.645, abs=1e-9)


def test_betresult_trusted_fit_replaces_side_bias():
    cfg.CALIBRATION_MAP_ENABLED = True
    cfg.SIDE_BIAS_ENABLED = True
    cm._map = {"cells": {"MLB|under": {"trusted": True, "n": 9999,
               "knots": [[0.5, 0.5], [0.62, 0.58], [0.7, 0.63]]}},
               "global": None}
    b = _bet(0.62, side="under", league="MLB")
    # 0.58 from the curve, NOT 0.645 (SIDE_BIAS) — no double-correction
    assert b.true_prob == pytest.approx(0.58, abs=1e-9)
    assert b.raw_true_prob == pytest.approx(0.62)


def test_green_devil_exempt_from_map():
    cfg.CALIBRATION_MAP_ENABLED = True
    cm._map = {"cells": {"MLB|under": {"trusted": True, "n": 9999,
               "knots": [[0.5, 0.5], [0.62, 0.58]]}}, "global": None}
    b = _bet(0.62, side="under", league="MLB", odds_type="goblin")
    assert b.true_prob == pytest.approx(0.62)    # untouched

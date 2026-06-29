"""
Pure functions for building market_observatory entries.

Revived for the CLV dataset (migration_016). simplify-v1 (commit 71c1091)
removed the observatory logger; this is the slimmed-down, calibration-free
version that matches the current architecture — `true_prob` is just the raw
consensus (there is no isotonic calibrator on this branch to apply).

Logging every priced side (not only the +EV bets) is what gives an ML model
the *unbiased* training universe: the markets we skipped, with their outcomes,
are exactly what distinguishes a good selection from a bad one.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Hard floor on raw consensus probability. A side below this is NOT logged —
# its complement is >= 0.70 and IS logged, so the tail is captured on the
# complementary record. Logging both sides of every prop would double row
# volume without adding signal at the tails.
OBS_MIN_PROB: float = 0.30


def should_log_observation(raw_prob: Optional[float]) -> bool:
    """Single source of truth for the observatory threshold."""
    if raw_prob is None:
        return False
    try:
        return float(raw_prob) >= OBS_MIN_PROB
    except (TypeError, ValueError):
        return False


def build_observation_entry(
    *,
    player_name: str,
    league: str,
    prop: str,
    line: Any,
    side: str,
    raw_prob: Optional[float],
    market_width: Optional[float],
    team: str = "",
    start_time: str = "",
    books_probs: Optional[dict] = None,
) -> Optional[dict]:
    """Build the canonical observatory entry dict — or None if the raw
    probability fails the >= OBS_MIN_PROB gate.

    The fields mirror what `_log_observatory_bg` (web/app.py) upserts into
    `market_observatory`. simplify-v1 has no calibrator, so `true_prob` equals
    the raw consensus; both are stored so the schema contract is unchanged.
    """
    if not should_log_observation(raw_prob):
        return None
    raw = float(raw_prob)
    return {
        "player_name":   player_name,
        "league":        league,
        "prop_type":     prop,
        "pp_line":       line,
        "side":          side,
        "true_prob":     raw,      # no calibration on simplify-v1
        "raw_true_prob": raw,
        "market_width":  market_width,
        "team":          team or "",
        "start_time":    start_time or "",
        "books_probs":   books_probs or {},
    }

"""
Pure functions for building observatory entries.

The pipeline match loop in `web/app.py` historically built every
observatory entry inside a closure. That made the (player, side,
raw_prob, market_width, calibrated_prob) shape impossible to test
without running the whole scrape. Lifting it here gives us:

  • a single canonical place where the >= 0.30 threshold is enforced
    (so future changes can't accidentally weaken the rule for one
    side and leave the other intact)
  • a pure function that's trivially testable against synthetic
    MatchedProp objects, so the contract "every priced side with raw
    consensus prob >= 0.30 produces an observation" is verifiable
  • a single place that handles missing calibration curves gracefully
    (previously `_observe` swallowed every exception → invisible
    silent fallbacks)
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


# The hard floor on raw consensus probability. Any side with raw_prob
# strictly below this is NOT logged to the observatory.
#
# Why 0.30: the complement side is then >= 0.70 and IS logged, so
# extreme-longshot data is captured under the complementary record.
# Logging both sides of every prop doubles the row volume without
# adding statistical signal at the tails, which dilutes the per-bucket
# recency-weighted aggregation. 0.30 is the lowest the calibrator
# benefits from across all current props × leagues.
OBS_MIN_PROB: float = 0.30


def should_log_observation(raw_prob: Optional[float]) -> bool:
    """Single source of truth for the observatory threshold. Callers
    that need to skip the rest of the entry-construction work check
    this first."""
    if raw_prob is None:
        return False
    try:
        return float(raw_prob) >= OBS_MIN_PROB
    except (TypeError, ValueError):
        return False


def calibrate_for_observation(
    league: str, prop: str, side: str, raw_prob: float,
) -> float:
    """Apply the live isotonic calibration to a raw probability, mirroring
    what BetResult would produce on the bet path. Falls back to the raw
    probability when calibration isn't loaded (test paths, fresh
    deploys) so the observatory row always has a sane true_prob.

    Defined here (not on _observe) so a future caller — e.g. a
    sandbox replay — can use the same conversion."""
    try:
        from engine.ev_calculator import _apply_calibration as _ac
        from engine.ev_calculator import _calibration_curves as _cc
        # Mirror the [0.001, 0.999] window applied by BetResult so the
        # observatory's true_prob can't drift outside the range the bet
        # path would have stored — same downstream log-loss math sees
        # both surfaces.
        return max(0.001, min(0.999, _ac(_cc, league, prop, side, raw_prob)))
    except Exception:
        return raw_prob


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
    """Build the canonical observatory entry dict — or None if the
    raw probability fails the >= 0.30 gate.

    The fields and shape mirror what `_log_observatory_bg` upserts
    into `market_observatory`. Anything that wants to enqueue an
    observation should funnel through this function so the schema
    contract is in one place.
    """
    if not should_log_observation(raw_prob):
        return None
    calibrated = calibrate_for_observation(league, prop, side, float(raw_prob))
    return {
        "player_name":   player_name,
        "league":        league,
        "prop_type":     prop,
        "pp_line":       line,
        "side":          side,
        "true_prob":     calibrated,
        "raw_true_prob": float(raw_prob),
        "market_width":  market_width,
        "team":          team or "",
        "start_time":    start_time or "",
        "books_probs":   books_probs or {},
    }


def collect_match_observations(matches, *, get_side_prob, get_side_mw,
                               get_side_books_probs) -> list[dict]:
    """Walk a list of MatchedProp objects and collect every observable
    side. Each callable is supplied by the pipeline — abstracted so
    the actual prob/MW/books extraction (which depends on per-book
    half-step equivalents) doesn't have to live in this module.

    Returns the entries in match order. The dedup invariant is NOT
    enforced here — observatory writes are upserts with
    on_conflict="market_key" and ignore_duplicates=True, so duplicate
    rows are a no-op at the DB.
    """
    out: list[dict] = []
    for m in matches:
        pp = getattr(m, "pp", None)
        if pp is None:
            continue
        league = (getattr(pp, "league", "") or "").upper()
        prop   = getattr(pp, "stat_type", "") or ""
        line   = getattr(pp, "line_score", None)
        start  = getattr(pp, "start_time", "") or ""
        team   = getattr(pp, "team", "") or ""
        player = getattr(pp, "player_name", "") or ""

        pp_side = getattr(pp, "side", "both") or "both"
        for side in ("over", "under"):
            if pp_side not in ("both", side):
                continue
            raw_prob = get_side_prob(m, side)
            if not should_log_observation(raw_prob):
                continue
            entry = build_observation_entry(
                player_name=player, league=league, prop=prop, line=line,
                side=side, raw_prob=raw_prob,
                market_width=get_side_mw(m, side),
                team=team, start_time=start,
                books_probs=get_side_books_probs(m, side),
            )
            if entry is not None:
                out.append(entry)
    return out

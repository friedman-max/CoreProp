"""
Tier routing for the auto-backtest worker.

Background — Phase 1A audit found that the current default `auto_slip_min_prob`
(0.5407, the Power-6 per-leg break-even) produces a logged-leg distribution
with a 50.3% hit rate — essentially chance — across the 38,788-row settled
market_observatory. Raising the threshold to 0.65 produces a 69% hit rate,
solidly +EV on every slip size, but at a volume cost (∼80 legs / 30d).

A single-threshold model forces a choice between volume and quality. A
tiered model preserves volume by routing legs to the slip-type-and-size
combination that has the right per-leg break-even for that leg's edge:

  Tier A   p ≥ 0.65, not halted, has signal      → eligible for ALL slip types
                                                   (incl. 3-Power BE=0.5503)
  Tier B   0.60 ≤ p < 0.65, not halted            → eligible only for 5/6-pick
                                                    (Power BE ≤ 0.5493,
                                                    Flex BE ≤ 0.5425)
  Tier C   0.55 ≤ p < 0.60                        → DISPLAY ONLY, never
                                                    auto-logged
  REJECT   p < 0.55 or halted                     → dropped

The thresholds (0.65 / 0.60 / 0.55) are derived from the live threshold sweep
in analysis/_deepdive — hit rates jump cleanly at those bands and align with
the slip-size break-even table in engine/constants.py BREAK_EVEN.

This module is the single source of truth for tier assignment. Both the
auto-backtest worker (web/app.py) and the Phase 3 strategy comparison
logger (analysis/strategy_compare.py) read from here so the live logger
and the simulator stay in lockstep.
"""
from __future__ import annotations

from typing import Iterable, Literal, Optional

Tier = Literal["A", "B", "C", "REJECT"]

# ─────────────────────────────────────────────────────────────────────────
# Tier thresholds. Anchored on the live threshold sweep — see
# Phase 1A deepdive output for the hit-rate / volume curve.
# ─────────────────────────────────────────────────────────────────────────

TIER_A_MIN_PROB: float = 0.65
TIER_B_MIN_PROB: float = 0.60
TIER_C_MIN_PROB: float = 0.55

# Tier C is display-only — never auto-logged. The auto-backtest worker
# rejects any leg below TIER_B_MIN_PROB; the UI "Combined Lines" tab can
# still show Tier C plays with a "not auto-bet" badge for human review.
AUTO_LOG_FLOOR: float = TIER_B_MIN_PROB

# Slip-type/size combos a tier may participate in. Tier A is unrestricted;
# Tier B is barred from 2/3/4-pick because 3-Power BE (0.5503) and 4-Power
# BE (0.5623) both exceed Tier B's lower bound — including a Tier B leg in
# a 3-Power slip can drag the slip average below the slip break-even even
# when every leg is "+EV" by the user's threshold.
#
# Format: tier → {(slip_type_lower, n_legs): bool eligible}
_TIER_ELIGIBILITY: dict[str, dict[tuple[str, int], bool]] = {
    "A": {
        ("power", 2): True, ("power", 3): True, ("power", 4): True,
        ("power", 5): True, ("power", 6): True,
        ("flex",  3): True, ("flex",  4): True, ("flex",  5): True,
        ("flex",  6): True,
    },
    "B": {
        # Power BE: 2→0.577, 3→0.550, 4→0.562, 5→0.549, 6→0.541.
        # Tier B floor = 0.60 ≥ all of 5-Power and 6-Power BE.
        ("power", 5): True, ("power", 6): True,
        # Flex BE: 3→0.577, 4→0.550, 5→0.543, 6→0.542.
        # Tier B floor = 0.60 ≥ all of 4/5/6-Flex BE.
        ("flex",  4): True, ("flex",  5): True, ("flex",  6): True,
    },
    # C is auto-log-rejected — never reached by the worker, kept here so
    # downstream consumers (logger / sandbox UI) can query intent symmetrically.
    "C": {},
    "REJECT": {},
}


# ─────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────

def tier_for_prob(true_prob: float | None, *, calibration_halted: bool = False) -> Tier:
    """Tier label for a single calibrated probability.

    Halted cells go to REJECT regardless of the probability — a high
    `true_prob` from a circuit-broken cell is exactly the high-confidence-
    but-wrong combination D1 identified as the worst-CLV bucket.
    """
    if calibration_halted:
        return "REJECT"
    if true_prob is None:
        return "REJECT"
    try:
        p = float(true_prob)
    except (TypeError, ValueError):
        return "REJECT"
    if p >= TIER_A_MIN_PROB:
        return "A"
    if p >= TIER_B_MIN_PROB:
        return "B"
    if p >= TIER_C_MIN_PROB:
        return "C"
    return "REJECT"


def tier_for_leg(leg: dict) -> Tier:
    """Tier label for a leg dict (BetResult.to_dict() shape)."""
    return tier_for_prob(
        leg.get("true_prob"),
        calibration_halted=bool(leg.get("calibration_halted")),
    )


def tier_eligible_for_slip(tier: Tier, slip_type: str, n_legs: int) -> bool:
    """Is `tier` allowed to participate in (slip_type, n_legs) auto-logs?"""
    if tier in ("C", "REJECT"):
        return False
    key = (slip_type.lower(), int(n_legs))
    return _TIER_ELIGIBILITY.get(tier, {}).get(key, False)


def filter_legs_for_slip(
    legs: Iterable[dict], slip_type: str, n_legs: int,
) -> list[dict]:
    """Filter a list of leg dicts to those eligible for (slip_type, n_legs).

    Returned list preserves input order; callers (auto-backtest worker, the
    Phase 3 logger) re-sort downstream.
    """
    out: list[dict] = []
    for leg in legs:
        t = tier_for_leg(leg)
        if tier_eligible_for_slip(t, slip_type, n_legs):
            out.append(leg)
    return out


def tier_summary(legs: Iterable[dict]) -> dict[str, int]:
    """Diagnostic: count legs per tier. Used by the auto-backtest worker's
    log line and by the strategy comparison logger."""
    counts: dict[str, int] = {"A": 0, "B": 0, "C": 0, "REJECT": 0}
    for leg in legs:
        counts[tier_for_leg(leg)] += 1
    return counts


def effective_min_prob(
    user_min_prob: Optional[float],
    slip_type: str,
    n_legs: int,
) -> float:
    """Compute the effective per-leg min prob for the auto-backtest worker.

    If the user has explicitly set `auto_slip_min_prob`, respect it. Otherwise
    use the tier floor that's eligible for this (slip_type, n_legs): TIER_A
    for short slips, TIER_B for long slips. Never below the slip's break-even.
    """
    # Hard floor — never log a leg below the slip's own break-even.
    from engine.constants import BREAK_EVEN
    be = BREAK_EVEN.get((str(n_legs), slip_type.lower()), 0.5407)

    if user_min_prob is not None:
        return max(float(user_min_prob), be)

    # Auto-route: short slips need Tier-A floor, long slips can use Tier-B.
    key = (slip_type.lower(), int(n_legs))
    if _TIER_ELIGIBILITY.get("B", {}).get(key, False):
        return max(TIER_B_MIN_PROB, be)
    return max(TIER_A_MIN_PROB, be)

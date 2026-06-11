"""
Sharp-anchor pricing engine — the OddsJam method, validated on our data.

Principle: the live sharp price IS the model. Past data is a scoreboard,
never a model input.

    fair_prob = devig( Pinnacle's two-sided market at PP's line )
    tradeable = fair_prob >= SHARP_MIN_PROB  (0.56)  and entry is 4-6 leg Flex
    no Pinnacle price -> never tradeable (display-only)

Why 0.56 and not lower: backtested on 40k settled observatory rows —
at pinnacle>=0.54 the selected legs hit 53.7% (BELOW the 54.2% 6-Flex
break-even); at >=0.56 they hit 60.5% (UNDER-only 65.6%, CI-significant).
The edge exists only where Pinnacle disagrees with PrizePicks by a real
margin. Do not lower this number to manufacture volume; volume comes from
Pinnacle market coverage, not looser thresholds.

Why Shin devig: the historical `books["pinnacle"]` values that the backtest
validated were produced by devig_shin on Pinnacle's two-sided market
(engine/consensus._devig_book). Using the same devig keeps the live engine
faithful to the backtested rule.

Explicitly NOT here, by design: no soft-book blending, no sharpness
weights, no isotonic / RWBC / beta calibration, no worst-case minimum,
no synthetic complement odds. One-sided Pinnacle markets return None —
we do not invent the missing side.
"""
from __future__ import annotations

import logging
from typing import Iterable, Optional

from config import SHARP_MIN_PROB, ANCHOR_MODE, WORST_CASE_UNDER_ONLY
from engine.devig import devig_shin, devig_worst_case

logger = logging.getLogger(__name__)

# Slip shapes the placeability-constrained backtest validated as +EV.
# 3-Power is the DEFAULT: needing only 3 co-available sharp legs, it formed
# 44 placeable slips (1.52/day, +25.0u, +56.8%/slip) vs 29 for 4-Flex
# (+11.0u) and just 12 for strict 6-Flex (-1.6u) — placeability beats
# per-slip EV theory on real slates. Flex 4-6 stays allowed for users who
# explicitly configure it.
# PAYOUT DEPENDENCY: +56.8% assumes the 6x 3-Power payout in
# engine/constants.POWER_PAYOUTS. If PrizePicks pays 5x, per-leg break-even
# jumps 0.550 -> 0.585 and the edge thins to ~+11% — verify in-app.
DEFAULT_SLIP_TYPE = "Power"
DEFAULT_SLIP_SIZE = 3
_ALLOWED_SHAPES = {("power", 3), ("flex", 4), ("flex", 5), ("flex", 6)}


def pinnacle_fair_from_books(books: Iterable, side: str) -> Optional[float]:
    """Devigged Pinnacle probability for `side`, or None when Pinnacle
    doesn't price this market two-sided.

    `books` is the per-side BookOdds list app.py already builds
    (engine.consensus.BookOdds shape: book_name / over_odds / under_odds /
    both_sided). Only a both-sided Pinnacle market counts — a one-sided
    price cannot be devigged without inventing the other side, which is
    exactly the synthetic-odds noise this engine exists to remove.
    """
    side = (side or "").lower()
    if side not in ("over", "under"):
        return None
    for b in books:
        if (getattr(b, "book_name", "") or "").lower() != "pinnacle":
            continue
        if not getattr(b, "both_sided", False):
            return None
        over_odds = getattr(b, "over_odds", None)
        under_odds = getattr(b, "under_odds", None)
        if over_odds is None or under_odds is None:
            return None
        try:
            p_over, p_under = devig_shin(over_odds, under_odds)
        except Exception as exc:
            logger.warning("sharp_anchor: devig failed (%s/%s): %s",
                           over_odds, under_odds, exc)
            return None
        p = p_over if side == "over" else p_under
        return max(0.001, min(0.999, float(p)))
    return None


def worst_case_fair_from_books(books: Iterable, side: str) -> Optional[float]:
    """Most conservative fair probability across ALL two-sided books.

    Two nested minimums (the OddsJam 'worst case' construction):
      1. Per book: devig_worst_case runs four vig-removal models
         (multiplicative, additive, power, Shin) and keeps the LOWEST fair
         probability for our side — the most pessimistic assumption about
         how the book distributed its margin.
      2. Across books: take the lowest of those per-book worst cases —
         the most conservative book wins.

    If even this floor clears PP's break-even, the bet is +EV under every
    devig assumption and every contributing book. One-sided books are
    skipped — no synthetic complements, same contract as the Pinnacle path.
    """
    side = (side or "").lower()
    if side not in ("over", "under"):
        return None
    vals: list[float] = []
    for b in books:
        if not getattr(b, "both_sided", False):
            continue
        over_odds = getattr(b, "over_odds", None)
        under_odds = getattr(b, "under_odds", None)
        if over_odds is None or under_odds is None:
            continue
        try:
            p_over, p_under = devig_worst_case(over_odds, under_odds)
        except Exception as exc:
            logger.warning("sharp_anchor: worst-case devig failed (%s/%s): %s",
                           over_odds, under_odds, exc)
            continue
        vals.append(p_over if side == "over" else p_under)
    if not vals:
        return None
    return max(0.001, min(0.999, float(min(vals))))


def fair_from_books(books: Iterable, side: str) -> Optional[float]:
    """Mode dispatcher (config.ANCHOR_MODE). Returns the decision fair
    probability, or None when this row has no tradeable price source.

      pinnacle    Pinnacle two-sided devig or nothing.
      hybrid      Pinnacle when present; rows Pinnacle doesn't price fall
                  back to the worst-case fair (UNDER-gated by default).
      worst_case  worst-case fair for everything (UNDER-gated by default;
                  set WORST_CASE_UNDER_ONLY=false for the literal any-side
                  rule — backtested at ~break-even, realized -14u; beware).
    """
    side_l = (side or "").lower()
    wc_side_ok = (side_l == "under") or (not WORST_CASE_UNDER_ONLY)

    if ANCHOR_MODE == "worst_case":
        if not wc_side_ok:
            return None
        return worst_case_fair_from_books(books, side)

    pin = pinnacle_fair_from_books(books, side)
    if ANCHOR_MODE == "hybrid":
        if pin is not None:
            return pin
        if wc_side_ok:
            return worst_case_fair_from_books(books, side)
        return None

    # Default: pure Pinnacle.
    return pin


def is_tradeable(fair_prob: Optional[float]) -> bool:
    """Single gate: Pinnacle fair probability clears the validated floor."""
    if fair_prob is None:
        return False
    return float(fair_prob) >= SHARP_MIN_PROB


def slip_shape_allowed(slip_type: str, n_legs: int) -> bool:
    """Shapes that cleared the placeability-constrained backtest:
    3-Power (default) and 4/5/6-leg Flex."""
    return ((slip_type or "").lower(), int(n_legs)) in _ALLOWED_SHAPES

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

from config import SHARP_MIN_PROB
from engine.devig import devig_shin

logger = logging.getLogger(__name__)

# Slip shapes the backtest validated as placeable and +EV. 4-leg fallback
# included: feasibility-constrained replay showed 4-6 leg Flex at +40.8%
# ROI/slip vs +17.6% for strict 5/6 (the 4-leg days add real volume).
ALLOWED_SLIP_TYPE = "flex"
ALLOWED_SLIP_SIZES = (4, 5, 6)


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


def is_tradeable(fair_prob: Optional[float]) -> bool:
    """Single gate: Pinnacle fair probability clears the validated floor."""
    if fair_prob is None:
        return False
    return float(fair_prob) >= SHARP_MIN_PROB


def slip_shape_allowed(slip_type: str, n_legs: int) -> bool:
    """Only 4-6 leg Flex cleared the placeability-constrained backtest."""
    return (slip_type or "").lower() == ALLOWED_SLIP_TYPE and int(n_legs) in ALLOWED_SLIP_SIZES

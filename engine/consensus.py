"""
Sharpness-weighted consensus engine (VWAP-style).

Combines devigged probabilities from multiple sportsbooks into a single
"true" probability using:

  1. **Operator sharpness weights** — empirically derived per-book influence
     reflecting each operator's price-discovery quality for player props.
  2. **Market width penalty** — tighter markets (higher confidence) receive
     more influence; wide, uncertain markets are discounted.
  3. **Scaled single-source discount** — when only one book offers a line,
     apply a conservative discount that scales with odds magnitude.

The consensus formula mirrors Volume-Weighted Average Price (VWAP) from
traditional financial markets:

    P_consensus = Σ(p_i × w_i × (1/M_i)) / Σ(w_i × (1/M_i))

Where p_i = Power Method devigged prob, w_i = sharpness weight,
M_i = market width (overround %).
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from typing import Optional

import config as cfg
from engine.devig import (
    american_to_implied,
    devig_power,
    devig_shin,
    devig_multiplicative,
    devig_worst_case,
    devig_single_sided_scaled,
    apply_single_source_discount,
    market_width_cents,
    prob_to_american,
    revigg_power,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# simplify-v1: the decision number is the most-conservative devigged
# probability across books (worst_case_prob), a pure min — so per-book
# sharpness weights and empirical bias corrections no longer gate decisions
# and have been removed. The VWAP `consensus_prob` is kept only as a
# display-side reference and is now a plain unweighted mean.
# ---------------------------------------------------------------------------

# Minimum market width (overround %) to avoid division-by-zero.
# A market with lower overround than this is already extremely efficient.
_MIN_MARKET_WIDTH = 1.0  # 1% overround


# ---------------------------------------------------------------------------
# Book data container
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class BookOdds:
    """Odds from a single sportsbook for one prop."""
    book_name: str          # "fanduel", "draftkings", "pinnacle"
    over_odds: Optional[int | float]
    under_odds: Optional[int | float]
    both_sided: bool


# ---------------------------------------------------------------------------
# Core consensus computation
# ---------------------------------------------------------------------------

def _single_sided_price(book: BookOdds) -> Optional[int | float]:
    """The lone posted American price on a single-sided book (over if present,
    else under). None for two-sided books."""
    if book.both_sided:
        return None
    if book.over_odds is not None:
        return book.over_odds
    if book.under_odds is not None:
        return book.under_odds
    return None


def _single_sided_too_juiced(book: BookOdds) -> bool:
    """True when the only price on a single-sided book is more juiced than the
    cutoff (e.g. -10000). Such a price protects the book; it is not a credible
    probability, so the book must not contribute a fair. Two-sided books and
    normal-vig one-way alts (e.g. -120) are never flagged."""
    price = _single_sided_price(book)
    return price is not None and price < cfg.MAX_SINGLE_SIDED_JUICE


def _has_direct_odds(book: BookOdds, side: str) -> bool:
    """Check if a book has direct (non-derived) odds for the requested side."""
    if book.both_sided:
        return True
    if side == "over" and book.over_odds is not None:
        return True
    if side == "under" and book.under_odds is not None:
        return True
    return False


def _devig_book(book: BookOdds, side: str) -> Optional[float]:
    """
    Devig a single book's odds for the requested side using the best
    available method.

    - Both-sided → Shin (1993) Method (primary). Estimates the per-market
      insider-trading parameter z directly from the implied prices, so
      each market gets a bias correction tuned to its own overround
      rather than the global favorite-longshot prior assumed by Power.
    - Single-sided → Scaled single-sided devig for the available side,
      complement (1 - p) for the missing side.
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        p_over, p_under = devig_shin(book.over_odds, book.under_odds)
        return p_over if side == "over" else p_under

    # Single-sided: same juice cutoff + credibility cap as the worst-case path,
    # so the consensus number stays bounded for green-devil milestone lines.
    if _single_sided_too_juiced(book):
        return None
    cap = cfg.SINGLE_SIDED_PROB_CAP
    if book.over_odds is not None:
        p_over = min(devig_single_sided_scaled(book.over_odds), cap)
        return p_over if side == "over" else 1.0 - p_over
    if book.under_odds is not None:
        p_under = min(devig_single_sided_scaled(book.under_odds), cap)
        return p_under if side == "under" else 1.0 - p_under

    return None


def _devig_book_worst_case(book: BookOdds, side: str) -> Optional[float]:
    """
    Devig using worst-case method for conservative output.
    Same complement logic for single-sided books.

    Single-sided guardrail (simplify-v1): an extremely juiced one-way price
    (more negative than config.MAX_SINGLE_SIDED_JUICE) contributes nothing,
    and any single-sided devigged prob is capped at config.SINGLE_SIDED_PROB_CAP
    — no one-way market with no two-sided market behind it deserves more trust.
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        p_over, p_under = devig_worst_case(book.over_odds, book.under_odds)
        return p_over if side == "over" else p_under

    # Single-sided: refuse a juiced price outright, else devig the available
    # side, cap it, and complement for the missing side.
    if _single_sided_too_juiced(book):
        return None
    cap = cfg.SINGLE_SIDED_PROB_CAP
    if book.over_odds is not None:
        p_over = min(devig_single_sided_scaled(book.over_odds), cap)
        return p_over if side == "over" else 1.0 - p_over
    if book.under_odds is not None:
        p_under = min(devig_single_sided_scaled(book.under_odds), cap)
        return p_under if side == "under" else 1.0 - p_under

    return None


def _get_market_width(book: BookOdds) -> float:
    """
    Get market width in percentage points.
    Returns the overround for two-sided markets, or a conservative
    default for single-sided markets (indicating low confidence).
    """
    if book.both_sided and book.over_odds is not None and book.under_odds is not None:
        width = market_width_cents(book.over_odds, book.under_odds)
        return max(width, _MIN_MARKET_WIDTH)
    # Single-sided markets get a high-width penalty (low confidence)
    return 15.0  # ~15% assumed overround for one-way markets


_DEFAULT_MARGIN = 0.07   # 7% — typical US sportsbook overround for props

def _get_side_odds(book: BookOdds, side: str) -> Optional[int | float]:
    """
    Get the American odds for a specific side.

    If the book only has the opposite side, derive realistic vigged odds
    for the requested side using the inverse Power Method with a standard
    7% overround.  This ensures derived odds look like real book odds
    (implied probs sum to ~107%) rather than fair/no-vig odds.
    """
    direct = book.over_odds if side == "over" else book.under_odds
    if direct is not None:
        return direct

    # Derive from the opposite side using inverse Power Method re-vig
    opposite = book.under_odds if side == "over" else book.over_odds
    if opposite is not None:
        available_true = devig_single_sided_scaled(opposite)
        missing_true = 1.0 - available_true
        if missing_true <= 0 or missing_true >= 1:
            return None
        # Re-vig with realistic margin
        if side == "over":
            vigged_over, _ = revigg_power(missing_true, available_true, _DEFAULT_MARGIN)
            return prob_to_american(vigged_over)
        else:
            _, vigged_under = revigg_power(available_true, missing_true, _DEFAULT_MARGIN)
            return prob_to_american(vigged_under)

    return None


def compute_true_probability(
    books: list[BookOdds],
    side: str,
    league: str | None = None,
    prop: str | None = None,
) -> tuple[Optional[float], Optional[float], dict]:
    """
    Compute the consensus true probability for a given side (over/under)
    across all available sportsbooks.

    Returns:
        (consensus_prob, worst_case_prob, metadata)

    Where:
    - consensus_prob: VWAP sharpness-weighted probability (informational)
    - worst_case_prob: most conservative probability (used for EV decisions)
    - metadata: dict with n_books, devig_method, market_widths, etc.

    `league`/`prop` are retained for signature compatibility with callers
    (clv_checker, pipeline) but no longer drive any per-book correction — the
    decision number is the pure min-across-books worst case.
    """
    # ── Safeguard: reject purely complement-derived probabilities ─────────
    # If NO book has direct odds for the requested side (i.e. every book's
    # probability is derived via complement from the opposite side), reject.
    # Complement-derived probabilities from extreme longshot single-sided
    # lines (e.g. +700 'to record 1+ shots') are unreliable and produce
    # phantom high-EV bets.
    has_any_direct = any(_has_direct_odds(b, side) for b in books)
    if not has_any_direct:
        return None, None, {"n_books": 0, "devig_method": "no_direct_odds"}

    # Collect per-book data as tuples: (power_prob, worst_prob, width, odds).
    entries: list[tuple] = []
    for book in books:
        # Drop extremely juiced one-way prices outright (e.g. -10000): they
        # protect the book, they don't inform a probability.
        if _single_sided_too_juiced(book):
            continue
        power_prob = _devig_book(book, side)
        worst_prob = _devig_book_worst_case(book, side)
        odds = _get_side_odds(book, side)

        if power_prob is None or odds is None:
            continue

        width = _get_market_width(book)
        entries.append((power_prob, worst_prob, width, odds))

    if not entries:
        return None, None, {"n_books": 0, "devig_method": "none"}

    n_books = len(entries)

    # ------------------------------------------------------------------
    # Single-source fallback
    # ------------------------------------------------------------------
    if n_books == 1:
        power_prob, worst_prob, width, odds = entries[0]
        prob = worst_prob if worst_prob is not None else power_prob

        # Apply the scaled single-source uncertainty discount
        discounted = apply_single_source_discount(prob, odds)

        return (
            discounted,
            discounted,
            {
                "n_books":      1,
                "devig_method": "single_source_scaled",
                "market_width": float(width),
            },
        )

    # ------------------------------------------------------------------
    # Multi-source aggregation
    # ------------------------------------------------------------------
    # The DECISION number is `worst_case_prob` — the single lowest worst-case
    # devigged probability across books (most conservative line + most
    # conservative devig). `consensus_prob` is kept only as a plain unweighted
    # display mean; min ≤ mean always, so worst_case ≤ consensus by construction.
    prob_sum = 0.0
    width_sum = 0.0
    worst_case_prob: Optional[float] = None

    for power_prob, worst_prob, width, _odds in entries:
        prob_sum += power_prob
        width_sum += width
        if worst_prob is not None and (worst_case_prob is None or worst_prob < worst_case_prob):
            worst_case_prob = worst_prob

    consensus_prob = prob_sum / n_books if n_books > 0 else None
    consensus_width = width_sum / n_books if n_books > 0 else None

    metadata = {
        "n_books":      n_books,
        "devig_method": "worst_case_min",
        "market_width": consensus_width,
    }

    return consensus_prob, worst_case_prob, metadata


# ---------------------------------------------------------------------------
# Convenience: build BookOdds from matcher objects
# ---------------------------------------------------------------------------

def books_from_match(fd, dk, pin) -> list[BookOdds]:
    """
    Build a list of BookOdds from the FanDuelProp-shaped objects used by the
    matcher (fd, dk, pin can each be None).
    """
    books = []
    if fd is not None:
        books.append(BookOdds(
            book_name="fanduel",
            over_odds=fd.over_odds,
            under_odds=fd.under_odds,
            both_sided=fd.both_sided,
        ))
    if dk is not None:
        books.append(BookOdds(
            book_name="draftkings",
            over_odds=dk.over_odds,
            under_odds=dk.under_odds,
            both_sided=dk.both_sided,
        ))
    if pin is not None:
        books.append(BookOdds(
            book_name="pinnacle",
            over_odds=pin.over_odds,
            under_odds=pin.under_odds,
            both_sided=pin.both_sided,
        ))
    return books


def books_from_match_for_side(m, side: str) -> list[BookOdds]:
    """Build the BookOdds list using each book's per-side equivalent at
    PP's line. When PP is a whole number, this is the half-step alt that's
    mathematically equivalent for `side`; otherwise it falls back to the
    exact-line book.

    The opposite side is intentionally masked to None so the consensus
    devig path treats this as a single-sided market. A book at line+0.5
    has a valid over price for PP's whole line, but its under price at
    that same alt line corresponds to a different push semantic and must
    not be folded into the consensus."""
    side = (side or "").lower()
    if side not in ("over", "under"):
        return []

    if side == "over":
        fd_eq, dk_eq, pin_eq = m.fd_over_equiv, m.dk_over_equiv, m.pin_over_equiv
        nv_eq = getattr(m, "nv_over_equiv", None)
    else:
        fd_eq, dk_eq, pin_eq = m.fd_under_equiv, m.dk_under_equiv, m.pin_under_equiv
        nv_eq = getattr(m, "nv_under_equiv", None)

    books: list[BookOdds] = []
    pp_line = m.pp.line_score

    def _add(book_name: str, prop) -> None:
        if prop is None:
            return
        # Exact-line equivalent: full both-sided BookOdds, unchanged.
        if prop.line == pp_line:
            books.append(BookOdds(
                book_name=book_name,
                over_odds=prop.over_odds,
                under_odds=prop.under_odds,
                both_sided=prop.both_sided,
            ))
            return
        # Half-step equivalent: only the relevant side is valid.
        if side == "over":
            books.append(BookOdds(
                book_name=book_name,
                over_odds=prop.over_odds,
                under_odds=None,
                both_sided=False,
            ))
        else:
            books.append(BookOdds(
                book_name=book_name,
                over_odds=None,
                under_odds=prop.under_odds,
                both_sided=False,
            ))

    _add("fanduel",    fd_eq)
    _add("draftkings", dk_eq)
    _add("pinnacle",   pin_eq)
    _add("novig",      nv_eq)
    return books

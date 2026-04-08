"""
Advanced de-vig utilities.

Implements multiple margin-removal methodologies for sports proposition markets:

- **Power Method**: Nonlinear exponentiation that naturally handles the
  favorite-longshot bias.  Solves  π_over^k + π_under^k = 1  via bisection.
- **Multiplicative**: Proportional normalization (baseline / fallback).
- **Additive**: Equal subtraction of overround.
- **Worst-Case**: Runs all methods and returns the lowest (most conservative)
  true probability for each side.
- **Scaled Single-Sided**: For one-way / unmatched markets, applies a vig
  assumption that scales with odds magnitude — longshots get heavier vig.
"""
from __future__ import annotations

import math
from typing import Optional





# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def american_to_decimal(american: int | float) -> float:
    """Convert American odds to decimal odds."""
    if american > 0:
        return (american / 100.0) + 1.0
    else:
        return (100.0 / abs(american)) + 1.0


def american_to_implied(american: int | float) -> float:
    """Raw (vigged) implied probability from American odds."""
    return 1.0 / american_to_decimal(american)


def prob_to_american(prob: float) -> float:
    """Convert true probability to American odds."""
    if prob <= 0 or prob >= 1:
        return 0.0
    decimal = 1.0 / prob
    if decimal >= 2.0:
        return (decimal - 1.0) * 100.0
    else:
        return -100.0 / (decimal - 1.0)


def market_width_cents(over_american: int | float, under_american: int | float) -> float:
    """
    Calculate market width in cents.

    Width = |implied_over + implied_under - 1| expressed as a percentage.
    A 20-cent market (-110/-110) has width ≈ 4.76%.
    A 70-cent market (-120/-150) has much wider width.
    We return the raw overround percentage (e.g. 4.76 for a -110/-110 market).
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    return (implied_over + implied_under - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Multiplicative de-vig (baseline)
# ---------------------------------------------------------------------------

def devig_multiplicative(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Multiplicative de-vig.  Returns (true_over_prob, true_under_prob).
    Scales each implied probability proportionally so they sum to 1.
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    total = implied_over + implied_under
    if total <= 0:
        return 0.5, 0.5
    return implied_over / total, implied_under / total


# ---------------------------------------------------------------------------
# Additive de-vig
# ---------------------------------------------------------------------------

def devig_additive(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Additive de-vig.  Subtracts an equal share of the overround from each
    outcome.  Can produce negative probabilities for extreme longshots —
    those are clamped to a small ε.
    """
    implied_over = american_to_implied(over_american)
    implied_under = american_to_implied(under_american)
    overround = implied_over + implied_under - 1.0
    half_or = overround / 2.0

    p_over = max(implied_over - half_or, 0.001)
    p_under = max(implied_under - half_or, 0.001)

    # Re-normalise so they sum to 1
    total = p_over + p_under
    return p_over / total, p_under / total


# ---------------------------------------------------------------------------
# Power Method de-vig
# ---------------------------------------------------------------------------

def _power_objective(k: float, pi_over: float, pi_under: float) -> float:
    """Evaluate  π_over^k + π_under^k  — target is 1.0."""
    return pi_over ** k + pi_under ** k


def devig_power(
    over_american: int | float,
    under_american: int | float,
    *,
    tol: float = 1e-9,
    max_iter: int = 200,
) -> tuple[float, float]:
    """
    Power Method de-vig.  Solves for exponent *k* such that:

        π_over^k + π_under^k = 1

    Because k > 1 in a market with vig, raising the fractional implied
    probability to *k* reduces the overall probability.  Crucially, smaller
    probabilities (longshots) are reduced by a *larger relative proportion*
    than larger probabilities (favorites), mirroring the favorite-longshot
    bias that bookmakers embed.

    Falls back to multiplicative if bisection fails to converge.
    """
    pi_over = american_to_implied(over_american)
    pi_under = american_to_implied(under_american)

    booksum = pi_over + pi_under
    if booksum <= 1.0:
        # No vig detected — return implied directly (already fair)
        return pi_over, pi_under

    # Bisection for k in [1, k_max]
    k_lo, k_hi = 1.0, 20.0

    # Ensure bracket validity
    f_lo = _power_objective(k_lo, pi_over, pi_under) - 1.0  # positive (booksum > 1)
    f_hi = _power_objective(k_hi, pi_over, pi_under) - 1.0

    if f_hi > 0:
        # k_hi too small — fall back to multiplicative
        return devig_multiplicative(over_american, under_american)

    for _ in range(max_iter):
        k_mid = (k_lo + k_hi) / 2.0
        f_mid = _power_objective(k_mid, pi_over, pi_under) - 1.0

        if abs(f_mid) < tol:
            break

        if f_mid > 0:
            k_lo = k_mid
        else:
            k_hi = k_mid

    k = (k_lo + k_hi) / 2.0
    p_over = pi_over ** k
    p_under = pi_under ** k

    # Normalise for floating-point safety
    total = p_over + p_under
    if total <= 0:
        return devig_multiplicative(over_american, under_american)

    return p_over / total, p_under / total


# ---------------------------------------------------------------------------
# Worst-Case de-vig (defensive failsafe)
# ---------------------------------------------------------------------------

def devig_worst_case(
    over_american: int | float,
    under_american: int | float,
) -> tuple[float, float]:
    """
    Run Power, Multiplicative, and Additive methods in parallel.
    Return the **lowest** true probability for each side across all methods.

    This provides the most mathematically conservative estimate, aggressively
    protecting against unobservable margins and variance.
    """
    methods = [devig_power, devig_multiplicative, devig_additive]

    over_probs = []
    under_probs = []

    for method in methods:
        try:
            p_o, p_u = method(over_american, under_american)
            over_probs.append(p_o)
            under_probs.append(p_u)
        except Exception:
            continue

    if not over_probs:
        return 0.5, 0.5

    return min(over_probs), min(under_probs)


# ---------------------------------------------------------------------------
# Scaled single-sided de-vig
# ---------------------------------------------------------------------------

# Base vig for standard lines near even money (-110 / +100)
_BASE_VIG = 0.05

# Additional vig per unit of "longshot distance" — at +500 this adds ~5%
_LONGSHOT_SLOPE = 0.15


def devig_single_sided_scaled(american: int | float) -> float:
    """
    Improved single-sided de-vig with scaled vig assumption.

    Instead of a flat vig (e.g. 7%), the assumed vig increases with odds
    magnitude.  Longshots carry astronomically higher unobservable holds,
    so we penalise them more aggressively:

        implied_prob      vig_assumed
        ──────────────    ───────────
        ~50% (even)       ~5%
        ~33% (+200)       ~7%
        ~20% (+400)       ~9%
        ~17% (+500)       ~10%
        ~9%  (+1000)      ~12%

    Formula:
        distance = max(0,  0.50 - implied_prob)  # how far from 50-50
        vig = BASE_VIG + LONGSHOT_SLOPE × distance
        true_prob = implied_prob / (1 + vig)
    """
    implied = american_to_implied(american)

    # Distance from 50-50 — longshots are further
    distance = max(0.0, 0.50 - implied)

    vig = _BASE_VIG + _LONGSHOT_SLOPE * distance
    return implied / (1.0 + vig)


def devig_single_sided(american: int | float) -> float:
    """
    Legacy single-sided de-vig.  Kept as a compatibility shim that now
    delegates to the improved scaled version.
    """
    return devig_single_sided_scaled(american)


# ---------------------------------------------------------------------------
# Single-source uncertainty discount
# ---------------------------------------------------------------------------

def apply_single_source_discount(
    prob: float,
    american_odds: int | float,
) -> float:
    """
    When only one sportsbook offers a line, apply a scaled uncertainty
    discount.  The discount increases with odds magnitude because isolated
    longshot markets carry the highest, most predatory unobservable holds.

        ~even money (+100):  ~5% discount   → multiply by 0.95
        +200–+400:           ~7% discount   → multiply by 0.93
        +500+:               ~10% discount  → multiply by 0.90

    Formula:
        discount_factor = 0.95 - 0.05 × clamp((|american| - 100) / 400, 0, 1)
    """
    abs_odds = abs(american_odds)
    # Linear ramp from 0.95 (at ±100) → 0.90 (at ±500+)
    ratio = max(0.0, min(1.0, (abs_odds - 100.0) / 400.0))
    discount_factor = 0.95 - 0.05 * ratio

    return prob * discount_factor

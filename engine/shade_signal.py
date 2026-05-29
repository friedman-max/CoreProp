"""
PP shade signal (Maybe Cool Fix C1).

PrizePicks doesn't quote odds — they offer fixed payouts (Power 6 = 40×,
3 = 6×, etc.). Implicitly, PP assumes each leg has a break-even probability
of 1/decimal_payout (0.5408 for Power-6 components). If a book-consensus
devigged probability for a side is materially above 0.5408, PP is offering
that side at "shaded" pricing — favorable to the bettor.

The MAGNITUDE of the shade is information the calibrator currently ignores:
  - Phase 1A audit D1 found mean CLV at p≥0.65 is -14.9% — i.e. the model's
    high-confidence picks systematically lose CLV. These are precisely the
    legs with LARGE shade (probability far from 0.5408).
  - One hypothesis: large shade is correlated with high reverse-engineering
    risk — PP's smart-money traders close those gaps faster than the
    average leg.

This module provides:
  1. pp_shade(true_prob) → float
       Signed shade in probability-points. Positive = consensus thinks side
       is more likely than PP's implicit break-even.
  2. shade_bucket(shade) → str
       Categorical bucket for binning historical CLV-by-shade analysis.
  3. is_anti_public(leg, *, shade_threshold) → bool
       Stub anti-public filter. Returns True if the leg's shade exceeds
       `shade_threshold` AND lands in a (league, side) cell flagged as
       historically anti-skill at high shade. The flag list is loaded from
       data/anti_public_cells.json and is refreshed by the strategy
       comparison logger (Phase 3).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

from engine.constants import OPTIMAL_BREAK_EVEN

logger = logging.getLogger(__name__)

# Path to the (cell, shade_bucket) deny-list refit by Phase 3.
ANTI_PUBLIC_FILE = os.path.join(
    os.path.dirname(__file__), "..", "data", "anti_public_cells.json",
)


# ─────────────────────────────────────────────────────────────────────────
# Shade computation
# ─────────────────────────────────────────────────────────────────────────

def pp_shade(true_prob: float | None) -> float:
    """Signed PP shade in probability-points.

    Positive value: side is more likely than PP's implicit break-even
    (0.5408 for Power-6 components). Magnitude is information about how
    far PP has shaded the line relative to fair pricing.

    Returns 0.0 on None / invalid input so call sites don't need to guard.
    """
    if true_prob is None:
        return 0.0
    try:
        p = float(true_prob)
    except (TypeError, ValueError):
        return 0.0
    if not (0.0 < p < 1.0):
        return 0.0
    return p - OPTIMAL_BREAK_EVEN


# Shade buckets used by historical CLV-by-shade analysis. Boundaries
# tuned to make each bucket roughly equipopulated on the 38k observatory
# sample (FINDINGS-era bucketing).
_SHADE_BUCKETS: list[tuple[float, float, str]] = [
    (-1.00, -0.05, "very_negative"),
    (-0.05,  0.00, "negative"),
    ( 0.00,  0.03, "neutral_low"),
    ( 0.03,  0.06, "neutral_high"),
    ( 0.06,  0.10, "positive"),
    ( 0.10,  1.00, "very_positive"),
]


def shade_bucket(shade: float) -> str:
    """Categorical bucket label for a signed shade value."""
    try:
        s = float(shade)
    except (TypeError, ValueError):
        return "neutral_low"
    for lo, hi, label in _SHADE_BUCKETS:
        if lo <= s < hi:
            return label
    return "very_positive" if s > 0 else "very_negative"


# ─────────────────────────────────────────────────────────────────────────
# Anti-public deny-list
# ─────────────────────────────────────────────────────────────────────────
#
# Refit by Phase 3. JSON shape:
#   {
#     "updated_at": "2026-05-29T00:00:00Z",
#     "min_obs": 50,
#     "min_neg_clv_pp": -0.03,         # threshold to flag a cell
#     "denied": {
#         "NBA|over|very_positive":     {"n": 210, "mean_clv": -0.149},
#         "NBA|over|positive":          {"n": 513, "mean_clv": -0.095},
#         ...
#     }
#   }
#
# A leg is anti_public if its (league, side, shade_bucket) lands in
# `denied`. The denied dict is loaded once at module import and via
# reload_anti_public() after each Phase 3 refit.

_anti_public_map: dict[str, dict] = {}


def _load_anti_public() -> dict[str, dict]:
    """Read the deny-list off disk. Returns empty dict when missing."""
    try:
        if not os.path.exists(ANTI_PUBLIC_FILE):
            return {}
        with open(ANTI_PUBLIC_FILE) as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        denied = raw.get("denied") or {}
        return denied if isinstance(denied, dict) else {}
    except Exception as exc:
        logger.warning("anti_public load failed: %s", exc)
        return {}


def reload_anti_public() -> int:
    """Re-read the deny-list. Returns count of denied buckets."""
    global _anti_public_map
    _anti_public_map = _load_anti_public()
    return len(_anti_public_map)


# Cold-start: try once at import.
_anti_public_map = _load_anti_public()


def is_anti_public(
    leg: dict | None,
    *,
    shade_threshold: float = 0.06,
) -> bool:
    """True if the leg lands in a (league, side, shade_bucket) flagged as
    historically anti-skill at high shade.

    `shade_threshold` is a floor — buckets below this are never flagged
    even if their historical CLV is negative (could be ordinary variance).
    The Phase 3 refit sets `min_neg_clv_pp` for the cell-level criterion.
    """
    if not leg:
        return False
    if not _anti_public_map:
        return False

    shade = pp_shade(leg.get("true_prob"))
    if shade < shade_threshold:
        return False

    league = (leg.get("league") or "").upper()
    side   = (leg.get("side") or "").lower()
    bucket = shade_bucket(shade)
    key = f"{league}|{side}|{bucket}"
    return key in _anti_public_map


def annotate(leg: dict) -> dict:
    """Return a copy of `leg` with `pp_shade` and `shade_bucket` fields
    appended. Used by Phase 3 logger so the comparison dataset carries
    shade info on every row.
    """
    out = dict(leg)
    s = pp_shade(leg.get("true_prob"))
    out["pp_shade"] = round(s, 4)
    out["shade_bucket"] = shade_bucket(s)
    return out

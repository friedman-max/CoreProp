"""
Smoothed empirical hit-rate calibration.

For any line we ask: "historically, when a line for this (league, prop,
side) had this devig probability, how often did it hit?" The answer is
a Beta-Binomial shrunken empirical hit rate within a 5%-wide raw-prob
band, with the raw devig probability itself acting as the Beta prior.

    p_calibrated = (hits_w + κ·raw_prob) / (n_eff + κ)

- Thin band (small n_eff) → answer ≈ raw_prob (the prior).
- Thick band (large n_eff) → answer ≈ band-empirical hit rate.
- Smooth interpolation between the two via the single hyperparameter κ.

This is intentionally simpler than the PAV calibrator in
`isotonic_calibration.py`: no monotonicity enforcement, no parent-level
shrinkage, no curve-fitting. The only feature is "fit history within
this exact (league, prop, side) bucket, shrunk toward the book's own
implied probability." The raw devig prob is already a strong baseline,
so the calibrated number can never drift far from it on thin data.

State source: this module reads the same `isotonic_state.json`
sufficient statistics maintained by the PAV refit job. The cell layout
is `[sum_w, sum_yw, sum_xw, outcome_w, outcome_hits_w]`; we only use
positions 3 and 4 (recency-weighted outcome count and hits) so CLV
observations don't contaminate the empirical hit rate.
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

EMPIRICAL_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "empirical_calibration.json")

# Beta prior strength. At n_eff = κ the empirical hit rate carries 50%
# of the weight vs the raw devig probability; at 4·κ, ~80%. Higher κ ⇒
# more shrinkage toward the book line on thin buckets; lower κ ⇒
# empirical evidence takes over faster.
PRIOR_STRENGTH = 50.0

# Width of each raw-probability band. 5% bands give ~14 bands across the
# bet-relevant range [30%, 99%] — fine enough that within-band variation
# is small, coarse enough that each band aggregates meaningful sample.
BAND_WIDTH = 0.05

# Match the bin grid the isotonic state uses (200 bins on [0, 1]).
_STATE_BIN_COUNT = 200

# Minimum recency-weighted outcome count for a band to be emitted at
# all. Below this the band is dropped from the table and `apply_empirical`
# falls back to the raw probability — the prior would dominate anyway,
# so emitting the band would just add noise.
MIN_BAND_N_EFF = 2.0


def _band_index(p: float) -> int:
    p = min(max(p, 0.0), 0.999999)
    # Add a tiny epsilon before flooring: 0.60 / 0.05 evaluates to
    # 11.9999…, which would otherwise put a clean 0.60 into band 11
    # ([0.55, 0.60)) instead of band 12 ([0.60, 0.65)).
    return int(p / BAND_WIDTH + 1e-9)


def _band_range(idx: int) -> tuple[float, float]:
    lo = idx * BAND_WIDTH
    return lo, min(lo + BAND_WIDTH, 1.0)


def _aggregate_band(by_bin: dict, lo: float, hi: float) -> tuple[float, float]:
    """Sum (outcome_w, outcome_hits_w) across the state x_bins that fall
    inside [lo, hi). Returns (n_eff, hits_w)."""
    bin_lo = int(round(lo * _STATE_BIN_COUNT))
    bin_hi = int(round(hi * _STATE_BIN_COUNT))
    n_eff = 0.0
    hits_w = 0.0
    for b, cell in by_bin.items():
        if bin_lo <= b < bin_hi and len(cell) >= 5:
            n_eff += cell[3]
            hits_w += cell[4]
    return n_eff, hits_w


# ---------------------------------------------------------------------------
# Build / persist
# ---------------------------------------------------------------------------

def build_empirical_table(state: dict) -> dict:
    """Build a per-(league, prop, side) band table from the PAV refit's
    sufficient-statistics state. Output shape:

        {
          "version": 1,
          "fitted_at": "...",
          "props": {
             "nba|points|over": {
                "bands": [
                   {"lo": 0.50, "hi": 0.55, "n_eff": 12.3, "hit_rate": 0.58},
                   ...
                ]
             },
             ...
          },
          "config": {"prior_strength": 50.0, "band_width": 0.05}
        }
    """
    out: dict = {
        "version":   1,
        "fitted_at": datetime.now(timezone.utc).isoformat(),
        "props":     {},
        "config": {
            "prior_strength": PRIOR_STRENGTH,
            "band_width":     BAND_WIDTH,
        },
    }

    n_bands = int(round(1.0 / BAND_WIDTH))
    for key, by_bin in (state.get("props") or {}).items():
        # Only keep 3-key (league|prop|side) entries; the legacy 2-key
        # bucket conflates over/under and would muddy the calibration.
        parts = key.split("|")
        if len(parts) != 3 or parts[2] not in ("over", "under"):
            continue
        bands: list[dict] = []
        for idx in range(n_bands):
            lo, hi = _band_range(idx)
            n_eff, hits_w = _aggregate_band(by_bin, lo, hi)
            if n_eff < MIN_BAND_N_EFF:
                continue
            bands.append({
                "lo":       round(lo, 4),
                "hi":       round(hi, 4),
                "n_eff":    round(n_eff, 3),
                "hit_rate": round(hits_w / n_eff, 6),
            })
        if bands:
            out["props"][key] = {"bands": bands}

    return out


def save_empirical_calibration(table: dict) -> None:
    """Persist the table to disk and mirror to Supabase."""
    try:
        os.makedirs(os.path.dirname(EMPIRICAL_FILE), exist_ok=True)
        with open(EMPIRICAL_FILE, "w") as f:
            json.dump(table, f, indent=2)
    except Exception as exc:
        logger.error("EmpiricalCalibration: write failed: %s", exc)
        return
    try:
        from engine.persistence import sync_state_to_supabase
        sync_state_to_supabase("empirical_calibration", table)
    except Exception as exc:
        logger.warning("EmpiricalCalibration: Supabase mirror failed: %s", exc)


def load_empirical_calibration() -> dict:
    """Load the empirical table from disk, falling back to the Supabase
    mirror when the local file is absent (ephemeral-filesystem deploys).
    Returns an empty table if neither source has a v1 payload."""
    raw: dict | None = None
    if os.path.exists(EMPIRICAL_FILE):
        try:
            with open(EMPIRICAL_FILE, "r") as f:
                raw = json.load(f)
        except Exception:
            raw = None

    if raw is None:
        try:
            from engine.persistence import load_state_from_supabase
            mirrored, _ = load_state_from_supabase("empirical_calibration")
            if isinstance(mirrored, dict) and mirrored.get("version") == 1:
                try:
                    os.makedirs(os.path.dirname(EMPIRICAL_FILE), exist_ok=True)
                    with open(EMPIRICAL_FILE, "w") as f:
                        json.dump(mirrored, f, indent=2)
                except Exception:
                    pass
                raw = mirrored
        except Exception as exc:
            logger.debug("EmpiricalCalibration: Supabase fallback unavailable: %s", exc)

    if not isinstance(raw, dict) or raw.get("version") != 1:
        return {"props": {}, "fitted_at": None, "config": {}}

    return {
        "props":     raw.get("props") or {},
        "fitted_at": raw.get("fitted_at"),
        "config":    raw.get("config") or {},
    }


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def apply_empirical(table: dict, league: str | None, prop: str | None,
                    side: str | None, raw_prob: float) -> float:
    """Beta-Binomial shrunken empirical hit rate.

    Returns `raw_prob` unchanged when the table doesn't have data for
    this exact (league, prop, side) bucket at this raw-prob band — the
    Beta prior would dominate anyway, so a missing band and a near-empty
    band collapse to the same answer.
    """
    if not (0.0 < raw_prob < 1.0):
        return raw_prob
    side_norm = (side or "").lower()
    if not (table and league and prop and side_norm in ("over", "under")):
        return raw_prob

    entry = (table.get("props") or {}).get(f"{league}|{prop}|{side_norm}")
    if not entry:
        return raw_prob

    idx = _band_index(raw_prob)
    lo, hi = _band_range(idx)
    band = None
    for b in entry.get("bands", []):
        if abs(b["lo"] - lo) < 1e-9:
            band = b
            break
    if band is None:
        return raw_prob

    n_eff = float(band.get("n_eff") or 0.0)
    hit_rate = float(band.get("hit_rate") or 0.0)
    if n_eff <= 0.0:
        return raw_prob

    # Posterior mean of Beta(α + hits, β + misses) where the prior is
    # Beta(κ·raw_prob, κ·(1-raw_prob)).
    hits_w = hit_rate * n_eff
    return (hits_w + PRIOR_STRENGTH * raw_prob) / (n_eff + PRIOR_STRENGTH)

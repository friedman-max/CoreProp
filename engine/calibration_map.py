"""
Isotonic recalibration map — bends the raw consensus probability onto the
realized hit-rate curve, per (league, side).

Why this exists (2026-07-09 odds audit, analysis/FINDINGS.md §2 + the
CALIBRATION_RUNBOOK):

    The raw no-vig consensus is miscalibrated in two ways a constant
    additive shift (`config.SIDE_BIAS`) cannot fix:

      1. Offset — PP shades lines toward OVERs (~5-10pp); SIDE_BIAS handles
         this and only this.
      2. SLOPE — the model's calibration slope is alpha ~= 0.49 globally: it
         overstates its departure from a coin flip by ~2x. On some cells
         (NBA UNDER, NHL) the slope is INVERTED. No additive constant can
         correct a slope, which is why calibration below the 0.65 gate has
         been left to the high threshold alone.

    Isotonic regression fits a monotonic non-decreasing map raw -> realized
    that subsumes both the offset and the slope in one pass. On a cell whose
    signal is inverted, the monotone fit correctly collapses toward the
    cell's base rate — the prediction becomes HONEST (calibrated) even though
    it becomes uninformative. That is the intended behavior: calibration
    makes the number accurate; it does not manufacture signal that isn't
    there. Cells with no usable signal should still be handled by CELL_DROPS,
    not by pretending the map rescued them.

Discipline (mirrors SIDE_BIAS, per the runbook):
  - Fit on `raw_true_prob` (the one ruler). `raw_true_prob` is never touched,
    so CLV, the observatory training corpus, and every future refit keep
    measuring against the same uncorrected number.
  - Applied ONLY to the DECISION prob (`BetResult.true_prob`), and ONLY when
    `config.CALIBRATION_MAP_ENABLED` is true (defaults OFF). Green devils are
    exempt — the corpus is standard lines.
  - When the map is enabled and a cell has a TRUSTED fit, it REPLACES the
    per-cell SIDE_BIAS (the isotonic curve already carries any offset;
    stacking both would double-correct). Cells without a trusted fit fall
    back to the existing SIDE_BIAS path, so partial coverage degrades safely.

The fit is refreshed hourly by the scheduler (alongside the correlation map)
and persisted to data/calibration_map.json + the Supabase app_state_cache
mirror, exactly like engine.correlation.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import config as cfg

logger = logging.getLogger(__name__)

# Persisted JSON artefact — auto-loaded at import, refreshed hourly.
CALIBRATION_FILE = os.path.join(
    os.path.dirname(__file__), "..", "data", "calibration_map.json"
)

# Bins the raw prob into fixed-width cells before fitting. Binning turns the
# noisy per-observation (p, 0/1) cloud into stable per-bin hit rates that
# PAVA can pool; it also keeps the persisted map tiny (one knot per populated
# bin). 2% matches the width of the diagnostic buckets in calibration.py.
_BIN_WIDTH = 0.02
_BIN_LO = 0.02          # ignore sub-2% and 98%+ raw probs (devig never emits them)
_BIN_HI = 0.98

# A bin must carry at least this many resolved observations to enter the fit.
# Below it the bin's hit rate is noise and would inject spurious violations
# for PAVA to pool. ~25 gives a binomial SE of ~0.10 on the bin rate.
_MIN_BIN_OBS = 25

# Trust gates for a per-cell fit. A cell is applied ONLY when it clears BOTH:
#   - MIN_CELL_OBS total resolved rows (enough signal to bend the curve), and
#   - MIN_BINS_SPANNED populated bins (a curve, not a single point we'd be
#     extrapolating flat across the whole range).
# Below either, the cell is written to the artefact for diagnostics but
# apply_calibration() will not use it — it falls through to the global map,
# then to identity (raw).
MIN_CELL_OBS = 500
MIN_BINS_SPANNED = 5

# How far back to pull settled observatory rows for the fit. Calibration
# drift is slower than a single slate but faster than a season; 90 days
# balances responsiveness against sample size and matches the RAM budget the
# correlation fit already proved safe on the 512 MB tier.
_FIT_WINDOW_DAYS = 90

# Module-level map populated at import and after each scheduler run.
# Shape: {"cells": {"NBA|over": {"n": int, "knots": [[x, y], ...]}, ...},
#         "global": {"n": int, "knots": [[x, y], ...]}}
_map: dict = {}


# ---------------------------------------------------------------------------
# Pool Adjacent Violators Algorithm (weighted isotonic regression)
# ---------------------------------------------------------------------------

def _pava(values: list[float], weights: list[float]) -> list[float]:
    """Weighted isotonic (non-decreasing) fit via PAVA.

    Given per-bin hit rates `values` (ordered by bin) and per-bin counts
    `weights`, returns a same-length list of fitted values that is monotone
    non-decreasing and minimizes the weighted squared error. Adjacent bins
    that violate monotonicity are pooled into their weighted mean.

    Dependency-free (no sklearn/scipy in the runtime image).
    """
    # Each block: [pooled_value, pooled_weight, span]. Merge left while the
    # previous block's mean exceeds the current one (a down-step violation).
    blocks: list[list[float]] = []
    for v, w in zip(values, weights):
        blocks.append([v, w, 1])
        while len(blocks) > 1 and blocks[-2][0] > blocks[-1][0]:
            v2, w2, l2 = blocks.pop()
            v1, w1, l1 = blocks.pop()
            nw = w1 + w2
            nv = (v1 * w1 + v2 * w2) / nw if nw > 0 else (v1 + v2) / 2.0
            blocks.append([nv, nw, l1 + l2])

    fitted: list[float] = []
    for v, _w, span in blocks:
        fitted.extend([v] * int(span))
    return fitted


# ---------------------------------------------------------------------------
# Fitting
# ---------------------------------------------------------------------------

def _bin_index(p: float) -> Optional[int]:
    if p < _BIN_LO or p >= _BIN_HI:
        return None
    return int((p - _BIN_LO) / _BIN_WIDTH)


def _bin_center(idx: int) -> float:
    return _BIN_LO + (idx + 0.5) * _BIN_WIDTH


def _fit_cell(pairs: list[tuple[float, int]]) -> Optional[dict]:
    """Fit one isotonic curve from (raw_prob, outcome) pairs.

    Returns {"n": int, "knots": [[x, y], ...], "trusted": bool} or None when
    the cell has too few observations to bin at all. `trusted` reflects the
    MIN_CELL_OBS / MIN_BINS_SPANNED gates; untrusted cells are kept for
    diagnostics but not applied.
    """
    n = len(pairs)
    if n == 0:
        return None

    # Accumulate hit count + total per bin.
    hits: dict[int, int] = {}
    tot: dict[int, int] = {}
    for p, o in pairs:
        idx = _bin_index(p)
        if idx is None:
            continue
        tot[idx] = tot.get(idx, 0) + 1
        hits[idx] = hits.get(idx, 0) + (1 if o else 0)

    # Ordered, sufficiently-populated bins only.
    idxs = sorted(i for i in tot if tot[i] >= _MIN_BIN_OBS)
    if not idxs:
        return None

    xs = [_bin_center(i) for i in idxs]
    rates = [hits[i] / tot[i] for i in idxs]
    wts = [float(tot[i]) for i in idxs]

    fitted = _pava(rates, wts)
    knots = [[round(x, 4), round(max(0.001, min(0.999, y)), 4)]
             for x, y in zip(xs, fitted)]

    trusted = (n >= MIN_CELL_OBS) and (len(knots) >= MIN_BINS_SPANNED)
    return {"n": n, "knots": knots, "trusted": trusted}


def _load_settled_rows() -> list[dict]:
    """Pull settled `market_observatory` rows (raw_true_prob + outcome) for
    the fit window. Uses the service-role handle like the correlation fit;
    the observatory is public-read anyway."""
    from engine.database import get_db  # lazy: avoid import cycle at load
    db = get_db()
    if not db:
        return []

    cutoff_iso = (datetime.now(timezone.utc)
                  - timedelta(days=_FIT_WINDOW_DAYS)).isoformat()
    try:
        rows: list = []
        page_size = 1000
        offset = 0
        while True:
            page = (
                db.table("market_observatory")
                  .select("league, side, raw_true_prob, true_prob, result, game_start")
                  .in_("result", ["hit", "miss"])
                  .gte("game_start", cutoff_iso)
                  .order("game_start", desc=False)
                  .range(offset, offset + page_size - 1)
                  .execute()
                  .data
            ) or []
            rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return rows
    except Exception as exc:
        logger.warning("Calibration map: observatory query failed: %s", exc)
        return []


def fit_calibration_map() -> Optional[dict]:
    """Fit per-(league, side) and a pooled global isotonic map from settled
    observatory rows, persist as JSON (+ Supabase mirror), and return the
    payload. Called hourly by the scheduler. Returns None when there is no
    settled data yet.

    The fit reads `raw_true_prob` (the pre-correction consensus — the one
    ruler). Legacy rows written before migration_006 have NULL raw_true_prob;
    those pre-date the split and stored the calibrated value in true_prob, so
    we COALESCE to true_prob exactly as the SIDE_BIAS refit does.
    """
    from engine.constants import is_excluded_league

    rows = _load_settled_rows()
    if not rows:
        logger.info("Calibration map fit: no settled observatory rows yet.")
        return None

    by_cell: dict[tuple[str, str], list[tuple[float, int]]] = {}
    pooled: list[tuple[float, int]] = []
    for r in rows:
        league = (r.get("league") or "").upper()
        if is_excluded_league(league):
            continue
        side = (r.get("side") or "").lower()
        raw = r.get("raw_true_prob")
        if raw is None:
            raw = r.get("true_prob")
        try:
            p = float(raw)
        except (TypeError, ValueError):
            continue
        if p <= 0.0 or p >= 1.0:
            continue
        outcome = 1 if r.get("result") == "hit" else 0
        rec = (p, outcome)
        by_cell.setdefault((league, side), []).append(rec)
        pooled.append(rec)

    cells: dict[str, dict] = {}
    for (league, side), pairs in by_cell.items():
        fit = _fit_cell(pairs)
        if fit is not None:
            cells[f"{league}|{side}"] = fit

    global_fit = _fit_cell(pooled)

    payload = {
        "version": 1,
        "fitted_at": datetime.now(timezone.utc).isoformat(),
        "window_days": _FIT_WINDOW_DAYS,
        "cells": cells,
        "global": global_fit,
    }

    # Persist to disk + Supabase mirror (same pattern as the correlation map).
    try:
        os.makedirs(os.path.dirname(CALIBRATION_FILE) or ".", exist_ok=True)
        import json
        with open(CALIBRATION_FILE, "w") as f:
            json.dump(payload, f, indent=2)
    except Exception as exc:
        logger.warning("Calibration map: disk write failed: %s", exc)

    try:
        from engine.persistence import sync_state_to_supabase
        sync_state_to_supabase("calibration_map", payload)
    except Exception as exc:
        logger.warning("Calibration map: Supabase mirror failed: %s", exc)

    n_trusted = sum(1 for c in cells.values() if c.get("trusted"))
    logger.info(
        "Calibration map fit: %d cells (%d trusted), global n=%s",
        len(cells), n_trusted,
        (global_fit or {}).get("n"),
    )
    return payload


# ---------------------------------------------------------------------------
# Load / apply
# ---------------------------------------------------------------------------

def load_calibration_map() -> dict:
    """Read the persisted map from disk (or the newer Supabase mirror) into
    the module dict used by apply_calibration()."""
    try:
        from engine.persistence import load_artefact
        payload = load_artefact(
            "calibration_map", CALIBRATION_FILE,
            validator=lambda p: isinstance(p, dict) and "cells" in p,
        )
    except Exception as exc:
        logger.warning("Calibration map load: %s", exc)
        payload = None
    return payload or {}


def reload_calibration_map() -> int:
    """Refresh the module-level map from disk after a fit. Returns the number
    of trusted cells (for the scheduler log)."""
    global _map
    _map = load_calibration_map()
    cells = _map.get("cells", {}) or {}
    return sum(1 for c in cells.values() if c.get("trusted"))


def _interp(knots: list[list[float]], p: float) -> float:
    """Piecewise-linear interpolation of the isotonic knots at raw prob `p`,
    clamped flat beyond the fitted range (we never extrapolate a monotone
    curve past the data that earned it)."""
    if not knots:
        return p
    if p <= knots[0][0]:
        return knots[0][1]
    if p >= knots[-1][0]:
        return knots[-1][1]
    # Linear scan is fine — a cell has at most ~48 knots.
    for i in range(1, len(knots)):
        x0, y0 = knots[i - 1]
        x1, y1 = knots[i]
        if p <= x1:
            if x1 == x0:
                return y1
            t = (p - x0) / (x1 - x0)
            return y0 + t * (y1 - y0)
    return knots[-1][1]


def apply_calibration(raw_prob: float, league: str, side: str) -> Optional[float]:
    """Map a raw consensus prob onto the realized-rate curve for its cell.

    Returns the calibrated probability, or None when there is no TRUSTED fit
    for the cell (and no trusted global fit) — the caller then falls back to
    the SIDE_BIAS path. Prefers the per-cell curve; uses the pooled global
    curve when the cell itself isn't trusted but the global fit is.
    """
    if not _map:
        return None
    key = f"{(league or '').upper()}|{(side or '').lower()}"
    cell = (_map.get("cells", {}) or {}).get(key)
    if cell and cell.get("trusted") and cell.get("knots"):
        return max(0.001, min(0.999, _interp(cell["knots"], raw_prob)))
    g = _map.get("global")
    if g and g.get("trusted") and g.get("knots"):
        return max(0.001, min(0.999, _interp(g["knots"], raw_prob)))
    return None


# Auto-load at import so the first request / unit tests already see whatever
# has been persisted (matches engine.correlation).
_map = load_calibration_map()

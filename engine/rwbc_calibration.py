"""
RWBC — Reliability-Weighted Bayesian Calibration.

Drop-in alternative to engine/isotonic_calibration.py, fit per
(league, prop, side) cell using Walsh & Joshi's Brier decomposition:

    Resolution  R = Σ_b (n_b · (mean_pred_b − mean_pred_cell)²) / n_cell
    Reliability E = Σ_b (n_b · (mean_pred_b − mean_obs_b   )²) / n_cell
    Trust weight w_cell = R / (R + E + ε)   ∈ [0, 1]

    p_calibrated = w_cell · p_model + (1 − w_cell) · p_cell_posterior

where p_cell_posterior is the Beta-Binomial shrunk empirical hit rate of
the cell (prior anchored on the global hit rate). When w_cell collapses
below W_CELL_HALT_THRESHOLD the cell is "untradeable" — calibrate()
returns None and the auto-backtester filters those legs out of the pool.

Three production hardening properties:

  (1) Hard circuit breaker (Gemini #1). Cells with no resolution can't
      be saved by Beta-Binomial smoothing — they get None, not a flat
      population mean. Same contract for cells that have never been seen.
  (2) Unambiguous bucket-key semantics (Gemini #2). The `prob_bucket`
      column on the observation frame is the 5pp prob-prediction bin
      WITHIN a cell, NOT the cell identifier. Runtime assertion below.
  (3) Publication gate (Gemini #3). New cell rows only overwrite the
      persisted row when Δn_eff ≥ N_PUBLISH_GATE since the last publish.
      Prevents thin slates from whipping w_cell around hour to hour.

Persistence: state lives in the calibration_cells table (migration_010).
Apply-path reads a process-local _cell_cache dict that's refreshed by
load_cell_cache_from_db(). Refit job writes through publish_cell().
"""
from __future__ import annotations

import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────
# Constants — see plan locked decisions 4-9
# ─────────────────────────────────────────────────────────────────────────

# Beta-Binomial prior strength on the cell empirical mean. 30 ≈ "the
# prior is worth ~30 observations worth of evidence." Cells with n_eff
# below this get strong shrinkage toward the global hit rate; cells
# with n_eff ≫ 30 essentially ignore the prior.
GLOBAL_PRIOR_STRENGTH = 30.0

# Circuit-breaker threshold. Cells below this are untradeable.
# Determined empirically (probe 06): w_cell of the 4 known-bad cells
# (NBA UNDER, NHL OVER, NHL UNDER, WNBA UNDER) all sit below 0.15.
# 0.20 is a small safety buffer above that floor.
W_CELL_HALT_THRESHOLD = 0.20

# Minimum new effective observations required since the last publish
# before a cell row gets overwritten. Smooths daily slate variance —
# without this, a Sunday MLB slate of ~180 obs would whip the weights
# every refit. With 60-day half-life + 50-obs gate, a single cell
# typically publishes 2-5x per week.
N_PUBLISH_GATE = 50

# Width of the within-cell prob bin used for Brier resolution / reliability
# decomposition. 5pp matches the existing Observatory chart and gives
# enough buckets in mid-prob region (0.30-0.70) without thinning to
# single-row buckets in the tails.
BUCKET_WIDTH = 0.05

# Numerical floor in the w_cell denominator. Prevents 0/0 in cells with
# both R=0 and E=0 (which would mean every prediction in the cell was
# both identical AND perfectly equal to the realized rate — vanishingly
# unlikely but cheap to guard).
EPS = 1e-6


# ─────────────────────────────────────────────────────────────────────────
# Process-local cache + last-halt-log timestamp dict
# ─────────────────────────────────────────────────────────────────────────

# (league, prop, side) → CellStats
_cell_cache: dict[tuple[str, str, str], "CellStats"] = {}
_cache_lock = threading.Lock()

# Per-cell timestamp of last "RWBC.halt" log line emission. Prevents log
# spam — on a heavy slate we'd otherwise log 1 line per leg per cell
# every cycle. Limit to 1 emission per cell per refit window (~1 hour).
_last_halt_log_at: dict[tuple[str, str, str], float] = {}
_HALT_LOG_INTERVAL_SEC = 3600


@dataclass(frozen=True)
class CellStats:
    """Per-cell calibration snapshot. Frozen so it can be passed by
    reference without surprise mutation downstream."""
    league: str
    prop: str
    side: str
    w_cell: float
    p_post: float
    n_eff: float
    resolution: float
    reliability_error: float
    mean_pred: float
    mean_obs: float
    last_fit_at: datetime
    last_publish_at: datetime
    last_publish_n_eff: float


# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────

def bucket_of(p: float, width: float = BUCKET_WIDTH) -> float:
    """Round p into a fixed-width bin center. Mirrors analysis/common.py."""
    lo = int(p / width) * width
    return round(lo + width / 2, 4)


def _normalize_side(side: str | None) -> str:
    return (side or "").strip().lower()


# ─────────────────────────────────────────────────────────────────────────
# Fit
# ─────────────────────────────────────────────────────────────────────────

def fit_one_cell(
    observations: list[dict],
    global_hit_rate: float,
) -> dict | None:
    """Fit a single (league, prop, side) cell.

    `observations` is a list of dicts with at minimum:
        {"pred": float, "y": 0|1, "weight": float}

    Optionally "prob_bucket" may be precomputed; otherwise it's derived
    from pred via bucket_of().

    Returns a dict with the cell's RWBC stats, or None if the cell is
    too thin to fit (n_eff < 5 or only one prob bucket).
    """
    if not observations:
        return None

    # Materialize once.
    preds  = [float(o["pred"])   for o in observations]
    ys     = [float(o["y"])      for o in observations]
    ws     = [float(o["weight"]) for o in observations]
    bucks  = [
        float(o.get("prob_bucket", bucket_of(p)))
        for o, p in zip(observations, preds)
    ]

    n_eff = sum(ws)
    if n_eff < 5.0:
        return None

    # ── Hard precondition (Gemini #2). The bucket field MUST be the prob
    # bin within the cell. If every observation lands in the same bin,
    # resolution will be 0 and w_cell will collapse — that's a real
    # signal in itself ("cell has no within-cell discrimination"), so
    # we don't error out, but we DO refuse to publish a w_cell on it
    # below.
    distinct_buckets = len(set(bucks))

    mean_pred = sum(p * w for p, w in zip(preds, ws)) / n_eff

    # Bucket-level rollup. Manual two-pass groupby because we don't want
    # to depend on pandas at runtime — this module is import-light by
    # design (called from the auto-backtest hot path).
    bucket_n: dict[float, float] = {}
    bucket_wp: dict[float, float] = {}
    bucket_wy: dict[float, float] = {}
    for b, p, y, w in zip(bucks, preds, ys, ws):
        bucket_n[b]  = bucket_n.get(b,  0.0) + w
        bucket_wp[b] = bucket_wp.get(b, 0.0) + w * p
        bucket_wy[b] = bucket_wy.get(b, 0.0) + w * y

    res = 0.0
    rel = 0.0
    for b, n_b in bucket_n.items():
        pred_b = bucket_wp[b] / n_b
        obs_b  = bucket_wy[b] / n_b
        res += n_b * (pred_b - mean_pred) ** 2
        rel += n_b * (pred_b - obs_b)     ** 2
    res /= n_eff
    rel /= n_eff

    # With a single bucket, resolution is mechanically zero — w_cell would
    # collapse from the math, not from genuine model failure. Avoid the
    # false-positive halt by leaving w_cell undefined in that case.
    if distinct_buckets < 2:
        w_cell = 0.0
    else:
        w_cell = max(0.0, min(1.0, res / (res + rel + EPS)))

    # Beta-Binomial posterior on cell empirical mean.
    k_eff = sum(y * w for y, w in zip(ys, ws))
    alpha0 = global_hit_rate * GLOBAL_PRIOR_STRENGTH
    beta0  = (1.0 - global_hit_rate) * GLOBAL_PRIOR_STRENGTH
    p_post = (k_eff + alpha0) / (n_eff + alpha0 + beta0)

    return {
        "w_cell": float(w_cell),
        "p_post": float(p_post),
        "n_eff":  float(n_eff),
        "resolution":        float(res),
        "reliability_error": float(rel),
        "mean_pred": float(mean_pred),
        "mean_obs":  float(k_eff / n_eff),
        "distinct_buckets": int(distinct_buckets),
    }


def fit_all_cells(
    observations_by_cell: dict[tuple[str, str, str], list[dict]],
    global_hit_rate: float,
) -> dict[tuple[str, str, str], dict]:
    """Fit every (league, prop, side) cell from a dict-of-lists input.

    Caller groups observations into the cell-keyed dict and supplies the
    global hit rate (computed once from the pooled observation stream).
    Returns a dict of fits, ready to hand to publish_cell() under the
    publication gate.
    """
    out: dict[tuple[str, str, str], dict] = {}
    for key, obs in observations_by_cell.items():
        fit = fit_one_cell(obs, global_hit_rate)
        if fit is None:
            continue
        out[key] = fit
        logger.info(
            "RWBC.fit         scope=%s|%s|%s n_eff=%.1f w_cell=%.3f p_post=%.3f res=%.5f rel=%.5f",
            key[0], key[1], key[2],
            fit["n_eff"], fit["w_cell"], fit["p_post"],
            fit["resolution"], fit["reliability_error"],
        )
    return out


# ─────────────────────────────────────────────────────────────────────────
# Persistence — publish with gate
# ─────────────────────────────────────────────────────────────────────────

def publish_if_delta_n_exceeded(
    db,
    fits: dict[tuple[str, str, str], dict],
    *,
    min_delta: float = N_PUBLISH_GATE,
) -> tuple[int, int]:
    """Apply the Δn_eff publication gate (Gemini #3) and upsert passing
    cells into calibration_cells. Returns (n_published, n_skipped).
    """
    if db is None:
        logger.warning("RWBC.publish skipped: no db client")
        return (0, 0)

    # Pull current persisted n_eff for every cell we're about to consider,
    # in a single query, so we don't do len(fits) round-trips.
    existing: dict[tuple[str, str, str], float] = {}
    try:
        res = (
            db.table("calibration_cells")
              .select("league, prop, side, last_publish_n_eff")
              .execute()
        )
        for row in (res.data or []):
            existing[(row["league"], row["prop"], row["side"])] = float(
                row.get("last_publish_n_eff") or 0.0
            )
    except Exception as exc:
        logger.warning("RWBC.publish: could not read existing cells (%s); "
                       "treating all as new", exc)

    now_iso = datetime.now(timezone.utc).isoformat()
    rows_to_upsert: list[dict] = []
    n_published = 0
    n_skipped   = 0

    for key, fit in fits.items():
        prior_n = existing.get(key, 0.0)
        delta_n = fit["n_eff"] - prior_n
        if key in existing and delta_n < min_delta:
            n_skipped += 1
            logger.info(
                "RWBC.publish     scope=%s|%s|%s published=false reason=delta_n_eff_below_gate "
                "delta=%.1f gate=%.1f",
                key[0], key[1], key[2], delta_n, min_delta,
            )
            continue
        rows_to_upsert.append({
            "league":              key[0],
            "prop":                key[1],
            "side":                key[2],
            "w_cell":              fit["w_cell"],
            "p_post":              fit["p_post"],
            "n_eff":               fit["n_eff"],
            "resolution":          fit["resolution"],
            "reliability_error":   fit["reliability_error"],
            "mean_pred":           fit["mean_pred"],
            "mean_obs":            fit["mean_obs"],
            "last_fit_at":         now_iso,
            "last_publish_at":     now_iso,
            "last_publish_n_eff":  fit["n_eff"],
        })
        n_published += 1
        logger.info(
            "RWBC.publish     scope=%s|%s|%s published=true delta_n_eff=%.1f",
            key[0], key[1], key[2], delta_n,
        )

    if rows_to_upsert:
        try:
            (db.table("calibration_cells")
               .upsert(rows_to_upsert, on_conflict="league,prop,side")
               .execute())
        except Exception as exc:
            logger.error("RWBC.publish: upsert failed: %s", exc)
            return (0, n_skipped + n_published)

    return (n_published, n_skipped)


def record_history(
    db,
    *,
    scope: str,
    brier_current: float | None,
    brier_rwbc: float | None,
    n_settled: int,
    publish_skipped: bool,
) -> None:
    """Append one row to calibration_history — feeds the trend panel."""
    if db is None:
        return
    try:
        (db.table("calibration_history")
           .insert({
               "scope":           scope,
               "brier_current":   brier_current,
               "brier_rwbc":      brier_rwbc,
               "n_settled":       int(n_settled),
               "publish_skipped": bool(publish_skipped),
           })
           .execute())
    except Exception as exc:
        logger.warning("RWBC.history: insert failed for %s: %s", scope, exc)


# ─────────────────────────────────────────────────────────────────────────
# Cache load + inference path
# ─────────────────────────────────────────────────────────────────────────

def load_cell_cache_from_db(db) -> int:
    """Replace the process-local cache from calibration_cells. Returns
    the number of cells loaded. Safe to call repeatedly (e.g. once per
    refit completion, or on app startup).
    """
    global _cell_cache
    if db is None:
        return 0
    try:
        res = db.table("calibration_cells").select("*").execute()
    except Exception as exc:
        logger.error("RWBC.cache: load failed (table may not exist yet? "
                     "migration_010): %s", exc)
        return 0

    new_cache: dict[tuple[str, str, str], CellStats] = {}
    for row in (res.data or []):
        try:
            new_cache[(row["league"], row["prop"], row["side"])] = CellStats(
                league=row["league"], prop=row["prop"], side=row["side"],
                w_cell=float(row["w_cell"]),
                p_post=float(row["p_post"]),
                n_eff=float(row["n_eff"]),
                resolution=float(row["resolution"]),
                reliability_error=float(row["reliability_error"]),
                mean_pred=float(row["mean_pred"]),
                mean_obs=float(row["mean_obs"]),
                last_fit_at=_parse_ts(row.get("last_fit_at")),
                last_publish_at=_parse_ts(row.get("last_publish_at")),
                last_publish_n_eff=float(row["last_publish_n_eff"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("RWBC.cache: skipping malformed row %s: %s", row, exc)
            continue

    with _cache_lock:
        _cell_cache = new_cache
    logger.info("RWBC.cache: loaded %d cells", len(new_cache))
    return len(new_cache)


def _parse_ts(s: str | None) -> datetime:
    if not s:
        return datetime.now(timezone.utc)
    try:
        # Supabase returns ISO with trailing Z or +00:00
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


def calibrate(
    p_model: float,
    league: str | None,
    prop: str | None,
    side: str | None,
) -> float | None:
    """Apply RWBC to a single leg. Returns None when the cell is
    untradeable per the circuit breaker (Gemini #1).

    `None` is the explicit "skip this leg" signal that flows up to
    engine/ev_calculator.py and then to web/app.py's auto-backtest
    worker, which filters None-prob legs out of the slip pool.
    """
    side_n = _normalize_side(side)
    if not league or not prop or side_n not in ("over", "under"):
        return None
    key = (league, prop, side_n)
    with _cache_lock:
        c = _cell_cache.get(key)
    if c is None:
        # Brand-new cell with no historical evidence. Cold-start contract:
        # treat as untradeable until next refit folds in resolved obs.
        return None
    if c.w_cell < W_CELL_HALT_THRESHOLD:
        _maybe_log_halt(key, c.w_cell)
        return None
    p_cal = c.w_cell * p_model + (1.0 - c.w_cell) * c.p_post
    # Numerical guard mirroring the existing isotonic path so downstream
    # log-loss / EV math doesn't blow up on edges.
    return max(0.001, min(0.999, p_cal))


def _maybe_log_halt(key: tuple[str, str, str], w_cell: float) -> None:
    """Emit one RWBC.halt log per cell per ~hour so the log doesn't
    explode on heavy slates."""
    import time
    now = time.time()
    last = _last_halt_log_at.get(key, 0.0)
    if now - last < _HALT_LOG_INTERVAL_SEC:
        return
    _last_halt_log_at[key] = now
    logger.info(
        "RWBC.halt        league=%s prop=%s side=%s w_cell=%.3f reason=below_halt_threshold",
        key[0], key[1], key[2], w_cell,
    )


def cache_stats() -> dict:
    """Diagnostic snapshot for /api/observatory/rwbc."""
    with _cache_lock:
        cells = list(_cell_cache.values())
    total = len(cells)
    halted = sum(1 for c in cells if c.w_cell < W_CELL_HALT_THRESHOLD)
    return {
        "n_cells":         total,
        "n_halted":        halted,
        "n_active":        total - halted,
        "mean_w_cell":     (sum(c.w_cell for c in cells) / total) if total else 0.0,
        "halt_threshold":  W_CELL_HALT_THRESHOLD,
        "publish_gate":    N_PUBLISH_GATE,
    }


def all_cells() -> list[CellStats]:
    """Snapshot of every cell for the Observatory heatmap."""
    with _cache_lock:
        return list(_cell_cache.values())

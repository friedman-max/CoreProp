"""
Empirical sharpness-weight calibration for the consensus engine.

The consensus engine's per-book weights (`SHARPNESS_WEIGHTS` in
`engine/consensus.py`) are hardcoded — FanDuel:1.20, Pinnacle:0.95,
DraftKings:0.90. This module re-fits those weights from observed data so a
book's influence reflects its actual price-discovery quality on this
platform's market mix, not a global prior.

Signal: closing line value. If a book's devigged probability at log time is
consistently close to the eventual closing probability, that book is sharp
and should carry more weight in consensus. We measure this directly via the
mean squared error of (book_devigged_prob − closing_prob) per book.

Mapping MSE → weight:
    err_b   = sqrt(MSE_b)                      # book's typical CLV error
    score_b = 1 / max(err_b, EPSILON)          # inverse: sharper is bigger
    weight_b = score_b / mean(scores) * BASE   # rescale around 1.0

Books with fewer than `MIN_BOOK_OBS` observations fall back to the hardcoded
weight so we don't refit on noise. The output JSON lives next to the
isotonic curves and is loaded by `consensus._get_sharpness_weight()`.

Forward-compatible: this module reads the `books` JSONB column added by
`migration_003.sql`. If the column doesn't exist or no rows have data
populated yet, the module returns None and the consensus engine keeps the
hardcoded defaults.
"""
from __future__ import annotations

import os
import json
import math
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from engine.database import get_db

logger = logging.getLogger(__name__)

SHARPNESS_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "sharpness_weights.json")

# Below this many CLV observations for a book, we don't trust the empirical
# weight and fall back to the hardcoded prior. ~150 gives RMSE std error of
# roughly 0.005 on a typical book.
MIN_BOOK_OBS = 150

# Recency half-life. Same intuition as the calibration recency: recent CLV
# residuals reflect current market behavior.
SHARPNESS_HALF_LIFE_DAYS = 60.0

# Hard date cutoff to keep the table scan bounded on the 512 MB tier; older
# rows fall well below the half-life weight floor anyway.
SHARPNESS_LOOKBACK_DAYS = 180

# Output weight scale. The consensus engine treats raw weights multiplicatively
# in a VWAP-style average; the absolute scale doesn't matter for the consensus
# output, but keeping the mean near 1 keeps the values intuitive.
TARGET_MEAN_WEIGHT = 1.0

# Numerical floor on the per-book error used in the inverse mapping.
EPSILON = 0.01


_LN2 = math.log(2.0)


def _recency_weight(observation_dt: datetime | None, now: datetime) -> float:
    """True half-life decay: weight = 0.5 at Δ=HALF_LIFE."""
    if observation_dt is None:
        return 1.0
    delta_days = max(0.0, (now - observation_dt).total_seconds() / 86400.0)
    return math.exp(-delta_days * _LN2 / SHARPNESS_HALF_LIFE_DAYS)


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00") if isinstance(s, str) else s
        dt = datetime.fromisoformat(s) if isinstance(s, str) else s
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def update_sharpness_weights() -> dict | None:
    """
    Refit empirical book sharpness weights from market_observatory CLV data
    and persist them to disk.

    No-ops cleanly when the `books` column doesn't exist yet (migration_003
    not applied) or when no rows have per-book data populated.
    """
    db = get_db()
    if not db:
        return None

    now = datetime.now(timezone.utc)

    cutoff_iso = (now - timedelta(days=SHARPNESS_LOOKBACK_DAYS)).isoformat()
    try:
        res = (
            db.table("market_observatory")
            .select("books, closing_prob, game_start")
            .not_.is_("closing_prob", "null")
            .gte("game_start", cutoff_iso)
            .execute()
        )
        rows = res.data or []
    except Exception as exc:
        logger.debug("Sharpness: market_observatory load failed (migration_003 may be missing): %s", exc)
        return None

    if not rows:
        logger.info("Sharpness: no rows with per-book + closing_prob data yet.")
        return None

    # Per-book accumulators: weighted MSE.
    sum_w_err: dict[str, float] = defaultdict(float)   # Σ (w · (book_p − closing_p)²)
    sum_w:     dict[str, float] = defaultdict(float)   # Σ w
    n_obs:     dict[str, int]   = defaultdict(int)

    for r in rows:
        try:
            cp = float(r.get("closing_prob"))
        except (TypeError, ValueError):
            continue
        if not (0.0 < cp < 1.0):
            continue
        books = r.get("books") or {}
        if not isinstance(books, dict) or not books:
            continue
        ts = _parse_dt(r.get("game_start"))
        w = _recency_weight(ts, now)
        if w <= 0:
            continue

        for book_name, book_p in books.items():
            try:
                bp = float(book_p)
            except (TypeError, ValueError):
                continue
            if not (0.0 < bp < 1.0):
                continue
            err_sq = (bp - cp) ** 2
            sum_w_err[book_name] += w * err_sq
            sum_w[book_name]     += w
            n_obs[book_name]     += 1

    eligible = {b: n_obs[b] for b in n_obs if n_obs[b] >= MIN_BOOK_OBS}
    if not eligible:
        logger.info(
            "Sharpness: no book reached %d observations yet (max=%d).",
            MIN_BOOK_OBS, max(n_obs.values()) if n_obs else 0,
        )
        return None

    # Compute per-book MSE → inverse-RMSE score → rescaled weight.
    rmse: dict[str, float] = {}
    score: dict[str, float] = {}
    for b in eligible:
        mse = sum_w_err[b] / sum_w[b] if sum_w[b] > 0 else None
        if mse is None or mse < 0:
            continue
        rmse[b] = math.sqrt(mse)
        score[b] = 1.0 / max(rmse[b], EPSILON)

    if not score:
        return None

    mean_score = sum(score.values()) / len(score)
    weights = {b: round((s / mean_score) * TARGET_MEAN_WEIGHT, 4) for b, s in score.items()}

    out = {
        "version":   1,
        "fitted_at": now.isoformat(),
        "config": {
            "min_book_obs":             MIN_BOOK_OBS,
            "sharpness_half_life_days": SHARPNESS_HALF_LIFE_DAYS,
        },
        "weights":   weights,
        "diagnostics": {
            b: {"n_obs": int(n_obs[b]), "rmse": round(rmse[b], 5), "weight": weights[b]}
            for b in weights
        },
    }

    try:
        os.makedirs(os.path.dirname(SHARPNESS_FILE), exist_ok=True)
        with open(SHARPNESS_FILE, "w") as f:
            json.dump(out, f, indent=2)
    except Exception as exc:
        logger.error("Sharpness: write failed: %s", exc)
        return None

    logger.info(
        "Sharpness: refit weights for %d books — %s",
        len(weights),
        ", ".join(f"{b}={w:.2f}" for b, w in sorted(weights.items())),
    )
    return out


def load_sharpness_weights() -> dict[str, float]:
    """
    Load the empirical per-book weights, or an empty dict if no fit is
    available. Callers should fall back to the hardcoded prior in that case.
    """
    if not os.path.exists(SHARPNESS_FILE):
        return {}
    try:
        with open(SHARPNESS_FILE, "r") as f:
            raw = json.load(f)
        weights = raw.get("weights") or {}
        return {str(k).lower(): float(v) for k, v in weights.items()}
    except Exception:
        return {}

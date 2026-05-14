"""
Rolling Brier-score monitor for the calibration pipeline.

The hierarchical isotonic calibrator can only help if the calibrated
probability is a better predictor of empirical hit rate than the raw
consensus it's correcting. The monitor checks this directly: every
hourly refit, we compute Brier scores over the last MONITOR_WINDOW_DAYS
of resolved rows on both `raw_true_prob` and `true_prob`, log a warning
if calibrated is worse than raw, and persist the last MONITOR_HISTORY
runs so the Observatory can show a time-series of the diagnostic.

Brier score = mean( (predicted_prob - outcome)² ).  Lower is better.
A calibrated value that's worse on Brier than raw is a sign that the
calibrator is overfitting noise (or the apply path has a bug). Both
should be investigated before acting on +EV signals — at that point
the system would have a positive expected loss on a randomly sampled
bet.
"""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta, timezone

from engine.database import get_db

logger = logging.getLogger(__name__)

MONITOR_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "brier_monitor.json")

# Rolling window. Long enough to suppress weekday noise, short enough that
# a regression shows up within a day or two of when it started.
MONITOR_WINDOW_DAYS = 7

# Keep the last N monitor snapshots on disk so the Observatory can plot
# the trend rather than just the latest value.
MONITOR_HISTORY = 168  # one week of hourly runs

# Below this many resolved observations we don't compute a meaningful
# Brier score; the snapshot is still recorded as `n_obs < min` so the
# Observatory can show "warming up".
MIN_OBS = 50


def _brier(rows: list[tuple[float, int]]) -> float | None:
    """Brier score over (prob, outcome) pairs."""
    if not rows:
        return None
    n = len(rows)
    return sum((p - y) ** 2 for p, y in rows) / n


def _pull_recent_resolved(window_days: int) -> list[dict]:
    """Pull resolved (raw_true_prob, true_prob, result) triples from the
    market_observatory table over the last `window_days`. Falls back to
    the legacy column set if migration_006 hasn't been applied — in
    which case the monitor will report identical raw and calibrated
    Brier scores until new rows accumulate."""
    db = get_db()
    if not db:
        return []

    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat()
    page_size = 1000
    rows: list[dict] = []
    offset = 0
    cols_v2 = "raw_true_prob, true_prob, result, resolved_at"
    cols_v1 = "true_prob, result, resolved_at"
    cols = cols_v2
    while True:
        try:
            q = (db.table("market_observatory")
                   .select(cols)
                   .gte("resolved_at", cutoff_iso)
                   .in_("result", ["hit", "miss", "won", "lost", "win", "loss"])
                   .order("resolved_at", desc=False)
                   .range(offset, offset + page_size - 1))
            res = q.execute()
        except Exception as exc:
            if cols == cols_v2:
                # raw_true_prob column probably missing — retry with legacy.
                cols = cols_v1
                continue
            logger.warning("BrierMonitor: pull failed: %s", exc)
            return rows
        chunk = res.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
    return rows


def _coerce_outcome(result: str | None) -> int | None:
    r = (result or "").lower()
    if r in ("hit", "won", "win"):
        return 1
    if r in ("miss", "lost", "loss"):
        return 0
    return None


def compute_brier_snapshot(window_days: int = MONITOR_WINDOW_DAYS) -> dict:
    """
    Compute Brier scores for raw and calibrated probabilities over the
    last `window_days`. Returns a snapshot dict suitable for persistence
    and for the Observatory UI:

        {
          "fitted_at":      ISO8601,
          "window_days":    int,
          "n_obs":          int,
          "brier_raw":      float | None,
          "brier_calibrated": float | None,
          "delta":          float | None,   # raw - calibrated (positive = calibrated helped)
          "status":         "ok" | "warming_up" | "regression",
        }
    """
    raw_rows = _pull_recent_resolved(window_days)
    pairs_raw: list[tuple[float, int]] = []
    pairs_cal: list[tuple[float, int]] = []
    for r in raw_rows:
        y = _coerce_outcome(r.get("result"))
        if y is None:
            continue

        # Calibrated probability: the value the model used to act on.
        try:
            tp = float(r.get("true_prob"))
        except (TypeError, ValueError):
            tp = None

        # Raw probability: pre-calibration consensus. Falls back to true_prob
        # for legacy rows where the column wasn't yet populated — in that
        # case the two numbers are identical and the delta will be zero.
        rp_val = r.get("raw_true_prob") if "raw_true_prob" in r else None
        try:
            rp = float(rp_val) if rp_val is not None else tp
        except (TypeError, ValueError):
            rp = tp

        if tp is not None and 0.0 < tp < 1.0:
            pairs_cal.append((tp, y))
        if rp is not None and 0.0 < rp < 1.0:
            pairs_raw.append((rp, y))

    n = len(pairs_cal)
    snapshot = {
        "fitted_at":         datetime.now(timezone.utc).isoformat(),
        "window_days":       window_days,
        "n_obs":             n,
        "brier_raw":         None,
        "brier_calibrated":  None,
        "delta":             None,
        "status":            "warming_up" if n < MIN_OBS else "ok",
    }

    if n >= MIN_OBS:
        b_raw = _brier(pairs_raw)
        b_cal = _brier(pairs_cal)
        snapshot["brier_raw"] = round(b_raw, 6) if b_raw is not None else None
        snapshot["brier_calibrated"] = round(b_cal, 6) if b_cal is not None else None
        if b_raw is not None and b_cal is not None:
            snapshot["delta"] = round(b_raw - b_cal, 6)
            if b_cal > b_raw:
                snapshot["status"] = "regression"
                logger.warning(
                    "BrierMonitor: calibrated is WORSE than raw over %dd window — "
                    "raw=%.5f calibrated=%.5f Δ=%+.5f n=%d",
                    window_days, b_raw, b_cal, b_raw - b_cal, n,
                )
            else:
                logger.info(
                    "BrierMonitor: raw=%.5f calibrated=%.5f Δ=%+.5f n=%d (calibrator helping)",
                    b_raw, b_cal, b_raw - b_cal, n,
                )

    return snapshot


def _load_history() -> list[dict]:
    try:
        from engine.persistence import load_artefact
        data = load_artefact(
            "brier_monitor", MONITOR_FILE,
            validator=lambda p: isinstance(p, list),
        )
    except Exception:
        data = None
    return data if isinstance(data, list) else []


def _save_history(history: list[dict]) -> None:
    try:
        os.makedirs(os.path.dirname(MONITOR_FILE), exist_ok=True)
        with open(MONITOR_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as exc:
        logger.warning("BrierMonitor: history write failed: %s", exc)
        return
    try:
        from engine.persistence import sync_state_to_supabase
        sync_state_to_supabase("brier_monitor", history)
    except Exception as exc:
        logger.debug("BrierMonitor: Supabase mirror failed: %s", exc)


def update_brier_monitor(window_days: int = MONITOR_WINDOW_DAYS) -> dict:
    """Compute a fresh snapshot, append to the rolling history file, and
    return it. Called by the hourly refit job after the isotonic update
    so each refit is paired with its post-refit Brier check."""
    snapshot = compute_brier_snapshot(window_days)
    history = _load_history()
    history.append(snapshot)
    if len(history) > MONITOR_HISTORY:
        history = history[-MONITOR_HISTORY:]
    _save_history(history)
    return snapshot


def load_brier_history() -> list[dict]:
    """Return the persisted snapshots (most recent last). Used by the
    Observatory diagnostic endpoint. _load_history already prefers the
    newer of disk vs Supabase, so this is just a thin wrapper."""
    return _load_history()

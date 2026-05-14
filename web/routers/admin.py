"""Admin / diagnostic endpoints.

These are operator-only and unauthenticated. They expose no user data —
just process diagnostics (`/memory`) and a manual trigger for the same
refit the hourly scheduler runs (`/refit-calibration`).

`_memory_snapshot()` lives in web/app.py because it inspects state that
spans the whole app (payload cache, analytics cache, dataframes). We
re-export it as a thin dependency rather than duplicate the logic.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/memory")
def get_memory_diagnostics():
    """Diagnostic dump of process memory usage. RSS, peak RSS, payload-
    cache breakdown, pandas DataFrame inventory, GC stats, active thread
    names. Used to find what's eating the 512 MB tier."""
    from web.app import _memory_snapshot   # lazy: avoids import cycle at module load
    return _memory_snapshot()


@router.post("/refit-calibration")
def refit_calibration():
    """Force an immediate refit of all three persisted models (isotonic,
    sharpness, correlation) so production can populate the Observatory
    without waiting for the hourly job. Each refit runs independently —
    failures are reported per-section, not as a 500."""
    result: dict = {"isotonic": None, "sharpness": None, "correlation": None}

    try:
        from engine.isotonic_calibration import update_isotonic_calibration
        from engine.ev_calculator import reload_calibration as _reload_iso
        curves = update_isotonic_calibration()
        if curves:
            _reload_iso()
            result["isotonic"] = {
                "status":       "refit",
                "leagues":      len(curves.get("leagues") or {}),
                "props":        len(curves.get("props") or {}),
                "global_n_eff": (curves.get("global") or {}).get("n_eff"),
                "fitted_at":    curves.get("fitted_at"),
            }
        else:
            result["isotonic"] = {"status": "no-data"}
    except Exception as e:
        logger.error("Manual isotonic refit failed: %s", e)
        result["isotonic"] = {"status": "error", "detail": str(e)}

    try:
        from engine.sharpness_calibration import update_sharpness_weights
        from engine.consensus import reload_sharpness as _reload_sharp
        sharp = update_sharpness_weights()
        if sharp:
            n_books = _reload_sharp()
            result["sharpness"] = {
                "status":    "refit",
                "books":     n_books,
                "fitted_at": sharp.get("fitted_at"),
            }
        else:
            result["sharpness"] = {"status": "no-data"}
    except Exception as e:
        logger.error("Manual sharpness refit failed: %s", e)
        result["sharpness"] = {"status": "error", "detail": str(e)}

    try:
        from engine.correlation import update_correlation_map, reload_correlation, MIN_PAIR_OBS
        corr = update_correlation_map()
        if corr:
            n_trusted = reload_correlation()
            result["correlation"] = {
                "status":      "refit",
                "buckets":     len(corr.get("buckets") or {}),
                "trusted":     n_trusted,
                "min_pair_obs": MIN_PAIR_OBS,
            }
        else:
            result["correlation"] = {"status": "no-data"}
    except Exception as e:
        logger.error("Manual correlation refit failed: %s", e)
        result["correlation"] = {"status": "error", "detail": str(e)}

    return result

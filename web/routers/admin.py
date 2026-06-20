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
    """Force an immediate refit of the correlation map (the only model that
    still refits in simplify-v1) so it can populate without waiting for the
    hourly job. The decision number is the conservative min-across-books
    devig, which has nothing to learn."""
    result: dict = {"correlation": None}

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

"""Admin / diagnostic endpoints.

Operator-only, and now gated behind `require_admin` (X-Admin-Token, failing
closed when ADMIN_TOKEN is unset). They used to be unauthenticated on the
reasoning that they "expose no user data" — but that was too narrow:

  * `/memory` published live process internals to anyone who guessed the
    path: RSS and peak RSS, file-descriptor and thread counts, running
    thread names, the per-dataset payload-cache breakdown, and
    `state_lists`, which is a running count of how many bets and matched
    lines the paid product currently holds. That is competitive
    intelligence and a reconnaissance aid, not neutral telemetry.
  * `/refit-calibration` starts a real refit against the observatory on a
    single-worker 512MB dyno. Unauthenticated, it is a free DoS lever.

`_memory_snapshot()` lives in web/app.py because it inspects state that
spans the whole app (payload cache, analytics cache, dataframes). We
re-export it as a thin dependency rather than duplicate the logic.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from web.auth import require_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.get("/memory")
def get_memory_diagnostics(_admin: bool = Depends(require_admin)):
    """Diagnostic dump of process memory usage. RSS, peak RSS, payload-
    cache breakdown, pandas DataFrame inventory, GC stats, active thread
    names. Used to find what's eating the 512 MB tier."""
    from web.app import _memory_snapshot   # lazy: avoids import cycle at module load
    return _memory_snapshot()


@router.post("/refit-calibration")
def refit_calibration(_admin: bool = Depends(require_admin)):
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

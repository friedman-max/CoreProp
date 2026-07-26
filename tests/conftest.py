"""Shared pytest configuration.

Adds the repo root to sys.path so `import engine.<x>` and `import web.<x>`
work without an installed package. Also provides a single point to set
the env vars production code expects (Supabase URL/keys) so tests can run
offline.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Stub Supabase env so engine.database doesn't trip on import. Tests that
# need a real client patch get_db() instead.
os.environ.setdefault("SUPABASE_URL", "https://test.supabase.invalid")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
os.environ.setdefault("SUPABASE_ANON_KEY", "test-anon-key")

# Quiet APScheduler / httpx during tests.
os.environ.setdefault("PYTHONWARNINGS", "ignore::DeprecationWarning")

# Keep the suite OFFLINE. Entering `TestClient(app)` runs ASGI lifespan events,
# so web/app.py's startup hook would spawn the boot scrape (five live
# third-party APIs), the hourly refit and the CLV recovery pass. That made
# `pytest tests/` do real network I/O — and on a PrizePicks 429 its 10s/30s/90s
# backoff stretched a ~2s suite past two minutes, with CI's shared runner IPs
# hitting 429 routinely. See tests/api_tests/test_startup_jobs_disabled.py.
os.environ.setdefault("COREPROP_DISABLE_STARTUP_JOBS", "1")

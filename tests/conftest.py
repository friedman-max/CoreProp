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

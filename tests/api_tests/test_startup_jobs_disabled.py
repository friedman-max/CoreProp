"""The test suite must not fire the real scrape pipeline.

Entering `TestClient(app)` as a context manager runs ASGI lifespan events, which
runs `web/app.py`'s `@app.on_event("startup")`. Its last two statements spawn
threads unconditionally: `_run_periodic_models` (Supabase refits) and
`run_pipeline` (five live scrapers — PrizePicks, FanDuel, DraftKings, Pinnacle,
Novig). Four fixtures do this, so a plain `pytest tests/` hits third-party APIs.

Two costs, both real:

  * `daemon=True` does NOT save the process. `_run_pipeline_body` submits the
    scrapers to a `concurrent.futures.ThreadPoolExecutor` used as a context
    manager, so `__exit__` calls `shutdown(wait=True)`; on top of that
    `concurrent.futures.thread` registers an atexit hook that joins every
    worker regardless of daemon status. A daemon thread parked in a scraper
    therefore delays interpreter exit.
  * PrizePicks rate-limits by IP and the scraper backs off 10s -> 30s -> 90s
    (`scrapers/prizepicks.py`), so on a 429 the suite stalls for over two
    minutes. CI runs from shared GitHub runner IPs, where 429 is the norm, and
    `.github/workflows/tests.yml` sets `timeout-minutes: 10` — so this turns
    into a red build on an unrelated PR.

`COREPROP_DISABLE_STARTUP_JOBS` suppresses those two side effects only. The
scheduler itself still starts (it merely registers jobs; nothing fires inside a
short test), so this doesn't hollow out what the other tests exercise.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_conftest_sets_the_disable_flag():
    """The suite-wide default. Without this, every TestClient fixture scrapes."""
    assert os.environ.get("COREPROP_DISABLE_STARTUP_JOBS") == "1"


def test_startup_does_not_spawn_the_pipeline_when_disabled():
    """Run a TestClient in a subprocess and assert neither side-effect thread
    ever started. Threads are observed by name, so this can't be faked by the
    work merely finishing fast."""
    script = textwrap.dedent(
        """
        import os, sys, threading
        os.environ["COREPROP_DISABLE_STARTUP_JOBS"] = "1"
        os.environ.setdefault("SUPABASE_URL", "https://test.supabase.invalid")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
        os.environ.setdefault("SUPABASE_ANON_KEY", "test-anon-key")

        started = []
        _real = threading.Thread.start
        def spy(self):
            started.append(getattr(self, "_target", None).__name__
                           if getattr(self, "_target", None) else "?")
            return _real(self)
        threading.Thread.start = spy

        from fastapi.testclient import TestClient
        from web.app import app
        with TestClient(app) as c:
            c.get("/health")

        print("THREADS:" + ",".join(started))
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(_ROOT), capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, f"subprocess failed:\n{proc.stdout}\n{proc.stderr}"
    line = [l for l in proc.stdout.splitlines() if l.startswith("THREADS:")]
    assert line, f"no marker in output:\n{proc.stdout}"
    names = line[0][len("THREADS:"):]

    assert "run_pipeline" not in names, (
        f"startup spawned the live scrape pipeline despite the disable flag: {names}"
    )
    assert "_run_periodic_models" not in names, (
        f"startup spawned the Supabase refit despite the disable flag: {names}"
    )


def test_flag_absent_still_spawns_them():
    """Guard against the flag becoming permanent. Production MUST still scrape
    on boot — if this passes with the jobs suppressed, the disable is wired too
    broadly and the deployed app would never refresh."""
    script = textwrap.dedent(
        """
        import os, threading
        os.environ.pop("COREPROP_DISABLE_STARTUP_JOBS", None)
        os.environ.setdefault("SUPABASE_URL", "https://test.supabase.invalid")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")
        os.environ.setdefault("SUPABASE_ANON_KEY", "test-anon-key")

        started = []
        _real = threading.Thread.start
        def spy(self):
            t = getattr(self, "_target", None)
            started.append(t.__name__ if t else "?")
            # Do NOT actually start: we only assert intent, so this stays offline.
        threading.Thread.start = spy

        import web.app as m
        m.startup()
        print("THREADS:" + ",".join(started))
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(_ROOT), capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, f"subprocess failed:\n{proc.stdout}\n{proc.stderr}"
    names = [l for l in proc.stdout.splitlines() if l.startswith("THREADS:")][0]
    assert "run_pipeline" in names, (
        "with the flag unset, startup must still scrape — production depends on "
        f"it. Got: {names}"
    )

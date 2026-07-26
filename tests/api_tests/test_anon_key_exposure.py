"""The service-role key must never reach the browser.

`SUPABASE_ANON_KEY` is published: `web/app.py::root()` injects it into
index.html and `GET /api/ui-config` returns it. It used to be read as

    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", SUPABASE_KEY)

i.e. it fell back to `SUPABASE_SERVICE_KEY`. A service-role key BYPASSES RLS,
so one missing env var in production silently handed every visitor full
read/write on every table — visible in page source, no login needed. The same
hole opens if someone pastes the service key into `SUPABASE_ANON_KEY`.

These tests pin the requirement: with the anon key missing (or accidentally
equal to the service key) the app must refuse to serve rather than degrade to
the service key. A hard boot failure is an acceptable outcome — it costs one
deploy, whereas a leaked service key has to be rotated and may be harvested
without anyone noticing.

The env var is read at `engine.database` *import* time, so these run in a
subprocess with a scrubbed environment: mutating `os.environ` in-process would
not re-trigger the module-level read, and reloading the module would leave a
second `engine.database` visible to already-imported callers.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]

# Sentinels. The "service key" value is what must never be published.
_URL = "https://anonkeytest.supabase.invalid"
_SERVICE_KEY = "SERVICE-ROLE-KEY-MUST-NOT-BE-PUBLISHED"

_MARKER = "<<<COREPROP-JSON>>>"

# Imports engine.database + web.app and collects everything the browser can
# see, without starting the app lifespan (no scheduler, no scrapers, no
# network): the two injection points are plain functions.
_PROBE = f"""
import json, os, sys

out = {{}}

# engine.database calls load_dotenv() at import. Do it first ourselves so we can
# report whether a developer's local .env supplies SUPABASE_ANON_KEY behind our
# back — that would make this test vacuous, so the test skips instead.
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass
out["env_anon_after_dotenv"] = os.environ.get("SUPABASE_ANON_KEY")

try:
    import engine.database as dbmod
    out["module_anon"] = dbmod.SUPABASE_ANON_KEY
    out["module_service"] = dbmod.SUPABASE_KEY

    import web.app as appmod
    out["root_html"] = appmod.root().body.decode("utf-8")
    out["ui_config"] = appmod.get_ui_config()

    # The RLS-scoped per-user client: whatever apikey it sends must not be the
    # service key either, or tenant isolation is gone server-side too.
    client = dbmod.get_user_db("jwt-for-some-user")
    out["user_db_apikey"] = None if client is None else client.session.headers.get("apikey")

    out["boot"] = "served"
except BaseException as exc:
    out["boot"] = "refused"
    out["error"] = "{{}}: {{}}".format(type(exc).__name__, exc)

sys.stdout.write({_MARKER!r} + json.dumps(out))
"""


def _probe(anon_key: str | None) -> dict:
    """Import the app in a fresh interpreter with a controlled Supabase env.

    `anon_key=None` means the variable is absent entirely (the production
    misconfiguration). Returns the probe's JSON report.
    """
    env = {k: v for k, v in os.environ.items() if k != "SUPABASE_ANON_KEY"}
    env["SUPABASE_URL"] = _URL
    env["SUPABASE_SERVICE_KEY"] = _SERVICE_KEY
    env["PYTHONPATH"] = str(_ROOT)
    # Belt and braces: nothing here should touch Supabase, but if a code path
    # ever did, don't let it write.
    env["DISABLE_PERSISTENCE"] = "true"
    env["DISABLE_AUTO_BACKTEST"] = "true"
    if anon_key is not None:
        env["SUPABASE_ANON_KEY"] = anon_key

    proc = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=str(_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert _MARKER in proc.stdout, (
        "probe did not report; it crashed before it could.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return json.loads(proc.stdout.split(_MARKER, 1)[1])


def _assert_service_key_unreachable(report: dict, how: str) -> None:
    if report.get("env_anon_after_dotenv"):
        pytest.skip(
            "a local .env supplies SUPABASE_ANON_KEY; this test needs it absent "
            "from the subprocess environment"
        )

    if report["boot"] == "refused":
        # Failing loudly is the preferred outcome. It just must not fail by
        # echoing the secret into logs.
        assert _SERVICE_KEY not in report.get("error", ""), (
            f"startup error message leaks the service key: {report['error']}"
        )
        return

    assert _SERVICE_KEY not in report["root_html"], (
        f"GET / published the SERVICE-ROLE key to the browser ({how}). "
        "Any visitor can read it from page source and bypass RLS entirely."
    )
    assert report["ui_config"]["supabase_anon_key"] != _SERVICE_KEY, (
        f"GET /api/ui-config served the SERVICE-ROLE key as the anon key ({how})."
    )
    assert report["module_anon"] != _SERVICE_KEY, (
        f"engine.database.SUPABASE_ANON_KEY is the service key ({how})."
    )
    assert report.get("user_db_apikey") != _SERVICE_KEY, (
        f"get_user_db() sent the service key as apikey ({how}) — the "
        "RLS-scoped per-user client is really an all-tenant client."
    )


def test_service_key_is_not_published_when_anon_key_is_unset():
    """The documented fallback: SUPABASE_ANON_KEY missing in production."""
    _assert_service_key_unreachable(_probe(None), how="SUPABASE_ANON_KEY unset")


def test_service_key_is_not_published_when_anon_key_equals_it():
    """The copy/paste variant: the service key pasted into SUPABASE_ANON_KEY.
    Presence alone is not enough — the value must not be the service key."""
    report = _probe(_SERVICE_KEY)
    if report.get("env_anon_after_dotenv") == _SERVICE_KEY:
        # Expected: that's the value we injected. Don't let the .env skip fire.
        report = dict(report, env_anon_after_dotenv=None)
    _assert_service_key_unreachable(report, how="SUPABASE_ANON_KEY == service key")


def test_a_real_anon_key_is_served_and_scopes_the_user_client():
    """The happy path must keep working: a distinct anon key is published to
    the browser AND is the apikey on the RLS-scoped per-user client. That
    header is what makes `get_user_db(jwt)` tenant-scoped."""
    report = _probe("a-real-anon-key")

    assert report["boot"] == "served", (
        f"app refused to boot with a valid anon key: {report.get('error')}"
    )
    assert report["module_anon"] == "a-real-anon-key"
    assert report["ui_config"]["supabase_anon_key"] == "a-real-anon-key"
    assert '"supabase_anon_key":"a-real-anon-key"' in report["root_html"]
    assert report["user_db_apikey"] == "a-real-anon-key", (
        "get_user_db() must send the anon key as apikey — RLS scoping depends on it"
    )

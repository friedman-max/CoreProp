"""SPA fallback: the frontend is one page served at "/", but it gets opened at
arbitrary same-origin paths (a tapped Web Push / PWA relaunch, the /privacy &
/terms links, a shared deep link, a refresh on a deep link). Those must serve
the app, not FastAPI's {"detail":"Not Found"} — which is the JSON page users hit
after tapping a notification. `/api/*` misses and missing assets still 404.
"""
from __future__ import annotations

from fastapi.testclient import TestClient

from web.app import app

# No context manager: route resolution doesn't need the startup scheduler, and
# skipping it keeps the test hermetic (no background scrape thread).
client = TestClient(app)


def test_unknown_page_path_serves_the_app():
    r = client.get("/backtest")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "__COREPROP_CONFIG" in r.text          # the injected index.html


def test_privacy_and_terms_do_not_404():
    # Both are linked from the UI (web footer + iOS Account screen); they used
    # to return {"detail":"Not Found"}.
    for path in ("/privacy", "/terms"):
        assert client.get(path).status_code == 200, path


def test_root_still_serves_the_app():
    r = client.get("/")
    assert r.status_code == 200 and "__COREPROP_CONFIG" in r.text


def test_unknown_api_path_still_404_json():
    r = client.get("/api/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"] == "Not Found"      # honest API 404, not HTML


def test_missing_asset_still_404():
    # Anything with a file extension is a missing file, not an app route.
    assert client.get("/nope.png").status_code == 404
    assert client.get("/static/definitely-missing.js").status_code == 404


def test_known_routes_unshadowed():
    # The catch-all must not swallow explicit routes.
    assert client.get("/health").status_code == 200
    assert client.get("/sw.js").status_code == 200

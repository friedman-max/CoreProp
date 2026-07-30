"""The tab icon must actually resolve, at both paths browsers ask for.

The site shipped with no favicon declaration at all, so Chrome fell back to the
grey globe and cached the /favicon.ico 404. Two things have to hold, and each
broke independently:

1. index.html declares the icon links. `root()` rewrites the served HTML to
   inject the Supabase config, so the assertion is against the *response body*,
   not the file on disk.
2. The bare /favicon.ico path resolves. Browsers probe it regardless of what
   the HTML declares, and there is no catch-all route to absorb it.

The referenced files also have to exist — a link to a 404 renders the same grey
globe as no link at all, which is the failure this is guarding.
"""
from __future__ import annotations

import re

import pytest


@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient
    from web.app import app
    with TestClient(app) as c:
        yield c


def test_bare_favicon_path_resolves(client):
    """Browsers request /favicon.ico directly, not just what the HTML declares."""
    r = client.get("/favicon.ico")
    assert r.status_code == 200, "bare /favicon.ico must not 404 — Chrome caches it"
    assert r.headers.get("content-type") == "image/x-icon"
    assert len(r.content) > 0


def test_html_declares_icon_links(client):
    html = client.get("/").text
    assert 'rel="icon"' in html, "no <link rel=icon> — browser falls back to grey globe"
    assert 'rel="apple-touch-icon"' in html
    assert 'rel="manifest"' in html


@pytest.mark.parametrize(
    "path",
    [
        "/static/favicon.ico",
        "/static/favicon-32.png",
        "/static/favicon-16.png",
        "/static/apple-touch-icon.png",
        "/static/site.webmanifest",
        "/static/icon-192.png",
        "/static/icon-512.png",
    ],
)
def test_icon_asset_is_served(client, path):
    r = client.get(path)
    assert r.status_code == 200, f"{path} is referenced but missing"
    assert len(r.content) > 0


def test_every_declared_icon_href_exists(client):
    """A declared-but-missing icon renders exactly like no icon at all.

    Parses the hrefs out of the served HTML rather than hardcoding them, so
    adding a link without shipping the asset fails here.
    """
    html = client.get("/").text
    hrefs = re.findall(
        r'<link[^>]+rel="(?:icon|apple-touch-icon|manifest)"[^>]*>', html
    )
    assert hrefs, "expected icon/manifest link tags in the served HTML"

    checked = 0
    for tag in hrefs:
        m = re.search(r'href="([^"]+)"', tag)
        assert m, f"link tag has no href: {tag}"
        url = m.group(1)
        assert client.get(url).status_code == 200, f"{url} is declared but 404s"
        checked += 1
    assert checked >= 4


def test_favicon_ico_is_multi_resolution():
    """One .ico carrying 16/32/48 — Chrome picks 32, Windows shortcuts use 48.

    The ICONDIR header is parsed by hand rather than with Pillow: Pillow isn't in
    requirements-dev.txt and this is the only assertion that would need it.
    Layout is `reserved u16, type u16, count u16` then `count` 16-byte entries
    whose first two bytes are width/height (0 meaning 256).
    """
    import struct
    from pathlib import Path

    ico = Path(__file__).resolve().parents[2] / "web" / "static" / "favicon.ico"
    blob = ico.read_bytes()
    reserved, kind, count = struct.unpack("<HHH", blob[:6])
    assert reserved == 0 and kind == 1, "not a valid .ico file"

    sizes = set()
    for i in range(count):
        off = 6 + i * 16
        w, h = blob[off], blob[off + 1]
        sizes.add((w or 256, h or 256))

    for expected in [(16, 16), (32, 32), (48, 48)]:
        assert expected in sizes, f"favicon.ico is missing {expected}: {sizes}"


def test_manifest_icons_resolve(client):
    """PWA/Android install surfaces read the manifest, not the <link> tags."""
    manifest = client.get("/static/site.webmanifest").json()
    assert manifest["icons"], "manifest declares no icons"
    for icon in manifest["icons"]:
        assert client.get(icon["src"]).status_code == 200, f"{icon['src']} 404s"

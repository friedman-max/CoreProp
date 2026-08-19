"""Bundle-freshness layer 1: the cache-bust stamps match the bundles.

build.sh derives its build id as `cat web/static/dist/*.js | shasum | cut -c1-10`
and stamps it into every `?v=` token in index.html and the sw.js cache name. The
app is 10 plain global scripts whose cross-file globals only line up when every
bundle is from the SAME build, so a mixed set renders a blank page.

This catches a rebuild committed without its stamps, a hand-edited bundle, and a
partial dist/ commit. It CANNOT catch a .jsx edited with ./build.sh never run —
dist/ and the stamps are both unchanged then, and this test still passes. That
case needs the node CI job (see .github/workflows/tests.yml).

Note the glob is all of dist/*.js — 11 files, including the orphan auth-page.js
— and NOT build.sh's 10-entry FILES array, because build.sh's own
`cat "$OUT_DIR"/*.js` hashes everything in the directory. Using FILES would
produce a different digest.
"""
from __future__ import annotations

import hashlib
import re

from tests.api_tests.css_helpers import INDEX, WEB


def _expected_build_id() -> str:
    digest = hashlib.sha1()
    for path in sorted((WEB / "dist").glob("*.js")):
        digest.update(path.read_bytes())
    return digest.hexdigest()[:10]


def test_index_and_sw_stamps_match_the_bundles():
    expected = _expected_build_id()

    stamps = set(re.findall(r"/static/dist/[a-z0-9-]+\.js\?v=([a-z0-9]+)", INDEX.read_text(encoding="utf-8")))
    assert stamps, "no ?v= stamped script tags found in index.html"
    assert stamps == {expected}, f"index.html stamps {stamps} != bundle digest {expected}"

    sw = (WEB / "sw.js").read_text(encoding="utf-8")
    cache_names = set(re.findall(r"coreprop-shell-([A-Za-z0-9]+)", sw))
    assert cache_names == {expected}, f"sw.js cache {cache_names} != bundle digest {expected}"


def test_every_bundle_is_stamped():
    """All 10 script tags carry a stamp — an unstamped tag would be cached forever."""
    html = INDEX.read_text(encoding="utf-8")
    tags = re.findall(r"/static/dist/([a-z0-9-]+\.js)(\?v=[a-z0-9]+)?", html)
    unstamped = [name for name, stamp in tags if not stamp]
    assert not unstamped, f"dist script tags with no ?v= stamp: {unstamped}"

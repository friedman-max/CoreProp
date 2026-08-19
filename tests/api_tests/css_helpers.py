"""CSS parsing helpers for the stylesheet guard tests.

`web/static/index.html` carries the entire app stylesheet in one <style> block
(there is no .css file in the repo). These helpers turn that text into
(selector, declarations) pairs so the guards can assert per-rule facts without
each test re-implementing a parser.

Deliberately regex-based and dependency-free: the CI pytest job is pip-only and
must not grow a CSS-parser dependency.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WEB = REPO / "web" / "static"
INDEX = WEB / "index.html"
EXTENSION = WEB / "extension.html"
APP_MAIN_JSX = WEB / "app-main.jsx"
APP_MAIN_DIST = WEB / "dist" / "app-main.js"

# Accent colors in every notation used in this stylesheet. Both the var() and
# the literal forms are required: `.ev-row-data.is-sel` used a raw rgba(), so a
# var()-only pattern would have passed straight over it.
ACCENT_VARS = ("var(--primary)", "var(--primary-2)", "var(--primary-hi)")
ACCENT_HEX = ("#1e6fb0", "#6fbcec", "#195f97")
ACCENT_RGB = ((30, 111, 176), (111, 188, 236), (25, 95, 151))

_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}", re.S)


def style_block(path: Path = INDEX) -> str:
    """The contents of the <style> block, comments stripped.

    Comments are removed so commented-out code can neither trip a ban nor
    satisfy an invariant.
    """
    html = path.read_text(encoding="utf-8")
    blocks = re.findall(r"<style>(.*?)</style>", html, re.S)
    assert blocks, f"no <style> block found in {path.name}"
    return _COMMENT.sub("", "\n".join(blocks))


def rules(css: str):
    """Yield (selector, declarations) for every rule.

    @media preludes are skipped naturally: `@media (...)  {` cannot match
    because the text after its `{` contains another `{`, so the engine advances
    and matches the inner rule instead. Any selector still starting with `@`
    (e.g. @keyframes steps) is filtered out.
    """
    for selector, decls in _RULE.findall(css):
        selector = selector.strip()
        if not selector or selector.startswith("@"):
            continue
        yield selector, decls


def declarations(decls: str) -> list[str]:
    """Split a rule body into individual `prop:value` declarations."""
    return [d.strip() for d in decls.split(";") if d.strip()]


def squash(text: str) -> str:
    """Lowercase and remove all whitespace, for whitespace-insensitive matching."""
    return re.sub(r"\s+", "", text).lower()


def has_accent(value: str) -> bool:
    """True if a declaration value names an accent color in any notation."""
    v = squash(value)
    if any(squash(tok) in v for tok in ACCENT_VARS):
        return True
    if any(h in v for h in ACCENT_HEX):
        return True
    return any(f"({r},{g},{b}" in v for r, g, b in ACCENT_RGB)


def is_minigame(selector: str) -> bool:
    """The hero minigame is exempt from the gradient/orb bans.

    It is a deliberate near-clone of the PrizePicks large card (see the comment
    at index.html:453) and `.lp-game-flash` is exactly a border-radius:50%
    radial-gradient glow disc that stays. Matched on the literal class prefix so
    the exemption cannot silently widen.
    """
    return "lp-game-" in selector


def gradient_declarations(decls: str):
    """Yield declarations whose value contains any CSS gradient function."""
    for decl in declarations(decls):
        if re.search(r"(linear|radial|conic)-gradient\(", decl, re.I):
            yield decl

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
from collections.abc import Iterator
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WEB = REPO / "web" / "static"
INDEX = WEB / "index.html"
EXTENSION = WEB / "extension.html"
APP_MAIN_JSX = WEB / "app-main.jsx"
APP_MAIN_DIST = WEB / "dist" / "app-main.js"

# Every accent value in the palette, in every notation this stylesheet uses.
# Both the var() and the literal forms are required: `.ev-row-data.is-sel` used a
# raw rgba(), so a var()-only pattern would have passed straight over it.
# #195F97 is the hover-darken shade of --primary (.cp-btn-primary:hover and
# friends). --primary-lo is listed even though its rgba literal is already
# covered by ACCENT_RGB's (30,111,176): a new accent token that is not in
# ACCENT_VARS is expressible as a gradient the guard silently passes, which is
# the inverse of the miss described above. Adding an accent token to :root means
# adding it here.
#
# What this deliberately does NOT catch, so nobody reads the guard as airtight:
#   * Space-separated CSS Color 4 notation — `rgb(30 111 176 / 40%)`. Zero
#     usages today; ACCENT_RGB matches the comma form only.
#   * Hand-mixed accent-FAMILY blues that are not these tokens, e.g.
#     `.bt-slip-place-install`'s `linear-gradient(180deg,#1e3a5f,#15304f)` and
#     its hover `#26527f,#1b3d63` in index.html. Those read as accent-adjacent
#     on screen but share no value with the palette, so a value-matching guard
#     structurally cannot see them. (That hover also LIGHTENS, against the
#     hover-darkens rule — a real finding, but out of scope for this guard.)
ACCENT_VARS = (
    "var(--primary)",
    "var(--primary-2)",
    "var(--primary-hi)",
    "var(--primary-lo)",
)
ACCENT_HEX = ("#1e6fb0", "#6fbcec", "#195f97")
ACCENT_RGB = ((30, 111, 176), (111, 188, 236), (25, 95, 151))

_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}", re.S)
_QUOTED = re.compile(r"\"[^\"\n]*\"|'[^'\n]*'")


def style_block(path: Path = INDEX) -> str:
    """The contents of the <style> block, comments stripped.

    Comments are removed so commented-out code can neither trip a ban nor
    satisfy an invariant.

    A `{` or `}` inside a quoted value (`content:"}"`) would silently truncate
    the enclosing rule — `_RULE` is brace-counting-free, so it would end the
    declaration block early and resync on the next rule, dropping declarations
    with no error. Nothing in the stylesheet does this today, and the assertion
    below keeps it that way loudly rather than letting a future edit quietly
    blind every guard built on this parser.
    """
    html = path.read_text(encoding="utf-8")
    blocks = re.findall(r"<style[^>]*>(.*?)</style>", html, re.S)
    assert blocks, f"no <style> block found in {path.name}"
    css = _COMMENT.sub("", "\n".join(blocks))
    for m in _QUOTED.finditer(css):
        assert "{" not in m.group() and "}" not in m.group(), (
            f"brace inside a quoted CSS value defeats the rule parser: {m.group()[:60]}"
        )
    return css


def rules(css: str) -> Iterator[tuple[str, str]]:
    """Yield (selector, declarations) for every rule.

    @media preludes are skipped naturally: `@media (...) {` cannot match because
    the text after its `{` contains another `{`, so the engine advances and
    matches the inner rule instead. The `@` guard therefore only catches
    block-less at-rules (`@font-face`, `@import`), not nested ones.

    Keyframe steps ARE yielded, as bare `0%` / `from` / `to` selectors — which is
    deliberate, because it means a gradient hidden inside an animation is still
    caught. Two consequences for whoever writes the next guard: such a violation
    reports as `0% -> …` with no enclosing @keyframes name to locate it by, and
    `is_minigame` cannot exempt it (the step selector carries no `lp-game-`
    class), so an accent gradient added inside `@keyframes lpNudge` / `lpDotPop`
    would be an unexemptable failure.
    """
    for selector, decls in _RULE.findall(css):
        # Collapse newlines/runs of spaces: one selector in index.html spans
        # lines, and the raw text would misalign the failure message.
        selector = " ".join(selector.split())
        if not selector or selector.startswith("@"):
            continue
        yield selector, decls


def declarations(decls: str) -> list[str]:
    """Split a rule body into individual `prop:value` declarations."""
    return [d.strip() for d in decls.split(";") if d.strip()]


def squash(text: str) -> str:
    """Lowercase and remove all whitespace, for whitespace-insensitive matching."""
    return re.sub(r"\s+", "", text).lower()


def has_accent(decl: str) -> bool:
    """True if a `prop:value` declaration names an accent color in any notation.

    Named `decl`, not `value`: the only caller passes a whole declaration, so the
    property name is matched too. That is harmless because every ACCENT_VARS
    entry carries its closing paren — `var(--primary-grad)` does NOT contain
    `var(--primary)`, so an accent-prefixed custom property neither false-flags
    on its own name nor on being referenced. Dropping that paren would break it.
    """
    v = squash(decl)
    if any(squash(tok) in v for tok in ACCENT_VARS):
        return True
    if any(h in v for h in ACCENT_HEX):
        return True
    return any(f"({r},{g},{b}" in v for r, g, b in ACCENT_RGB)


def is_minigame(selector: str) -> bool:
    """The hero minigame is exempt from the gradient/orb bans.

    It is a deliberate near-clone of the PrizePicks large card (see the
    "MINIGAME (.lp-game)" comment block in index.html — anchored by name rather
    than line number, because Phase 1 adds tokens to :root and shifts every line
    below it) and `.lp-game-flash` is exactly a border-radius:50%
    radial-gradient glow disc that stays. Matched on the literal class prefix so
    the exemption cannot silently widen.

    For the accent-gradient guard this branch is currently a no-op, and should
    not be read as the minigame holding a blanket pass: the only gradient under
    `.lp-game-*` today is `.lp-game-flash`'s `rgba(110,255,0,…)` green, which
    `has_accent` rejects on value anyway. The exemption is pre-positioned for the
    later orb/blur guard, which the flash disc genuinely would trip. An accent
    gradient added under `.lp-game-*` would be exempted here — that is the cost
    of the exemption, and the reason it is prefix-scoped rather than broader.
    """
    return "lp-game-" in selector


def gradient_declarations(decls: str) -> Iterator[str]:
    """Yield declarations whose value contains any CSS gradient function."""
    for decl in declarations(decls):
        if re.search(r"(linear|radial|conic)-gradient\(", decl, re.I):
            yield decl

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
EV_PAGE_JSX = WEB / "ev-page.jsx"
EV_PAGE_DIST = WEB / "dist" / "ev-page.js"

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


_INJECTED = re.compile(r"\.textContent\s*=\s*`([^`]*)`", re.S)


def injected_style_block(path: Path = APP_MAIN_JSX) -> str:
    """The runtime-injected stylesheet from app-main.jsx or its compiled bundle.

    `app-main.jsx` builds a <style> element and assigns a template literal to its
    `.textContent`, then appends it to <head> LAST — so it wins every
    equal-specificity tie against index.html's <style> block, and three
    `.ev-row-data` tints over there carry `!important` for no other reason. It is
    the highest-priority stylesheet in the app, and until this helper existed no
    guard read it at all.

    One regex serves both files because esbuild's JSX transform leaves template
    literals alone: `dist/app-main.js` carries the same backticked text, newlines
    included. That is what lets the bans below run over the shipped bundle as
    well as the source, which matters for the same reason
    `test_accent_agrees_in_all_three_copies` reads both — dist/ is committed and
    served directly, so a source-only assertion can pass while the browser runs
    something else.

    Deliberately anchored on `.textContent =` rather than on the `styleEl` name:
    the name is local and esbuild is free to mangle it, the property is not.
    Comments are stripped for the same reason `style_block` strips them.
    """
    text = path.read_text(encoding="utf-8")
    blocks = _INJECTED.findall(text)
    assert blocks, (
        f"no `.textContent = `...`` injected stylesheet found in {path.name}. If "
        "the injection moved to a different mechanism, this helper and every ban "
        "built on it are now blind — fix the pattern, do not delete the caller."
    )
    return _COMMENT.sub("", "\n".join(blocks))


def style_sources() -> list[tuple[str, str]]:
    """Every stylesheet the app ships, as `(label, css)`.

    The permanent bans are supposed to hold "against every file in the set", and
    for three phases they iterated `index.html` alone. All four are listed here so
    a ban is one `for` loop away from total coverage:

      * `index.html` — the whole app stylesheet.
      * `app-main.jsx` — the runtime-injected sheet, highest priority of the four.
      * `dist/app-main.js` — what the browser actually executes.
      * `extension.html` — standalone by design (no React, no shared sheet) so it
        still renders for an expired session, which also means nothing else
        covers it.

    Labels rather than Paths because two entries share a file with a different
    extraction, so `path.name` would report the same name twice.
    """
    return [
        ("index.html <style>", style_block(INDEX)),
        ("app-main.jsx injected sheet", injected_style_block(APP_MAIN_JSX)),
        ("dist/app-main.js injected sheet", injected_style_block(APP_MAIN_DIST)),
        ("extension.html <style>", style_block(EXTENSION)),
    ]


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


def split_lengths(value: str) -> list[str]:
    """Split a spacing shorthand into its top-level parts, respecting parens.

    `value.split()` is wrong here and the reason is not hypothetical: CSS requires
    whitespace around `calc()`'s `+`/`-` operators, so
    `padding-left:calc(var(--row-px) - 3px)` splits into three garbage tokens
    (`calc(var(--row-px)`, `-`, `3px)`) and reports the +EV row's *correct*, fully
    derived 3px-border compensation as three off-scale values. `.cp-nav`'s
    four-part safe-area padding is four calc()s and would report as twelve.

    So depth-track `(` / `)` and only break on whitespace at depth 0. Nesting is
    the norm in this sheet (`clamp(var(--s-6),2.5vw,var(--s-10))`,
    `calc(var(--s-3) + env(safe-area-inset-top))`), which is exactly why a
    non-recursive scanner has to count rather than match.
    """
    parts, depth, buf = [], 0, []
    for ch in value.strip():
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if depth == 0 and ch.isspace():
            if buf:
                parts.append("".join(buf))
                buf = []
            continue
        buf.append(ch)
    if buf:
        parts.append("".join(buf))
    return parts


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

"""The permanent CLAUDE.md stylesheet bans, plus the token mirrors.

CLAUDE.md: "There are no gradients on accent surfaces, no blurred decorative
orbs, and no gradient-clipped text anywhere by deliberate choice — those were
removed, and re-adding one is a visual regression, not a flourish."

These are regression guards, not migration checks — the Phase-1 migration
invariants live in test_css_tokens.py.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import (
    APP_MAIN_DIST,
    APP_MAIN_JSX,
    EXTENSION,
    INDEX,
    declarations,
    gradient_declarations,
    has_accent,
    is_minigame,
    rules,
    squash,
    style_block,
)


def test_no_accent_gradients() -> None:
    """No gradient may contain an accent color.

    Semantic gradients (--green/--red/--amber and their rgba forms) and the
    neutral white shimmers are NOT accent and must keep passing: CLAUDE.md's ban
    is on accent surfaces only, and later phases flatten the semantic ones the
    visual design reaches. Widening this matcher would make the test fight the
    plan.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if is_minigame(selector):
            continue
        for decl in gradient_declarations(decls):
            if has_accent(decl):
                violations.append(f"{selector} -> {decl}")
    assert not violations, "accent gradients found:\n  " + "\n  ".join(violations)


def test_primary_hi_never_backs_inherited_text() -> None:
    """--primary-hi (.22) measures 4.45:1 under inherited --text-3 over .ev-table's
    backdrop; --primary-lo (.10) measures 5.03:1. So --primary-hi may be a
    box-shadow (a ring has no text on it) or a background on a rule that also sets
    an explicit light color -- never a background that lets text inherit.
    See the :root note at --primary-lo.

    This exists because the accent-gradient ban is what forced .ev-row-data.is-sel
    from a gradient to a flat fill, and the obvious flat fill (--primary-hi) is the
    one that fails AA. Nothing else in the suite looks at that background, so
    without this guard the fix is one paste from being silently reverted.

    Limit worth knowing: an explicit `color` on the SAME rule counts as safe, but
    this does no cascade analysis, so a rule that sets a safe color while a
    DESCENDANT re-declares a dim one (exactly the .ev-time case, one level down)
    would still pass. It catches the flat-fill mistake, not every contrast bug.
    """
    SAFE_TEXT = ("var(--primary-2)", "#fff", "#ffffff", "white")
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        ds = declarations(decls)
        bg = [d for d in ds
              if d.partition(":")[0].strip().lower() in ("background", "background-color")
              and "var(--primary-hi)" in squash(d)]
        if not bg:
            continue
        colors = [d for d in ds if d.partition(":")[0].strip().lower() == "color"]
        if not any(any(s in squash(c) for s in SAFE_TEXT) for c in colors):
            violations.append(f"{selector} -> {bg[0]}")
    assert not violations, (
        "--primary-hi behind text that inherits its color (4.45:1 < AA 4.5:1); "
        "use --primary-lo:\n  " + "\n  ".join(violations)
    )


def test_no_blurred_decorative_orbs() -> None:
    """A glow orb is `border-radius:50%` plus a blur or a radial-gradient.

    Matching a bare `filter:blur(` substring would flag `backdrop-filter:blur()`
    on the sticky nav and the modal scrim, which are legitimate material blurs
    on full-bleed surfaces, not decoration. So the matcher requires the round +
    glow *pair* instead. Zero today; this guards against re-adding the removed
    `.lp-orb` discs.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if is_minigame(selector):          # .lp-game-flash is exactly this pair, and stays
            continue
        body = squash(decls)
        round_ = "border-radius:50%" in body
        glow = re.search(r"(?<!backdrop-)filter:blur\(", body) or "radial-gradient(" in body
        if round_ and glow:
            violations.append(selector)
    assert not violations, f"blurred decorative orbs found: {violations}"


def test_no_gradient_clipped_text() -> None:
    """Zero instances today; regression guard."""
    violations = [
        selector
        for selector, decls in rules(style_block(INDEX))
        if "background-clip:text" in squash(decls)
    ]
    assert not violations, f"gradient-clipped text found: {violations}"


def _root_token(css: str, name: str) -> str:
    """Read a custom property's value out of the :root block.

    Scoped to :root by fact, not by parsing: every custom-property *definition*
    in both index.html and extension.html is inside :root today, so the first
    match is that one. Requiring the `:` immediately after the name is what keeps
    `--bg` off `--bg-2` and `--text` off `--text-3`. Redefining a token in a
    later rule (a theme override, a @media block) would need this to take the
    :root block explicitly.
    """
    m = re.search(rf"{re.escape(name)}\s*:\s*([^;}}]+)", css)
    assert m, f"{name} not found in :root"
    return m.group(1).strip().lower()


def test_accent_agrees_in_all_three_copies() -> None:
    """`--primary`, TWEAK_DEFAULTS.accent in the .jsx, AND in the committed bundle.

    An effect writes TWEAK_DEFAULTS.accent back as an *inline* style on
    document.documentElement, which beats the stylesheet. dist/app-main.js is
    what the browser actually runs and carries its own copy of the literal, so a
    .jsx-only assertion would pass while a stale bundle keeps writing the old
    accent everywhere. That is exactly the failure CLAUDE.md's rule exists to
    prevent, and it has happened before.
    """
    token = _root_token(style_block(INDEX), "--primary")

    jsx = APP_MAIN_JSX.read_text(encoding="utf-8")
    # The bundle is minified to a bare `accent:` where the source has `"accent":`.
    pattern = r"[\"']?accent[\"']?\s*:\s*[\"']([#0-9a-fA-F]+)[\"']"
    jsx_m = re.search(pattern, jsx)
    assert jsx_m, "TWEAK_DEFAULTS.accent not found in app-main.jsx"

    dist_m = re.search(pattern, APP_MAIN_DIST.read_text(encoding="utf-8"))
    assert dist_m, "accent literal not found in dist/app-main.js"

    assert token == jsx_m.group(1).lower() == dist_m.group(1).lower(), (
        f"accent disagrees: :root={token} jsx={jsx_m.group(1)} dist={dist_m.group(1)}"
    )


def test_extension_palette_mirror_matches() -> None:
    """extension.html restates the palette under different names.

    It is deliberately standalone (no React, no shared stylesheet) so it still
    renders for an expired session, which means it cannot read :root — its own
    comment says "Keep these values in step with those tokens" and nothing
    enforced that until now. Radii are deliberately NOT asserted: they are
    hand-copied numbers, and pinning them would make a future scale change fail
    in two places instead of one.
    """
    index_css = style_block(INDEX)
    ext_css = style_block(EXTENSION)
    for ext_name, index_name in (
        ("--bg", "--bg"),
        ("--panel", "--card"),
        ("--fg", "--text"),
        ("--muted", "--text-3"),
        ("--accent", "--primary-2"),
    ):
        assert _root_token(ext_css, ext_name) == _root_token(index_css, index_name), (
            f"extension.html {ext_name} != index.html {index_name}"
        )

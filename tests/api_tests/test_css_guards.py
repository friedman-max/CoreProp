"""The permanent CLAUDE.md stylesheet bans, plus the token mirrors.

CLAUDE.md: "There are no gradients on accent surfaces, no blurred decorative
orbs, and no gradient-clipped text anywhere by deliberate choice — those were
removed, and re-adding one is a visual regression, not a flourish."

These are regression guards, not migration checks — the Phase-1 migration
invariants live in test_css_tokens.py.
"""
from __future__ import annotations

import re
from pathlib import Path

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
    """A glow orb is a round element plus a blur or a radial-gradient background.

    Both halves are narrower than they look, and both narrowings are deliberate:

    * Round is three forms, not one: `50%`, `100%`, and a `9{3,}px` pill radius.
      Matching `border-radius:50%` alone silently missed `100%` and `9999px` —
      the latter is exactly what a `rounded-full`-style paste produces — so
      neither miss needed an intent to evade.
    * The glow half is property-scoped and blur is prefix-guarded. A bare
      `filter:blur(` substring would flag `backdrop-filter:blur()` on the sticky
      nav and the modal scrim, which are legitimate material blurs on full-bleed
      surfaces; a radial-gradient matched anywhere in the rule would flag a
      round element using one as a `mask-image`, a real non-decorative
      technique. Both are false positives, which is the failure direction that
      gets a guard weakened rather than obeyed.

    Still not caught, so nobody reads this as airtight: other round spellings
    (`9999rem`, `100vw`), and a gradient reached through a custom property
    (`--orb:radial-gradient(…)` + `background:var(--orb)`), whose declared
    property is `--orb` rather than a background.

    Zero today; this guards against re-adding the removed `.lp-orb` discs.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if is_minigame(selector):          # .lp-game-flash is exactly this pair, and stays
            continue
        body = squash(decls)
        if not re.search(r"border-radius:(50|100)%|border-radius:9{3,}px", body):
            continue
        backgrounds = [d for d in declarations(decls)
                       if d.partition(":")[0].strip().lower().startswith("background")]
        why = []
        if re.search(r"(?<!backdrop-)filter:blur\(", body):
            why.append("filter:blur()")
        if any("radial-gradient(" in squash(d) for d in backgrounds):
            why.append("radial-gradient background")
        if why:
            violations.append(f"{selector} -> round + {' + '.join(why)}")
    assert not violations, (
        "blurred decorative orbs found:\n  " + "\n  ".join(violations)
    )


def test_no_gradient_clipped_text() -> None:
    """Zero instances today; regression guard.

    Matched on the squashed declaration, so the `-webkit-background-clip:text`
    spelling is caught by the same substring.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        for decl in declarations(decls):
            if "background-clip:text" in squash(decl):
                violations.append(f"{selector} -> {decl}")
    assert not violations, (
        "gradient-clipped text found:\n  " + "\n  ".join(violations)
    )


def _root_token(css: str, name: str) -> str:
    """Read a custom property's value: the FIRST definition in the sheet.

    That is :root's value for every name this helper is *asked for* — the palette
    tokens the two callers below pass are each defined once, in :root, and :root
    is the first rule in both files. Requiring the `:` immediately after the name
    is what keeps `--bg` off `--bg-2` and `--text` off `--text-3`.

    It is NOT a general :root reader, and the caveat is live rather than
    theoretical: index.html already defines custom properties outside :root —
    `.lp` sets --lp-gap/--lp-max, and `@media (min-width:1400px)` overrides
    --lp-max further down. For a token with that shape, first-match-wins returns
    whichever definition happens to come first in the file, so asking for one
    means extracting the :root block properly instead.
    """
    m = re.search(rf"{re.escape(name)}\s*:\s*([^;}}]+)", css)
    assert m, f"{name} is not defined anywhere in this stylesheet"
    return m.group(1).strip().lower()


def _accent_literal(path: Path) -> str:
    """TWEAK_DEFAULTS.accent, from either the source or the minified bundle.

    Anchored on `TWEAK_DEFAULTS` because the key search is a substring match:
    `accent` has no word boundary in front of it, so an unanchored pattern takes
    the first accent-ish key in the file — `subaccent:` matches — and a
    theme-preset object added above TWEAK_DEFAULTS would let a real drift in the
    value that actually ships go undetected.

    A window from the name rather than a match on `TWEAK_DEFAULTS\\s*=\\s*\\{`:
    app-main.jsx has an `/*EDITMODE-BEGIN*/` comment between the `=` and the `{`.
    The value pattern is `[^"']+` rather than a hex class so that a switch to a
    non-hex colour fails as a *mismatch* against :root, which names the culprit,
    instead of as a bare "not found".
    """
    text = path.read_text(encoding="utf-8")
    i = text.find("TWEAK_DEFAULTS")           # the name survives minification
    assert i != -1, f"TWEAK_DEFAULTS not found in {path.name}"
    m = re.search(r"[\"']?accent[\"']?\s*:\s*[\"']([^\"']+)[\"']", text[i:i + 400])
    assert m, f"TWEAK_DEFAULTS.accent not found in {path.name}"
    return m.group(1)


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
    jsx = _accent_literal(APP_MAIN_JSX)
    dist = _accent_literal(APP_MAIN_DIST)
    assert token == jsx.lower() == dist.lower(), (
        f"accent disagrees: :root={token} jsx={jsx} dist={dist}"
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
        ext, idx = _root_token(ext_css, ext_name), _root_token(index_css, index_name)
        assert ext == idx, (
            f"extension.html {ext_name}={ext} != index.html {index_name}={idx}"
        )

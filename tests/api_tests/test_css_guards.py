"""The permanent CLAUDE.md stylesheet bans, plus the token mirrors.

CLAUDE.md: "There are no gradients on accent surfaces, no blurred decorative
orbs, and no gradient-clipped text anywhere by deliberate choice — those were
removed, and re-adding one is a visual regression, not a flourish."

These are regression guards, not migration checks — the Phase-1 migration
invariants live in test_css_tokens.py.
"""
from __future__ import annotations

from tests.api_tests.css_helpers import (
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

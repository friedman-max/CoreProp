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
    gradient_declarations,
    has_accent,
    is_minigame,
    rules,
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

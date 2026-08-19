"""Phase-1 migration invariants for the token system.

Separate from test_css_guards.py: these assert THIS design's rules (every radius
goes through a token, cards are flat, no white inset, the +EV row's four
horizontal-padding sites share one token), not CLAUDE.md's permanent bans.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import INDEX, declarations, rules, squash, style_block

EXPECTED_TOKENS = {
    "--r-xl": "20px",
    "--r-lg": "16px",
    "--r-md": "12px",
    "--r-sm": "8px",
    "--r-pill": "999px",
    "--s-1": "4px",
    "--s-2": "8px",
    "--s-3": "12px",
    "--s-4": "16px",
    "--s-5": "20px",
    "--s-6": "24px",
    "--s-8": "32px",
    "--s-10": "40px",
    "--s-12": "48px",
}


def _root_tokens() -> dict[str, str]:
    css = style_block(INDEX)
    root = re.search(r":root\s*\{(.*?)\}", css, re.S)
    assert root, ":root block not found"
    out = {}
    for decl in declarations(root.group(1)):
        if decl.startswith("--"):
            name, _, value = decl.partition(":")
            out[name.strip()] = value.strip()
    return out


def test_scales_are_defined():
    tokens = _root_tokens()
    missing = {k: v for k, v in EXPECTED_TOKENS.items() if tokens.get(k) != v}
    assert not missing, f"tokens missing or wrong: {missing}"


def test_row_px_is_a_fluid_clamp():
    """--row-px must stay a clamp: flattening it to one value is a density
    regression on wide monitors, which is the whole reason the density-overrides
    block exists."""
    value = squash(_root_tokens().get("--row-px", ""))
    assert value.startswith("clamp("), f"--row-px must be a clamp(), got {value!r}"
    assert "var(--s-5)" in value and "var(--s-6)" in value, (
        f"--row-px must clamp between --s-5 and --s-6, got {value!r}"
    )


def test_legacy_radius_aliases_point_at_the_new_scale():
    """Six landing rules still reference the old names; they must resolve to the
    new scale rather than keeping their old literal values."""
    tokens = _root_tokens()
    assert squash(tokens.get("--radius", "")) == "var(--r-lg)"
    assert squash(tokens.get("--radius-sm", "")) == "var(--r-md)"
    assert squash(tokens.get("--radius-xs", "")) == "var(--r-sm)"


def test_elevation_tokens_have_no_white_inset():
    """The `0 2px 0 rgba(255,255,255,.03) inset` highlight is the skeuomorphic
    tell and is removed everywhere, starting with the token itself."""
    tokens = _root_tokens()
    for name in ("--shadow-card", "--shadow-pop"):
        value = squash(tokens.get(name, ""))
        assert value, f"{name} is not defined"
        assert "inset" not in value, f"{name} still carries an inset: {value}"
    assert squash(tokens.get("--ring", "")) == "0004pxvar(--primary-hi)", (
        f"--ring should be the tokenized 4px focus ring, got {tokens.get('--ring')!r}"
    )


# Selectors whose radii are deliberately literal (spec Section 1).
#   lp-game-*  : the PrizePicks-board mimic keeps its geometry (22/26/14/7px).
#                The 26px stage sits 4px outside the 22px card so the padding
#                reads as a concentric frame, and .lp-game-wash's inset:0 only
#                aligns because it repeats the card's 22px exactly.
#   lp-sk-photo: must always equal .lp-game-photo-fb (the skeleton stands in
#                for the photo).
#   ev-check   : 16x16px, so --r-sm (8px) is exactly half its side and renders a
#                perfect circle. It fronts two <input type="checkbox"> toggles in
#                the slip builder, and a circular checkbox reads as a radio
#                button — an affordance change, not a 1-3px softening. Audited:
#                it is the only rule on the scale with a fixed dimension <= 16px.
#   the rest   : 1-4px radii on decorative bars/keys/swatches whose short
#                dimension is 3-12px, where 8px would round them into lozenges.
#
# All of these are the same rule: too small for the scale's 8px floor.
RADIUS_EXEMPT = (
    "lp-game-",
    "lp-sk-photo",
    "ev-check",
    "lp-vig-key",
    "cal-legend-item",
    "ev-legend-swatch",
    "lp-books-bartrack",
    "obs-heat-v",
    "lp-foot-nav",
)

# Literal values that stay literal: true circles/squares (50% and 999px render
# identically on a square) and deliberate square-corner resets.
RADIUS_LITERAL_OK = {"50%", "0"}


def test_every_radius_goes_through_a_token():
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if any(frag in selector for frag in RADIUS_EXEMPT):
            continue
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            if prop.strip().lower() != "border-radius":
                continue
            v = squash(value)
            # `var(--r` covers both the new --r-* scale and the --radius* aliases.
            if "var(--r" in v:
                continue
            if all(part in RADIUS_LITERAL_OK for part in value.split()):
                continue
            violations.append(f"{selector} -> border-radius:{value.strip()}")
    assert not violations, (
        f"{len(violations)} literal border-radius declarations remain:\n  "
        + "\n  ".join(violations)
    )

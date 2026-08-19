"""Phase-1 migration invariants for the token system.

Separate from test_css_guards.py: these assert THIS design's rules (every radius
goes through a token, cards are flat, no white inset, the +EV row's four
horizontal-padding sites share one token), not CLAUDE.md's permanent bans.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import INDEX, declarations, has_accent, rules, squash, style_block

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


# The twelve neutral-grey surface gradients and their flat replacements.
# Two are recessed wells, NOT cards: .ev-slip is a sidebar rail beside
# .ev-table (--card would merge the two), and .cal-curves is a chart well sunk
# below .an-panel (--card would make it lighter than its own container).
FLATTENED_SURFACES = {
    ".ev-filters": "var(--card)",
    ".ev-table": "var(--card)",
    ".bd-filters": "var(--card)",
    ".pp-card": "var(--card)",
    ".pp-faq details": "var(--card)",
    ".bt-card": "var(--card)",
    ".an-panel": "var(--card)",
    ".bd-tbl-wrap": "var(--card)",
    ".bt-slip": "var(--card)",
    ".ev-slip": "var(--bg-2)",
    ".cal-curves": "var(--bg)",
    # Not a gradient itself — a hardcoded #13131c that had to move with
    # .bd-tbl-wrap. The sticky Boards header floats over its own wrapper, so if
    # the two drift apart the header bands visibly as you scroll. Pinned here
    # because it is the one load-bearing pairing in this task, and it previously
    # only matched at scrollTop 0.
    ".bd-tbl thead th": "var(--card)",
}


def test_neutral_card_surfaces_are_flat():
    """No neutral card surface keeps a vertical grey gradient.

    Shimmers (.lp-sk, .cp-skel), the semantic outcome bars and tints, the
    save/place button state fills, and the minigame gradients are NOT neutral
    card surfaces and deliberately stay — do not widen this to every
    linear-gradient(180deg,...).
    """
    seen, violations = set(), []
    for selector, decls in rules(style_block(INDEX)):
        target = FLATTENED_SURFACES.get(selector.strip())
        if target is None:
            continue
        body = squash(decls)
        # Five of these selectors are re-opened by the density-override and
        # mobile blocks to tweak padding/overflow only (.ev-slip, .ev-filters
        # twice, .bd-filters, .ev-table). Those rules declare no background, so
        # they are not the surface declaration and neither carry the gradient nor
        # owe the flat token — checking them would fail the whole test forever.
        # `seen` is only credited by a rule that actually paints, so deleting a
        # surface's background outright still trips the `missing` check below.
        if "background:" not in body:
            continue
        seen.add(selector.strip())
        if "linear-gradient" in body:
            violations.append(f"{selector} still has a gradient")
        elif squash(target) not in body:
            violations.append(f"{selector} should use {target}")
    missing = set(FLATTENED_SURFACES) - seen
    # .cal-curves is dead (no markup references it) and may have been deleted.
    missing.discard(".cal-curves")
    assert not violations and not missing, f"violations={violations} missing={sorted(missing)}"


def test_no_white_inset_highlight_anywhere():
    """The `0 2px 0 rgba(255,255,255,.03) inset` highlight is removed from the
    token AND the nine rules that hardcoded it. Editing only the token reaches
    just .cp-modal and .pp-card and leaves half the surfaces skeuomorphic.

    The minigame's green idle-nudge inset and the semantic 4px left-bar insets on
    the compact result rows are not white highlights and stay.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        for decl in declarations(decls):
            body = squash(decl)
            if "inset" in body and "rgba(255,255,255" in body:
                violations.append(f"{selector} -> {decl.strip()}")
    assert not violations, "white inset highlights remain:\n  " + "\n  ".join(violations)


def test_no_accent_colored_button_glow():
    """Flat buttons: no accent-tinted drop shadow. The blue glow on the save and
    place CTAs is a skeuomorphic holdover."""
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            if prop.strip().lower() == "box-shadow" and has_accent(value):
                violations.append(f"{selector} -> {decl.strip()}")
    assert not violations, "accent-colored glows remain:\n  " + "\n  ".join(violations)


def test_ev_row_horizontal_padding_is_always_row_px():
    """Three rules must share the +EV row's horizontal padding, or the selected
    and logged rows fall out of alignment with each other.

    .ev-row is declared TWICE — a base rule and an unconditional re-declaration
    in the density-overrides block that wins on source order. Editing only the
    base rule ships a no-op.

    `.ev-row-hd` used to be pinned here too. Phase 2a deleted the header row: the
    airy list has no table header, so there is no header for the body rows to
    align with, and keeping its CSS alive only to satisfy this test would be dead
    code. The coupling that still matters is row <-> selected <-> logged.
    """
    css = style_block(INDEX)
    found = {".ev-row": 0, ".ev-row-data.is-sel": 0, ".ev-row-data.is-logged": 0}
    stale = []
    for selector, decls in rules(css):
        key = selector.strip()
        if key not in found:
            continue
        for decl in declarations(decls):
            prop = decl.partition(":")[0].strip().lower()
            if prop not in ("padding", "padding-left"):
                continue
            body = squash(decl)
            if "var(--row-px)" in body:
                found[key] += 1
            elif "clamp(" in body or "px" in body:
                stale.append(f"{key} -> {decl.strip()}")
    assert not stale, "hardcoded +EV row padding remains:\n  " + "\n  ".join(stale)
    # Both copies of .ev-row must be migrated, not just the base.
    assert found[".ev-row"] >= 2, f".ev-row: expected both copies, found {found['.ev-row']}"
    assert found[".ev-row-data.is-sel"] >= 1
    assert found[".ev-row-data.is-logged"] >= 1


def test_selected_and_logged_bars_are_both_3px():
    """The left bars must be equal width so their padding compensation is one
    shared value."""
    for selector, decls in rules(style_block(INDEX)):
        if selector.strip() in (".ev-row-data.is-sel", ".ev-row-data.is-logged"):
            for decl in declarations(decls):
                if decl.partition(":")[0].strip().lower() == "border-left":
                    assert "3px" in decl, f"{selector} left bar should be 3px: {decl.strip()}"


# --text-4 is 2.3:1 — decorative/disabled glyphs only, never readable text.
# These five are single-glyph decorations: a middot, two em dashes, an arrow,
# and an empty heat cell. Each was checked against its markup.
#
# .bt-slip-del is deliberately NOT here. Its ✕ is an interactive control's icon,
# which WCAG 1.4.11 holds to 3:1, and --text-4 left it at 2.21:1 at rest — only
# hover cleared it. It uses --text-3 now, so this test also pins that fix.
#
# Two caveats on the entries that remain:
#   .obs-heat-cell.is-empty has no markup anywhere (dead CSS) — the entry is a
#     permission, not a requirement, and keeps the test stable if the observatory
#     heat table is ever wired up.
#   .bd-edge-cell is only decorative because .bd-edge-cell.is-edge overrides the
#     "+EV" case to --green. If that override is ever removed, real text would
#     render at 2.10:1 and this test would NOT catch it, because the allowlist
#     matches by substring.
TEXT4_ALLOWED = (
    ".ev-meta-dot",
    ".bd-odds-empty",
    ".bd-edge-cell",
    ".pnl-custom-arrow",
    ".obs-heat-cell.is-empty",
)


def test_text4_is_only_on_decorative_glyphs():
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if "var(--text-4)" not in squash(decls):   # squash already strips whitespace
            continue
        if any(allowed in selector for allowed in TEXT4_ALLOWED):
            continue
        violations.append(selector.strip())
    assert not violations, (
        "--text-4 (2.3:1) used on readable text: " + ", ".join(violations)
    )

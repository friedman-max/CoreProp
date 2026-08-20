"""Phase-1 migration invariants for the token system.

Separate from test_css_guards.py: these assert THIS design's rules (every radius
goes through a token, cards are flat, no white inset, the +EV row's four
horizontal-padding sites share one token), not CLAUDE.md's permanent bans.
"""
from __future__ import annotations

import re

import pytest

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
    """The landing rules that still reference the old names must resolve to the
    new scale rather than keeping their old literal values.

    `--radius-xs` used to be asserted here too. Phase 1 kept all three aliases so
    no rule would break mid-migration; by the end of Phase 2b the radius
    migration was complete and --radius-xs had zero consumers, so it was deleted
    rather than left as a scale hole nobody reads. --radius and --radius-sm still
    have live consumers on the landing page and stay.
    """
    tokens = _root_tokens()
    assert squash(tokens.get("--radius", "")) == "var(--r-lg)"
    assert squash(tokens.get("--radius-sm", "")) == "var(--r-md)"
    assert "--radius-xs" not in tokens, (
        "--radius-xs was deleted as an unused alias; if it is back, it needs a "
        "consumer and this assertion should flip to check its value again"
    )


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


def test_ev_row_state_tints_are_flat():
    """The +EV row's logged/selected tints carry no gradient.

    The selected row was flattened in Phase 1 (--primary-lo). The logged row kept
    a 90deg red gradient on desktop while the mobile block already overrode it
    with a flat tint — so the same row read two different ways depending on
    viewport, and on desktop a flat blue selected row sat directly above a
    gradient red one. Phase 2a promotes the mobile flat values to the base rules
    and deletes the mobile copies.

    The legend swatch is a deliberate miniature of the logged row and must match.
    """
    targets = (
        ".ev-row-data.is-sel",
        ".ev-row-data.is-sel:hover",
        ".ev-row-data.is-logged",
        ".ev-row-data.is-logged:hover",
        ".ev-row-data.is-logged.is-sel",
        ".ev-row-data.is-logged.is-sel:hover",
        ".ev-legend-swatch",
    )
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if selector.strip() not in targets:
            continue
        for decl in declarations(decls):
            if decl.partition(":")[0].strip().lower().startswith("background") \
               and "gradient" in squash(decl):
                violations.append(f"{selector} -> {decl.strip()}")
    assert not violations, "gradient +EV state tints remain:\n  " + "\n  ".join(violations)


def test_no_mobile_ev_row_min_width():
    """`.ev-row{min-width:620px}` forced horizontal scrolling on phones. The airy
    row stacks instead, so the rule — and the flat-tint overrides that existed
    only because the gradient's faded end was unreadable inside that scroll — are
    gone."""
    css = style_block(INDEX)
    assert "min-width:620px" not in squash(css), "the +EV row still forces a 620px scroll"


# ── Hex-colour helpers, shared by every colour guard below ────────────────────

_HEX = re.compile(r"#[0-9a-fA-F]{3,8}\b")


def _expand_hex(value: str) -> str:
    """Normalize a hex colour for comparison: lowercased, shorthand expanded.

    `#ABC` and `#aabbcc` are the same colour, and a migration that leaves one
    spelling behind is still a migration that left something behind. Nothing in
    the stylesheet spells a token value in shorthand today; this exists so the
    comparison is about the colour rather than about the typing.
    """
    v = squash(value)
    if len(v) in (4, 5):          # #rgb / #rgba
        v = "#" + "".join(c * 2 for c in v[1:])
    return v


def _hex_forms(literal: str) -> set[str]:
    """The token values one literal could be hiding.

    An 8-digit hex is a 6-digit colour with an alpha byte stapled on, so
    `#16a34a40` hardcodes --save just as surely as `#16a34a` does — it simply
    cannot be *written* as `var(--save)40` (see the .cp-btn-save.is-dis note in
    index.html). Both forms are returned so the guard sees it and the exemption
    has to be spelled out rather than granted by a blind spot.
    """
    v = _expand_hex(literal)
    return {v, v[:7]} if len(v) == 9 else {v}


def _hex_literals(decl: str) -> set[str]:
    """Every token value any hex literal in this declaration could be hiding."""
    return {form for m in _HEX.finditer(decl) for form in _hex_forms(m.group())}


def _root_hex_tokens() -> dict[str, str]:
    """`token -> normalized hex value`, for the plain-hex tokens only.

    rgba() tokens (--green-hi, --primary-lo, --hair) are excluded on purpose:
    those are tints, and a rule that spells one of them out as an rgba() literal
    is a different — weaker, and harder to match — finding than one that hardcodes
    a base palette colour. Stated again in the limits note further down.
    """
    return {
        name: _expand_hex(value)
        for name, value in _root_tokens().items()
        if re.fullmatch(r"#[0-9a-fA-F]{3,8}", squash(value))
    }


# Whole selectors, never prefixes, each with a written reason — the idiom the
# deleted RED2_EXEMPT established, and for the same stated reason: an exemption
# justified by a property the test does not inspect is a hole, not an exemption.
# Shared by the per-family guard and the general one, so a site cannot be exempt
# from one and caught by the other.
#
#   .cp-btn-save.is-dis : `background:#16a34a40;color:#86EFAC80` — --save at 25%
#       alpha and --green-2 at 50%, as 8-digit hex. It cannot be tokenized:
#       `var(--save)40` does not concatenate, because var() substitutes into the
#       token stream and the trailing `40` becomes a separate component value, so
#       the declaration is invalid at computed-value time. Measured in Chromium it
#       computes to rgba(0,0,0,0) — the button silently loses its fill — and
#       CSS.supports() still answers true, because a declaration containing var()
#       defers its validity check. color-mix(in srgb, var(--save) 25%, transparent)
#       does render the same pixel and was rejected on value-identity grounds: it
#       computes to a color(srgb …) value rather than an rgba() one, and its 25%
#       only equals 0x40 (25.098%) because Chromium quantizes the hex's alpha to 8
#       bits first — so the equality would rest on a rounding behaviour instead of
#       on a value. The rule in index.html carries the same note, plus the
#       standing obligation that follows from it: if --save or --green-2 moves,
#       these two move by hand.
HEX_LITERAL_OK = {".cp-btn-save.is-dis"}


# ── The light-label family, and the save fill ─────────────────────────────────
#
# Phase 2b created --red-2, migrated its ~10 literal sites, and pinned exactly
# one of them here as `test_error_red_is_tokenized`. The other three members of
# the same pattern were never created: a light TEXT colour on a tint of its base
# was still a raw hex at 6 green (#86EFAC), 4 amber (#FDE68A) and 4 blue
# (#93C5FD) sites, and #16A34A — the save/confirm button FILL, a third green that
# is NOT --green — had no token at all.
#
# The tell that this was half-done rather than deliberate: all four already
# existed in ios/CoreProp/Theme/Theme.swift, and three of that file's doc comments
# read "Web's --green-2" / "--amber-2" / "--blue-2" for tokens web did not have.
# A design-system port had made the client more consistent than its source of
# truth. So this test is parameterized: the family cannot be migrated a quarter at
# a time again, and adding a fifth member means adding a row here.
#
# Phase 2b also carried a `RED2_EXEMPT = {".lp-game-err"}` so the landing-page
# minigame stayed untouched for one more phase. Phase 3 migrated that rule and
# deleted the exemption, which is why the red row below needs none: `.lp-game-err`
# was the only `lp-game-*` rule using a site-palette colour at all (PP's loss red
# is #FF4A4A, a different value that stays literal) and it renders outside the PP
# card as the game's role="alert" status line.
#
# `min_refs` is a FLOOR and is exact today, so ordinary growth passes and a drop
# fails. It is what stops a row passing vacuously: without it, deleting every
# consumer of --amber-2 would leave "the token is defined and nothing spells its
# hex" trivially true.
FAMILY = {
    #  token         value      min_refs  what it labels
    "--green-2": ("#86efac", 6, "hit / positive / OVER"),
    "--amber-2": ("#fde68a", 4, "push"),
    "--blue-2": ("#93c5fd", 4, "pending / DNP / UNDER"),
    "--red-2": ("#fca5a5", 16, "error / miss / negative"),
    "--save": ("#16a34a", 5, "save / confirm button fill"),
}


@pytest.mark.parametrize("token", sorted(FAMILY))
def test_label_family_token_is_defined_with_the_right_value(token):
    value, _min_refs, _role = FAMILY[token]
    tokens = _root_tokens()
    assert token in tokens, (
        f"{token} is missing from :root. It is the {FAMILY[token][2]} colour and "
        f"ios/CoreProp/Theme/Theme.swift documents it as existing here."
    )
    assert _expand_hex(tokens[token]) == value, (
        f"{token} should be {value}, got {tokens[token]!r}"
    )


@pytest.mark.parametrize("token", sorted(FAMILY))
def test_label_family_colour_is_never_a_literal(token):
    """No rule outside :root may spell the family's hexes.

    The pairs are not interchangeable, which is the whole reason each needs its
    own name: --red / --green / the push amber are the *semantic* fill-and-border
    colours, and the -2 variants are the lighter text that reads on a tint of one
    (--red-2 measures 8.5-10.7:1 over the logged-row tints, where --text-3 fails
    AA). --save is a third green again, distinguished from --green by being a fill
    under a white label rather than an outcome colour.
    """
    value, _min_refs, _role = FAMILY[token]
    violations = [
        f"{selector} -> {decl.strip()}"
        for selector, decls in rules(style_block(INDEX))
        if selector.strip() != ":root" and selector.strip() not in HEX_LITERAL_OK
        for decl in declarations(decls)
        if value in _hex_literals(decl)
    ]
    assert not violations, (
        f"hardcoded {value} ({FAMILY[token][2]}) outside :root — use var({token}):"
        "\n  " + "\n  ".join(violations)
    )


@pytest.mark.parametrize("token", sorted(FAMILY))
def test_label_family_token_is_actually_used(token):
    """Non-vacuity, per member. A token nothing references cannot fail the ban
    above, so the ban would pass for the wrong reason."""
    _value, min_refs, role = FAMILY[token]
    refs = sum(
        squash(decls).count(f"var({token})")
        for selector, decls in rules(style_block(INDEX))
        if selector.strip() != ":root"
    )
    assert refs >= min_refs, (
        f"var({token}) has {refs} references, expected at least {min_refs}. Either "
        f"the {role} sites were renamed/deleted (update the floor and say why) or "
        f"a migration was reverted."
    )


# ── The same rule, generalized to every hex token ─────────────────────────────
#
# The per-family test above is the faithful generalization of what guarded
# --red-2. This one asks the wider question — "does ANY colour with a token still
# appear as a literal?" — and the reason to have both is that they fail in
# different directions. This one is *discovered*: it reads :root and compares, so
# it automatically covers the next family somebody names, but it also goes silent
# if :root changes shape or a token is deleted. The five rows above are *named*,
# so they cannot.
#
# Going general was a measurement, not a preference. Before deciding, every hex
# literal outside :root was bucketed against the 21 hex-valued tokens: 101
# literals over 38 distinct values, and after this pass **zero** of those values
# equals a token. The near-misses are near-misses on purpose and are already
# documented at their sites (#0a0a12 vs --bg #0a0a0d, #1a1a26 vs --card-2
# #1a1a25, #FECACA vs --red-2 #FCA5A5, #FBBF24 vs --amber #F59E0B) — a
# value-equality guard passes all of them, correctly, because they are different
# colours and folding them together would be a visual change wearing a
# refactor's clothes. So the exemption list is ONE selector against a guarded set
# of 21 tokens. When the exemption list is that much shorter than the guarded set,
# general is the answer.
#
# What this deliberately does NOT catch, so nobody reads it as airtight:
#   * rgb()/rgba()/hsl() spellings of a token's colour. `.ev-row-data.is-sel`
#     used a raw rgba() before Phase 1 and css_helpers.ACCENT_RGB exists because
#     of it. Only the --*-hi/--*-lo tints are rgba() today and they are excluded
#     from _root_hex_tokens by design; a base colour re-spelled as rgb() would
#     pass here.
#   * Inline styles in .jsx. `ev-page.jsx` still carries #16a34a/#22c55e in three
#     style={} objects and components.jsx/landing.jsx carry #86EFAC/#FDE68A in
#     their book-badge maps. Those are real duplicates of this palette, they are
#     out of this test's file, and migrating them means rebuilding dist/.

def _literal_token_colours(css: str) -> tuple[list[str], int]:
    """`(violations, literals_inspected)` for one stylesheet.

    Returns the count so the caller can prove the scan saw something, and takes
    `css` rather than reading INDEX so the checker itself can be exercised on a
    synthetic fixture.
    """
    by_value = {v: k for k, v in _root_hex_tokens().items()}
    violations, inspected = [], 0
    for selector, decls in rules(css):
        sel = selector.strip()
        if sel == ":root":
            continue
        for decl in declarations(decls):
            for m in _HEX.finditer(decl):
                inspected += 1
                hit = next(
                    (by_value[f] for f in _hex_forms(m.group()) if f in by_value), None
                )
                if hit and sel not in HEX_LITERAL_OK:
                    violations.append(f"{sel} -> {decl.strip()}  (that is {hit})")
    return violations, inspected


def test_no_literal_hex_for_a_colour_that_has_a_token():
    tokens = _root_hex_tokens()
    violations, inspected = _literal_token_colours(style_block(INDEX))
    # Floors, not counts: today 21 tokens and 101 literals. A scan that stops
    # matching turns this into an assertion over nothing.
    assert len(tokens) >= 15, f"only {len(tokens)} hex tokens parsed out of :root"
    assert inspected >= 60, f"only {inspected} hex literals inspected"
    assert not violations, (
        f"{len(violations)} literal(s) spell a colour that has a token:\n  "
        + "\n  ".join(violations)
    )


def test_the_literal_hex_guard_can_actually_fail():
    """Prove the checker on a fixture, because the real stylesheet is clean and a
    clean run cannot distinguish "no violations" from "no scan".

    Pins the two normalizations as well: shorthand and 8-digit alpha both have to
    resolve onto the token, and a near-miss has to be left alone.
    """
    real = _root_hex_tokens()
    assert real.get("--save") == "#16a34a" and real.get("--card") == "#14141e", (
        "this fixture is written against --save/--card; :root moved"
    )
    fixture = """
      .fx-plain{background:#16A34A}
      .fx-alpha{background:#16a34a40}
      .fx-short{color:#fff}
      .fx-nearmiss{background:#14141f}
      .fx-fallback{background:var(--card,#14141e)}
      .cp-btn-save.is-dis{background:#16a34a40}
    """
    violations, inspected = _literal_token_colours(fixture)
    assert inspected == 6, inspected
    flagged = {v.split(" ->")[0] for v in violations}
    assert flagged == {".fx-plain", ".fx-alpha", ".fx-fallback"}, flagged


def test_every_hex_literal_exemption_still_matches_a_real_site():
    """A stale exemption is a standing permission for whatever moves onto that
    selector next. Borrowed from test_ios_tokens.py, which pays for the same guard
    twice."""
    live = {
        selector.strip()
        for selector, decls in rules(style_block(INDEX))
        if any(_hex_literals(decl) for decl in declarations(decls))
    }
    stale = sorted(HEX_LITERAL_OK - live)
    assert not stale, (
        "these exemptions match no rule that contains a hex literal — the site was "
        "fixed or renamed, so the entry is now a blank cheque. Delete it:\n  "
        + "\n  ".join(stale)
    )


def test_no_hex_fallback_inside_a_var_reference():
    """`var(--red,#ef4444)` is a second source of truth for a palette value, and
    the fallback only ever fires when the token is missing — which, for a token
    defined in :root, means never. Phase 2b removed this shape from
    `var(--card,#13131c)`; .bt-del-confirm carried the same thing on --red for two
    more phases, matching value and all, which is exactly why a matching fallback
    is worth banning rather than tolerating: nothing goes wrong until the token
    moves and one of the two copies doesn't.

    Scoped to COLOUR fallbacks. `@keyframes lpBarGrow`'s `scaleX(var(--p,0))` is a
    real default — landing.jsx sets --p inline per bar, so it genuinely is absent
    until then — and needs no exemption under this wording.
    """
    bad = re.compile(r"var\(\s*--[a-z0-9-]+\s*,\s*(#|rgb|hsl)", re.I)
    violations = [
        f"{selector} -> {decl.strip()}"
        for selector, decls in rules(style_block(INDEX))
        for decl in declarations(decls)
        if bad.search(decl)
    ]
    assert not violations, (
        "var() colour fallbacks duplicate the palette:\n  " + "\n  ".join(violations)
    )


# The four Backtest outcome cards. Named explicitly rather than discovered, so a
# rename cannot make this test pass by matching nothing.
BT_OUTCOME_RULES = {
    ".bt-slip-compact.is-win",
    ".bt-slip-compact.is-loss",
    ".bt-slip-compact.is-push",
    ".bt-slip-compact.is-pending",
}


def test_backtest_cards_have_one_left_bar_and_flat_fills():
    """Each outcome card carried TWO left bars — a 3px ::before gradient and a
    4px `inset 4px 0 0` box-shadow — plus a gradient background and a blurred
    glow. One 3px bar, flat fill, no glow.

    The box-shadow is asserted *absent*, not merely inset-free: three of the four
    rules paired the inset bar with a blurred `0 6px 20px -10px` outcome-coloured
    glow, and `test_no_accent_colored_button_glow` cannot see those — it matches
    on the accent blue only, and these glows are green/red/amber. After
    flattening, none of the four owns a box-shadow at all, so "no box-shadow" is
    the precise invariant and it catches either half coming back.

    Only `.bt-slip-compact.is-*` is in scope. The 3px `.bt-slip.is-*::before` bar
    keeps its gradient — that is the surviving single bar, and its fade to 20%
    alpha is the bar's shape rather than a surface fill.
    """
    css = style_block(INDEX)
    violations, seen = [], set()
    for selector, decls in rules(css):
        s = selector.strip()
        if not s.startswith(".bt-slip-compact.is-"):
            continue
        seen.add(s)
        body = squash(decls)
        if "inset4px00" in body:
            violations.append(f"{s} still has the 4px inset bar")
        if "linear-gradient" in body:
            violations.append(f"{s} still has a gradient fill")
        if "box-shadow:" in body:
            violations.append(f"{s} still has a box-shadow (the glow half)")
    missing = BT_OUTCOME_RULES - seen
    assert not missing, f"outcome rules not found — renamed? {sorted(missing)}"
    assert not violations, "\n  ".join(violations)


def test_backtest_pending_is_blue_on_every_surface():
    """Pending was amber on the card and blue on the bar and the badge. Pending
    means "not settled yet", not "warning" — amber implies caution the state does
    not carry, and two of the three sites were already blue.

    Pinned by the rgba/hex family rather than an exact value: the three surfaces
    deliberately differ in alpha and lightness (a .14 tint, a #60A5FA bar, a
    var(--blue-2) badge label), so equality would be wrong. What must hold is that
    none of them is in the amber family.

    Both notations of each family are listed. The badge's label used to be a
    literal #93C5FD and is var(--blue-2) now, so a hex-only AMBER tuple would no
    longer notice `color:var(--amber-2)` landing on it — the tokenization pass
    would have quietly hollowed this test out. Naming a new -2 token means adding
    it to whichever family it belongs to.
    """
    AMBER = ("rgba(251,191,36", "#fbbf24", "#fde68a", "var(--amber-2)")
    BLUE = ("rgba(96,165,250", "#60a5fa", "#93c5fd", "var(--blue-2)")
    targets = {
        ".bt-slip-compact.is-pending",
        ".bt-slip.is-pending::before",
        ".bt-slip-badge.is-pending",
    }
    seen, violations = set(), []
    for selector, decls in rules(style_block(INDEX)):
        s = selector.strip()
        if s not in targets:
            continue
        seen.add(s)
        body = squash(decls)
        if any(a in body for a in AMBER):
            violations.append(f"{s} is still amber: {body}")
        elif not any(b in body for b in BLUE):
            violations.append(f"{s} names no pending blue: {body}")
    assert seen == targets, f"pending rules not found: {sorted(targets - seen)}"
    assert not violations, "\n  ".join(violations)


# Marketing-surface spacing. Every padding/margin/gap length on a .lp-* or .pp-*
# rule must resolve through the spacing scale, so the signed-out pages share one
# rhythm with the app instead of ~80 hand-typed values.
#
# Exempt, by selector substring:
#   lp-game- / lp-sk : the hero minigame is a deliberate PrizePicks-board clone,
#                      exempt from the whole modernization — colours AND geometry.
#                      `.lp-sk*` is the minigame's skeleton and is measured
#                      against the loaded card to make the swap cost 0px of
#                      layout shift, so its values are load-bearing too. Note
#                      `.lp-game` itself has no trailing hyphen and so is not
#                      matched by the usual "lp-game-" check — it is listed
#                      explicitly.
#   lp-hero          : the hero's vertical budget is measured so the minigame
#                      card clears the fold at 1280x800 (see the note above
#                      .lp-hero in index.html). Its values are the result of that
#                      measurement, not drift.
#
# The plan for this phase also listed `lp-vig-svg` and `lp-books-bar` here, for
# "SVG and animation geometry, not layout spacing". Both were dropped after
# checking what they actually reach: this invariant only inspects padding /
# margin / gap, so a height, a border-radius or a transform is already outside
# it — `.lp-vig-svg` declares only `display`, and `.lp-books-bartrack` /
# `-barfill` / `-barlbl` / `-barpct` declare no spacing at all. The one thing the
# `lp-books-bar` prefix did reach was `.lp-books-bars`' gap/margin and
# `.lp-books-barrow`'s gap, which are ordinary layout spacing on the scale — the
# opposite of what the exemption said it was for.
MARKETING_SPACING_EXEMPT = (
    "lp-game",          # covers `.lp-game` and every `.lp-game-*`
    "lp-sk",
    "lp-hero",
)

# Rules whose spacing is deliberately literal because the 4px scale cannot
# express it. Spelled as whole selectors, not prefixes (the deleted RED2_EXEMPT's
# precedent), so an exemption cannot silently widen to a new rule. Every entry is
# a 1-3px value: a sub-scale nudge is optical alignment or a hairline, not
# spacing — the same reasoning Phase 1 used to keep its 1-4px decorative radii
# literal.
#
#   .lp-sr-only           : `margin:-1px` is half of the visually-hidden recipe
#                           (1x1px box, negative margin, clip). Not spacing at
#                           all, and it carries the aria-live region.
#   .lp-cov-grid          : `gap:1px` IS the divider — the grid paints --hair and
#                           the cells paint --bg, so the gap is the hairline
#                           between coverage cells. 4px would be a visible gutter.
#   .lp-bk-head           : the `1px` bottom pad optically seats the uppercase
#                           column labels on the first book tile. Its horizontal
#                           pad is tokenized.
#   .lp-why-steps div span: `margin-top:3px` aligns the step description's cap
#                           height with the digit in its circle beside it.
#
# Phase 3 (pricing) added four more, on the same 1-3px reasoning:
#
#   .pp-check             : `margin-top:1px` optically seats the 22px check circle
#                           against the cap height of the 14.5px benefit title
#                           beside it. Alignment, not spacing.
#   .pp-price-row         : `gap:2px` separates the three parts of one number
#                           ("$", "50", "/mo") on a shared baseline, so it is
#                           kerning between glyph runs. --s-1 doubles it and opens
#                           a visible hole after the currency symbol.
#   .pp-b-d               : `margin-top:2px` is a half-leading correction between
#                           a benefit's title and its description inside one text
#                           block, on top of the description's own line-height.
#                           --s-1 would read as two unrelated lines.
#   .pp-save              : `padding:2px var(--s-2)` — an 11px micro-badge inside
#                           the billing-toggle button; 4px of vertical pad fattens
#                           the pill against a 13.5px label. Only the vertical
#                           component is literal; the horizontal one is on the
#                           scale, and the exemption exists only because this test
#                           judges a declaration as a whole.
MARKETING_SPACING_LITERAL_OK = {
    ".lp-sr-only",
    ".lp-cov-grid",
    ".lp-bk-head",
    ".lp-why-steps div span",
    ".pp-check",
    ".pp-price-row",
    ".pp-b-d",
    ".pp-save",
}

# Lengths that are legal without a token: zero, auto-centring, and the two
# derived layout vars the landing page defines on `.lp`.
#
# Any clamp() passes: fluid spacing is the intended escape hatch for values that
# must differ between a phone and a desktop, and the clamp's own endpoints are
# tokens by convention (--lp-px, .pp-card). The inner pattern is `.*` rather than
# `[^)]*` precisely so a clamp that interpolates var() tokens still matches —
# values in this stylesheet carry no spaces, so a `.*` cannot span two lengths of
# a shorthand.
_SPACING_OK = re.compile(
    r"^(0|auto|var\(--s-\d+\)|var\(--lp-(?:gap|px)\)|clamp\(.*\))$"
)
_SPACING_PROPS = ("padding", "margin", "gap", "row-gap", "column-gap")


def test_marketing_spacing_goes_through_the_scale():
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        sel = selector.strip()
        if not re.search(r"\.(lp|pp)-", sel):
            continue
        if any(frag in sel for frag in MARKETING_SPACING_EXEMPT):
            continue
        if sel in MARKETING_SPACING_LITERAL_OK:
            continue
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            prop = prop.strip().lower()
            if prop not in _SPACING_PROPS and not prop.startswith(("padding-", "margin-")):
                continue
            # A shorthand is a list of lengths; every part must be legal.
            for part in value.split():
                if not _SPACING_OK.match(part.strip()):
                    violations.append(f"{sel} -> {prop}:{value.strip()}")
                    break
    assert not violations, (
        f"{len(violations)} off-scale marketing spacing declarations:\n  "
        + "\n  ".join(sorted(set(violations)))
    )

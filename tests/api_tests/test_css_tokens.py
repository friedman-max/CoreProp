"""Phase-1 migration invariants for the token system.

Separate from test_css_guards.py: these assert THIS design's rules (every radius
goes through a token, cards are flat, no white inset, the +EV row's four
horizontal-padding sites share one token), not CLAUDE.md's permanent bans.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import (
    INDEX,
    declarations,
    has_accent,
    rules,
    split_lengths,
    squash,
    style_block,
)

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


# Phase 2b carried a `RED2_EXEMPT = {".lp-game-err"}` here so the landing-page
# minigame stayed untouched for one more phase. Phase 3 migrated that rule to
# var(--red-2) and deleted the exemption, so this invariant is now unconditional:
# no rule outside :root may spell #FCA5A5. `.lp-game-err` was the only `lp-game-*`
# rule using a site-palette colour at all — PP's loss red is #FF4A4A, which is a
# different value and stays literal — and it renders outside the PP card as the
# game's role="alert" status line.
def test_error_red_is_tokenized():
    """#FCA5A5 is the error/miss red. It appeared in ~10 rules with no token, so
    a change meant finding every site by hand. --red-2 is that token.

    --red (#EF4444) is the *semantic* red for bars, borders and fills; --red-2 is
    the lighter text red that reads on a red tint (it measures 8.5-10.7:1 over
    the logged-row tints where --text-3 fails). They are not interchangeable.
    """
    css = style_block(INDEX)
    tokens = {}
    root = re.search(r":root\s*\{(.*?)\}", css, re.S)
    for decl in declarations(root.group(1)):
        if decl.startswith("--"):
            name, _, value = decl.partition(":")
            tokens[name.strip()] = value.strip()
    assert squash(tokens.get("--red-2", "")) == "#fca5a5", (
        f"--red-2 should be #FCA5A5, got {tokens.get('--red-2')!r}"
    )
    # No rule outside :root may hardcode it.
    violations = []
    for selector, decls in rules(css):
        if selector.strip() == ":root":
            continue
        if "#fca5a5" in squash(decls):
            violations.append(selector.strip())
    assert not violations, "hardcoded #FCA5A5 outside :root: " + ", ".join(violations)


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
    #93C5FD badge label), so equality would be wrong. What must hold is that none
    of them is in the amber family.
    """
    AMBER = ("rgba(251,191,36", "#fbbf24", "#fde68a")
    BLUE = ("rgba(96,165,250", "#60a5fa", "#93c5fd")
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
#
# "Any clamp() passes" is the known hole, and it is real rather than theoretical:
# `.lp-cta-card{padding:clamp(36px,4.5vw,56px) var(--s-6)}` derives from nothing —
# both endpoints are literal — and this pattern waves it through. The app-screen
# sibling below (`_APP_SPACING_OK`) closes it by requiring a `var(--` inside the
# function, and the two patterns differ for exactly that one declaration: applying
# the tightening here fails `.lp-cta-card` and nothing else. That is left as a
# reported worklist item rather than fixed by exempting the rule, because 36px/56px
# are not the 1-3px optical values MARKETING_SPACING_LITERAL_OK is for. Migrate
# `.lp-cta-card` to token endpoints and the two patterns can merge.
_SPACING_OK = re.compile(
    r"^(0|auto|var\(--s-\d+\)|var\(--lp-(?:gap|px)\)|clamp\(.*\))$"
)
_SPACING_PROPS = ("padding", "margin", "gap", "row-gap", "column-gap")


def _spacing_declarations(sel_filter) -> list[tuple[str, str, str]]:
    """`(selector, prop, normalised value)` for every spacing declaration whose
    selector satisfies `sel_filter`.

    Shared by the marketing and app-screen invariants so the two cannot disagree
    about what counts as a spacing declaration — the asymmetry this pair exists to
    remove was one surface having an invariant and the other not, and a second
    copy of the property list is how that comes back.
    """
    out = []
    for selector, decls in rules(style_block(INDEX)):
        sel = selector.strip()
        if not sel_filter(sel):
            continue
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            prop = prop.strip().lower()
            if prop not in _SPACING_PROPS and not prop.startswith(("padding-", "margin-")):
                continue
            out.append((sel, prop, " ".join(value.split())))
    return out


# ── Which test owns which selector ────────────────────────────────────────────
# The marketing filter requires a HYPHEN (`\.(lp|pp)-`), so the two bare page
# shells `.lp` and `.pp` match neither `lp-` nor `pp-` and, until the app-screen
# test below existed, were checked by nothing at all —
# `.pp{padding:var(--s-12) var(--s-6) 80px}` was unguarded on the one surface that
# had an invariant.
#
# They are owned by the APP-SCREEN test, deliberately, and the two scopes are
# written as exact complements (`is_marketing` / `not is_marketing`) so ownership
# is a property of the code rather than of two hand-maintained lists —
# `test_the_two_spacing_scopes_partition_the_stylesheet` proves it.
#
# `.pp` lands on the app side because the exemption its 80px needs (trailing
# scroll padding on a page container, which the scale cannot express) is a category
# the app test has to create anyway for `.ev-main` and `.bd-page`. Putting `.pp`
# on the marketing side would mean writing that reasoning twice, in two files'
# worth of comments, for one declaration.
def is_marketing(selector: str) -> bool:
    return bool(re.search(r"\.(lp|pp)-", selector))


def _has_class(selector: str) -> bool:
    return bool(re.search(r"\.[A-Za-z_-]", selector))


def test_marketing_spacing_goes_through_the_scale():
    violations = []
    for sel, prop, value in _spacing_declarations(is_marketing):
        if any(frag in sel for frag in MARKETING_SPACING_EXEMPT):
            continue
        if sel in MARKETING_SPACING_LITERAL_OK:
            continue
        # A shorthand is a list of lengths; every part must be legal.
        for part in split_lengths(value):
            if not _SPACING_OK.match(part):
                violations.append(f"{sel} -> {prop}:{value}")
                break
    assert not violations, (
        f"{len(violations)} off-scale marketing spacing declarations:\n  "
        + "\n  ".join(sorted(set(violations)))
    )


# ── App-screen spacing ────────────────────────────────────────────────────────
# The same invariant as the marketing one, for the four app screens (+EV, Boards,
# Backtest, Analytics/Observatory/Calibration) plus the shared chrome. It did not
# exist for three phases, which is how `.bt-card{padding:14px 16px}` — the spec's
# own headline example of padding drift — survived all of them: the surface the
# modernization was actually about was the one surface with no guard.
#
# Legal values are the marketing set minus the landing-only `--lp-*` layout vars,
# plus `--row-px` (the +EV row's derived horizontal padding, which three rules must
# share), and with the derived-function hole closed: a `calc()` / `clamp()` /
# `min()` / `max()` must contain at least one `var(--`, so a "derived" value
# actually derives from a token instead of being two literals in a clamp. See the
# note on `_SPACING_OK` for the single marketing declaration that blocks merging
# the two patterns.
_APP_SPACING_OK = re.compile(
    r"^(0|auto|var\(--s-\d+\)|var\(--row-px\)"
    r"|(?:calc|clamp|min|max)\(.*var\(--.*\))$"
)

# ── Exemption group 1: sub-scale optical values ───────────────────────────────
# Every entry is a declaration whose only literal parts are 1-3px. That is the
# standard every existing exemption in this file and in test_ios_tokens.py rests
# on: the scale's floor is --s-1 (4px), so below that a value is seating a glyph on
# a baseline, drawing a hairline, or constructing a control — snapping it to 4px
# changes the drawing, not the rhythm. A declaration with any literal part >= 4px
# is off the scale and does NOT qualify, however small the rest of it is; that is
# why `.cp-nav-c{padding:2px var(--s-4) 4px}` is absent while its `margin:` sibling
# is present, and why the 5/6/7/8px insets on the badges are all still in the
# worklist.
#
# Keys are the whole `"<selector> -> <prop>:<value>"` string, matched entire —
# never a prefix, never a bare selector. A selector-level key would exempt every
# spacing declaration on the rule (the shape MARKETING_SPACING_LITERAL_OK is stuck
# with, and the reason `.pp-save`'s entry has to apologise for covering its
# on-scale horizontal padding too). The cost of the tighter key is churn: changing
# an exempt value breaks its own exemption and fails this test. That is the safe
# direction — it asks for a re-read of the value — and it is the same trade
# test_ios_tokens.py's whole-line keys make.
APP_SPACING_OPTICAL_OK = {
    # A leg's prop line sits directly under its player name inside one text
    # block; 3px is the half-leading between them, on top of the line-height.
    # --s-1 would read as two unrelated lines. iOS makes the identical call at
    # 1pt (SlipCard's `VStack(spacing: 1)`).
    ".bt-leg-prop -> margin-top:3px":
        "leading between a leg's name and its prop line, not a gap",
    # Slip type over its timestamp, in a flex column. Two lines of one label.
    ".bt-slip-hd-l -> gap:2px":
        "type/timestamp leading inside one header label, not a gap",
    ".bt-slip-compact .bt-slip-hd-l -> gap:2px":
        "the compact card's copy of the same type/timestamp pair",
    # .bt-slip-foot-k above .bt-slip-foot-v: a label above its value in one
    # footer cell. Same pairing iOS exempts at 2pt in SlipCard and SlipView.
    ".bt-slip-foot-c -> gap:2px":
        "label-above-value leading inside one footer cell, not a gap",
    # The P&L range picker is a segmented control: one bordered track holding
    # abutting buttons. The 3px is the track's inset around them and the 2px is
    # the seam between segments — both are the control's construction, and 4px
    # visibly breaks it into separate buttons.
    ".pnl-range -> padding:3px":
        "segmented-control track inset around abutting buttons, not a gap",
    ".pnl-range -> gap:2px":
        "seam between two segments of one control, not a gap",
    # A 9.5px sample count under its heat value, inside one table cell. The
    # cell's own padding is the spacing; this is hairline leading.
    ".obs-heat-n -> margin-top:1px":
        "leading between a heat value and its count inside one cell",
    ".obs-mult-n -> margin-top:2px":
        "the multiplier table's copy of the same value/count pair",
    # The tinted value block inside a heat cell. Its vertical inset is what makes
    # the tint read as a chip rather than filling the cell; the cell's 8px/12px
    # padding is the actual spacing.
    ".obs-heat-v -> padding:2px 0":
        "vertical inset of a tint chip inside a cell that owns the spacing",
    # The mobile tab strip's 2px top nudge. Its side margins are already derived
    # (`calc(-1 * var(--s-4))`, pinned to .cp-nav's gutter minimum by the comment
    # above it) — only the optical top offset is literal.
    ".cp-nav-c -> margin:2px calc(-1 * var(--s-4)) 0":
        "optical top nudge on the scrollable tab strip; its sides are derived",
}

# ── Exemption group 2: constrained from outside the rhythm ────────────────────
# A different category, kept separate on purpose: these are not sub-scale, they are
# values whose constraint does not come from the spacing rhythm at all, so no
# token could express them even in principle.
#
# All seven are trailing BOTTOM padding on a page container — scroll breathing room
# under the last row so the final card is not flush against the viewport edge. The
# scale deliberately stops at --s-12 (48px) and there is no --s-7/--s-9/--s-11 by
# design, so 60px and 80px are unexpressible. Extending the scale to reach them
# would be a larger decision than this test, and is explicitly NOT what these
# entries are asking for.
#
# One hypothesis checked and disproved during the audit, recorded so nobody spends
# the time again: these are not clearance for a fixed bottom nav. `--nav-h` (67px)
# is the TOP nav — its only consumers are `.ev-slip{position:sticky;top:var(--nav-h)}`
# and `.ev`'s min-height — and the mobile tab row scrolls horizontally inside that
# same top nav rather than docking at the bottom. There is no under-nav bug hiding
# behind these numbers; they are simply generous trailing padding.
#
# Each container is listed once per declaration because each is re-declared in the
# density-override and @media blocks with a different gutter, and all copies must
# keep the same trailing value or the page's bottom gap changes with the viewport.
APP_SPACING_OUTSIDE_RHYTHM_OK = {
    ".ev-main -> padding:var(--s-6) var(--s-6) 80px":
        "trailing scroll room below the last +EV row; --s-12 (48px) is the "
        "scale's top step and cannot express 80px",
    ".ev-main -> padding:var(--s-6) clamp(var(--s-6),2.5vw,var(--s-10)) 80px":
        "the density-override copy of the same page container",
    ".bd-page -> padding:var(--s-5) var(--s-6) 60px":
        "trailing scroll room below the board table; unexpressible on the scale",
    ".bd-page -> padding:var(--s-5) clamp(var(--s-6),2.5vw,var(--s-10)) 60px":
        "the density-override copy of the same page container",
    ".bd-page -> padding:var(--s-5) var(--s-4) 60px":
        "the @media 900 copy of the same page container",
    ".pp -> padding:var(--s-12) var(--s-6) 80px":
        "trailing scroll room below the pricing card; unexpressible on the scale",
    ".pp -> padding:var(--s-12) clamp(var(--s-6),3vw,56px) 80px":
        "the wide-viewport copy of the same page container. NOTE: its gutter "
        "clamp tops out at a literal 56px, which passes only because the clamp "
        "also names var(--s-6) — that endpoint is worth migrating, but it is a "
        "gutter, not the trailing value this entry is about",
}

APP_SPACING_EXEMPT = {**APP_SPACING_OPTICAL_OK, **APP_SPACING_OUTSIDE_RHYTHM_OK}


def _app_spacing_violations() -> list[str]:
    violations = []
    for sel, prop, value in _spacing_declarations(
        lambda s: _has_class(s) and not is_marketing(s)
    ):
        key = f"{sel} -> {prop}:{value}"
        if key in APP_SPACING_EXEMPT:
            continue
        for part in split_lengths(value):
            if not _APP_SPACING_OK.match(part):
                violations.append(key)
                break
    return violations


def test_app_screen_spacing_goes_through_the_scale():
    """Every padding/margin/gap length on a non-marketing rule resolves through
    the spacing scale.

    FAILS TODAY, on purpose. The assertion message is the migration worklist and
    another agent works from it, so it is grouped and complete rather than short.
    Do not add an exemption to shrink it: the two admissible categories are spelled
    out above, and anything outside them is real drift.
    """
    violations = sorted(set(_app_spacing_violations()))
    by_prefix: dict[str, list[str]] = {}
    for v in violations:
        m = re.search(r"\.([a-z]+)-", v)
        by_prefix.setdefault(m.group(1) + "-" if m else "(page shells)", []).append(v)
    report = "\n".join(
        f"\n  === .{prefix} ({len(items)}) ==="
        + "".join(f"\n    {i}" for i in items)
        for prefix, items in sorted(by_prefix.items())
    )
    assert not violations, (
        f"{len(violations)} off-scale app-screen spacing declarations:{report}\n\n"
        "Use --s-1(4) --s-2(8) --s-3(12) --s-4(16) --s-5(20) --s-6(24) --s-8(32) "
        "--s-10(40) --s-12(48); there is deliberately no --s-7/--s-9/--s-11 and "
        "adding one is out of scope. --row-px is the +EV row's shared horizontal "
        "padding. A calc()/clamp()/min()/max() must name at least one var(--…) or "
        "it is not a derived value, it is a literal in a wrapper."
    )


def test_every_app_spacing_exemption_still_matches_a_real_site():
    """A stale exemption is a standing permission for whatever lands on its key
    next. This is the guard test_ios_tokens.py added after the same realisation;
    it caught a mis-keyed entry within hours of being written.
    """
    seen = {
        f"{sel} -> {prop}:{value}"
        for sel, prop, value in _spacing_declarations(
            lambda s: _has_class(s) and not is_marketing(s)
        )
    }
    stale = sorted(set(APP_SPACING_EXEMPT) - seen)
    assert not stale, (
        "these app-spacing exemptions match no declaration — the site moved or "
        "was migrated, and the exemption is now a blank cheque. Delete or fix "
        "the key:\n  " + "\n  ".join(stale)
    )


def test_the_two_spacing_scopes_partition_the_stylesheet():
    """Every rule with a class is owned by exactly one of the two spacing tests.

    This is the point of writing the app scope as `not is_marketing` rather than as
    a second prefix list: a new page prefix (a `.st-*` settings screen, say) is
    covered the moment it is written, and the bare `.lp` / `.pp` shells — which
    matched neither list while the marketing filter was the only one — cannot fall
    between them again.

    Rules with NO class are in neither scope, deliberately: `html,body{margin:0}`
    and `body{padding-bottom:env(safe-area-inset-bottom)}` are the document reset
    and the iOS home-indicator inset, neither of which is page rhythm. That set is
    asserted small and named, so a third element-selector spacing rule shows up
    here rather than silently escaping both tests.
    """
    def in_marketing_scope(sel: str) -> bool:
        return is_marketing(sel)

    def in_app_scope(sel: str) -> bool:
        return _has_class(sel) and not is_marketing(sel)

    classless, unowned, shared = set(), set(), set()
    for selector, decls in rules(style_block(INDEX)):
        sel = selector.strip()
        if _has_class(sel):
            # Asserted over the real filter functions rather than reasoned about,
            # so a future edit to either one is checked instead of assumed.
            owners = in_marketing_scope(sel) + in_app_scope(sel)
            if owners == 0:
                unowned.add(sel)
            elif owners > 1:
                shared.add(sel)
            continue
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            prop = prop.strip().lower()
            if prop in _SPACING_PROPS or prop.startswith(("padding-", "margin-")):
                classless.add(f"{sel} -> {prop}:{' '.join(value.split())}")
    assert not unowned, f"class rules owned by neither spacing test: {sorted(unowned)}"
    assert not shared, f"class rules claimed by both spacing tests: {sorted(shared)}"
    assert classless == {
        "html,body -> margin:0",
        "html,body -> padding:0",
        "body -> padding-bottom:env(safe-area-inset-bottom)",
    }, (
        "element-selector spacing rules changed. These are in neither spacing "
        f"scope; if a new one is page rhythm it needs a home:\n  {sorted(classless)}"
    )


def test_the_spacing_parser_sees_what_it_claims_to_see():
    """The failure mode that matters is a zero denominator, not a wrong answer.

    A regex that stops matching turns both spacing tests into assertions over an
    empty list and the suite goes green while the rhythm rots. Floors are well
    under today's counts (234 app / 87 marketing declarations) so ordinary
    migration does not trip them.
    """
    app = _spacing_declarations(lambda s: _has_class(s) and not is_marketing(s))
    marketing = _spacing_declarations(is_marketing)
    assert len(app) >= 180, f"only {len(app)} app-screen spacing declarations found"
    assert len(marketing) >= 60, f"only {len(marketing)} marketing ones found"
    # The scale itself must still be reachable, or "goes through the scale" is
    # vacuous in the other direction: a sheet with no var(--s-*) at all would
    # fail loudly, but one where the pattern no longer matches them would not.
    assert sum(1 for _s, _p, v in app if "var(--s-" in v) >= 60
    assert split_lengths("calc(var(--row-px) - 3px)") == ["calc(var(--row-px) - 3px)"], (
        "split_lengths stopped respecting parens; every calc() in the sheet would "
        "now be reported as several off-scale literals"
    )


# ── The 34px filter-bar contract ──────────────────────────────────────────────
# Both filter bars are `align-items:flex-end` flex rows, so a control that is not
# exactly 34px tall does not merely look wrong — it drags its own 10.5px column
# LABEL out of line with its neighbours' labels. index.html records three shipped
# bugs of exactly that shape, in four separate comments:
#
#   * 3px  — `.ev-chip` computed to 31px from padding, pulling "LEAGUE"/"SIDE" 3px
#            below "PROP TYPE"/"MIN TRUE %" (comment above .ev-chip).
#   * 2px  — a native `<select>` computes 2px taller than an `<input>` at the same
#            font-size/padding because its intrinsic box reserves room for the OS
#            glyph, so "LEAGUE" sat 2px high (comment above .bd-f select).
#   * 9.5px— `align-self:center` measured the cluster against a two-row `.bd-f`
#            column and floated Clear/badge/pager 9.5px above the controls
#            (comment above .bd-clear).
#
# Plus the note above `.cp-btn-sm` recording that the filter-bar buttons are
# deliberately OFF the 34/39/46px button height scale to hold this row, and the
# note above `.ev-clear` recording that it was given an explicit `height` rather
# than padding because --s-3 top/bottom measured 42px. This is the most-repeated
# regression in the file's history and the heights were plain literals with no
# assertion anywhere.
#
# Membership was verified against the markup, not taken from the CSS: `.ev-filters`
# in ev-page.jsx contains `.ev-chip`, `.cp-input.cp-input-sm`, `.ev-stepper` and
# `.ev-clear`; `FiltersBar` in page-boards.jsx contains `.bd-f` (label + input or
# select), `.bd-clear`, `.bd-badge` and `.bd-pag`. Two findings from doing that:
#   * `.bd-chip` / `.bd-chips` are DEAD CSS — no JSX renders them — so they are
#     not part of this contract despite living beside `.bd-clear` in the sheet.
#   * `.ev-slip-toggle` IS a child of `.ev-filters`, mobile only, and is
#     deliberately not 34px: it is a full-width bar spanning the tile
#     (`padding:13px 16px`), pinned by the radius test below instead.
#
# THE CONTROLS REACH 34px BY DIFFERENT MEANS, and this test asserts each by its
# own mechanism rather than pretending they are uniform. See the three groups.

# Group 1: an explicit `height:34px`. `*{box-sizing:border-box}` is set globally,
# so this is the whole box including the 1px border — nothing else needs checking.
FILTER_BAR_EXPLICIT_34 = {
    ".ev-chip": "+EV bar: League / Side / Green Devils / Sort toggles",
    ".ev-clear": "+EV bar: Clear (explicit height by measurement, see its comment)",
    ".bd-f select": "Boards bar: League and Book pickers, appearance:none",
    ".bd-clear": "Boards bar: Clear",
    ".bd-badge": "Boards bar: the '# lines' count pill",
    ".bd-pag": "Boards bar: the pager's 34px outer box",
}

# Group 2: derived, and computable from the stylesheet alone.
#   .ev-stepper declares NO height. Its box is its tallest child (a 32px button)
#   plus its own 1px border top and bottom = 34px. Both halves are asserted, and
#   so is the absence of a `height` on the wrapper — adding one there would make
#   the two mechanisms fight silently.

# Group 3: derived from PADDING + FONT-SIZE + LINE-HEIGHT, and therefore NOT
# computable here — the content height depends on Inter's metrics at 13px, which
# is a font-rasteriser question, not a CSS one. `.cp-input-sm` and `.bd-f input`
# are the two, and they are UNGUARDED on the 34px outcome. What IS assertable is
# the recipe that was measured to produce it, and the fact that the two must agree
# with each other: they sit in different bars beside controls pinned to 34px, so if
# one drifts, that bar's labels go out of line. So this test pins
# `padding:8px 10px` + `font-size:13px` + a 1px border on both, and a change to any
# of those fails here and asks for a re-measure in a browser rather than silently
# shipping a 2px offset. Nothing in this suite renders anything, so 34px itself
# cannot be verified for these two by any test in the repo.
FILTER_BAR_PADDING_DERIVED = {
    ".cp-input-sm": ".bd-f input",
}
_INPUT_RECIPE = {"padding": "8px 10px", "font-size": "13px"}


def _decls_for(selector: str) -> list[tuple[str, str]]:
    """Every `(prop, value)` declared for a selector, across all rules and all
    comma groups, in source order. Multiple rules matter: `.bd-f select` gets its
    border from `.bd-f input,.bd-f select` and its height from its own rule, and
    `.cp-input-sm` gets everything but padding/font-size from `.cp-input`."""
    out = []
    for sel, decls in rules(style_block(INDEX)):
        if selector not in [" ".join(p.split()) for p in sel.split(",")]:
            continue
        for decl in declarations(decls):
            prop, _, value = decl.partition(":")
            out.append((prop.strip().lower(), " ".join(value.split())))
    return out


def test_filter_bar_controls_with_an_explicit_height_are_all_34px():
    problems = []
    for selector, role in FILTER_BAR_EXPLICIT_34.items():
        heights = [v for p, v in _decls_for(selector) if p == "height"]
        if not heights:
            problems.append(f"{selector} ({role}) declares no height at all")
        # EVERY copy, not just the first: a later @media or density-override rule
        # wins on source order, and this row's alignment is viewport-independent.
        for h in heights:
            if h != "34px":
                problems.append(f"{selector} ({role}) -> height:{h}, must be 34px")
    assert not problems, (
        "the filter-bar 34px contract is broken:\n  " + "\n  ".join(problems)
        + "\n\nBoth bars are align-items:flex-end, so an off-height control also "
          "drags its own column label out of line with its neighbours'. See the "
          "comments above .ev-chip, .bd-f select, .bd-clear and .cp-btn-sm for the "
          "three offsets (3px / 2px / 9.5px) this has already shipped."
    )


def test_the_ev_stepper_derives_to_34px():
    """32px button + the wrapper's 1px border top and bottom."""
    wrapper = _decls_for(".ev-stepper")
    button = _decls_for(".ev-stepper button")
    assert not [v for p, v in wrapper if p == "height"], (
        ".ev-stepper must NOT declare a height — its 34px comes from its child "
        "plus its own border, and a literal here would let the two disagree"
    )
    border = [v for p, v in wrapper if p == "border"]
    assert border and border[0].startswith("1px"), (
        f".ev-stepper's border must be 1px (it is 2 of the 34px): {border}"
    )
    heights = [v for p, v in button if p == "height"]
    assert heights == ["32px"], (
        f".ev-stepper button must be 32px so the bordered wrapper is 34px: {heights}"
    )


def test_the_padding_derived_filter_inputs_share_one_recipe():
    """`.cp-input-sm` and `.bd-f input` reach 34px through font metrics this suite
    cannot compute (see Group 3 above). Their inputs are pinned instead, and they
    are pinned to be IDENTICAL, because each sits beside controls held at an
    explicit 34px in a different filter bar."""
    problems = []
    for a, b in FILTER_BAR_PADDING_DERIVED.items():
        for selector in (a, b):
            got = dict(_decls_for(selector))
            for prop, want in _INPUT_RECIPE.items():
                if got.get(prop) != want:
                    problems.append(
                        f"{selector} -> {prop}:{got.get(prop)}, measured recipe is "
                        f"{prop}:{want}"
                    )
        # .cp-input-sm carries no border of its own; it is always rendered as
        # `class="cp-input cp-input-sm"` (all four sites in ev-page.jsx), so the
        # 1px comes from .cp-input. Asserted on the source of the border rather
        # than on the modifier, or this would fail for the wrong reason.
        for selector in (".cp-input", b):
            borders = [v for p, v in _decls_for(selector) if p == "border"]
            if not (borders and borders[0].startswith("1px")):
                problems.append(f"{selector}'s border must be 1px: {borders}")
    assert not problems, (
        "the two padding-derived filter inputs left their measured recipe:\n  "
        + "\n  ".join(problems)
        + "\n\nThese two are the ONLY filter-bar controls whose 34px this suite "
          "cannot verify — the content height is Inter's line box at 13px. If you "
          "change any of these values, measure the rendered height in a browser "
          "against .ev-chip / .bd-f select before committing."
    )


def test_the_boards_pager_button_stays_inside_the_34px_row():
    """`.bd-pag-btn` is 28px INSIDE the 34px `.bd-pag`, which its comment calls
    out by name. A pager button grown to 34px+ would set the row height itself and
    the pinned `.bd-pag` above would become decorative."""
    heights = [v for p, v in _decls_for(".bd-pag-btn") if p == "height"]
    assert heights == ["28px"], f".bd-pag-btn must be 28px inside .bd-pag: {heights}"


def test_every_filter_bar_selector_still_exists():
    """Stale-key guard. A renamed control makes `_decls_for` return nothing, and
    the height loop above would then have no height to disagree with — the
    zero-denominator failure. This turns that into a named error."""
    css = style_block(INDEX)
    declared = {" ".join(p.split()) for sel, _ in rules(css) for p in sel.split(",")}
    expected = (
        set(FILTER_BAR_EXPLICIT_34)
        | set(FILTER_BAR_PADDING_DERIVED)
        | set(FILTER_BAR_PADDING_DERIVED.values())
        | {".ev-stepper", ".ev-stepper button", ".bd-pag-btn", ".cp-input"}
    )
    missing = sorted(expected - declared)
    assert not missing, (
        "filter-bar selectors named by the 34px contract no longer exist — "
        f"renamed? {missing}"
    )


# ── Derived radii ─────────────────────────────────────────────────────────────
# `test_every_radius_goes_through_a_token` above only asks whether `var(--r`
# appears SOMEWHERE in the value, so `calc(var(--r-lg) + 1px)` where the design
# needs `- 1px` passes it, and the symptom is a corner that misses its container by
# 2px — which reads as a rendering bug, not as a style choice. These three rules
# compute a radius against a neighbour's radius and must stay exactly right.
#
# The arithmetic follows one rule with two directions, and it is the reason the
# offsets are not interchangeable:
#
#   * A ring OUTSIDE a box needs `+ border-width`. `.pp-card-glow` is
#     `position:absolute;inset:-1px` over a `--r-xl` `.pp-card`, i.e. its own box
#     is 1px larger on every side. A circle of radius R offset outward by 1px has
#     radius R+1; at plain `var(--r-xl)` the glow's corner cuts across the card's.
#   * A fill INSIDE a bordered box needs `- border-width`. `.ev-slip-toggle`
#     (mobile) sits flush on the bottom inside edge of the `--r-lg` `.ev-filters`
#     tile, whose 1px border occupies the outer 1px of that curve. The inner curve
#     is therefore R-1; at plain `var(--r-lg)` the fill's corner pokes through the
#     border.
#   * `.ev-slip.ev-slip-mobile` takes the radius UNCHANGED, and that is not an
#     inconsistency: the drawer is a sibling of the tile, not a child, and paints
#     its own 1px border, so its outer curve is the tile's outer curve.
#
# All three are the BOTTOM corners only (`0 0 <r> <r>`) because each joins a
# square-cornered edge above it. A four-corner value on any of them is the same bug
# in a different disguise, so the whole expression is pinned rather than just the
# calc.
DERIVED_RADII = {
    ".pp-card-glow": (
        "calc(var(--r-xl) + 1px)",
        "a ring 1px OUTSIDE a --r-xl card (inset:-1px) — outward offset ADDS the "
        "border width; .pp-card must stay --r-xl for this to hold",
    ),
    ".ev-slip-toggle": (
        "0 0 calc(var(--r-lg) - 1px) calc(var(--r-lg) - 1px)",
        "a fill flush INSIDE the --r-lg .ev-filters tile's 1px border — inward "
        "offset SUBTRACTS the border width; top corners square because the "
        "toggle's top edge is a straight seam against the filter fields",
    ),
    ".ev-slip.ev-slip-mobile": (
        "0 0 var(--r-lg) var(--r-lg)",
        "the drawer is a SIBLING that paints its own 1px border, so its outer "
        "curve equals the tile's outer curve — no offset; top corners square "
        "because it joins the tile below its border (border-top:0)",
    ),
}


def test_derived_radii_keep_their_exact_offsets():
    problems = []
    for selector, (expected, why) in DERIVED_RADII.items():
        got = [v for p, v in _decls_for(selector) if p == "border-radius"]
        if not got:
            problems.append(f"{selector} declares no border-radius (renamed?) — {why}")
            continue
        # Every copy must agree. `.ev-slip-toggle` is declared twice (a base rule
        # with no radius plus the @media 900 rule that has it) and `.is-on`
        # squares it off in a third; only the ones that DO set a radius are here,
        # and all of them must be the pinned expression.
        for value in got:
            if squash(value) != squash(expected):
                problems.append(
                    f"{selector} -> border-radius:{value}\n      expected: "
                    f"{expected}\n      because:  {why}"
                )
    assert not problems, (
        "derived radii drifted — a wrong offset here reads as a rendering bug:\n  "
        + "\n  ".join(problems)
    )


def test_the_radius_offsets_still_have_the_containers_they_derive_from():
    """The premise of the test above. `calc(var(--r-xl) + 1px)` is only correct
    while `.pp-card` is `--r-xl`; if the card moves to `--r-lg` the glow silently
    becomes 5px too round, and pinning the expression alone would defend the bug.
    """
    for owner, token in ((".pp-card", "var(--r-xl)"), (".ev-filters", "var(--r-lg)")):
        radii = [squash(v) for p, v in _decls_for(owner) if p == "border-radius"]
        assert radii and all(r == squash(token) for r in radii), (
            f"{owner} must stay {token} — the derived radii above are computed "
            f"against it, got {radii}"
        )
    # And .ev-filters' border must still be the 1px the `- 1px` subtracts.
    border = [v for p, v in _decls_for(".ev-filters") if p == "border"]
    assert border and border[0].startswith("1px"), (
        f".ev-filters' border is what `calc(var(--r-lg) - 1px)` subtracts: {border}"
    )


# ── Elevation on the flattened surfaces ───────────────────────────────────────
# The spec asked for `--shadow-card` as the replacement lift for the gradient the
# flattening removed. It landed on ZERO new surfaces: the token's only two
# consumers are `.cp-modal` and `.pp-card`, both of which had it before.
#
# "None, and here is why" is a legitimate answer, so this test does not assert a
# direction — it asserts that a DECISION EXISTS for every flattened surface, and
# then enforces whichever way each one went. Every key of FLATTENED_SURFACES must
# appear in exactly one of the two dicts below, so a newly-flattened surface cannot
# be added without someone choosing. That cross-check is the whole mechanism; the
# dicts are the record.
#
# Read CARD_ELEVATION being empty as the deliberate current answer, not as a test
# that does nothing: the twelve surfaces below are classified and enforced, and the
# `--shadow-card` token itself is asserted alive with real consumers, so the
# denominators are 12 and 2 rather than 0.
#
# If the elevation pass concludes that some app surfaces DO take the shadow, move
# their keys from CARD_NO_ELEVATION to CARD_ELEVATION with the reason. This test
# fails until that move happens, on purpose — the failure IS the "an elevation
# decision was made about this surface" record.
CARD_ELEVATION: dict[str, str] = {
    # Empty. `.pp-card` is not here even though it carries the token, because it
    # is not one of the flattened surfaces' elevation decisions — it is the pricing
    # hero, on the marketing surface, and it kept a shadow it always had. Adding it
    # would make the partition assertion below meaningless (the set must be a
    # subset of FLATTENED_SURFACES for the cross-check to mean anything).
}

CARD_NO_ELEVATION: dict[str, str] = {
    # The two recessed wells. A drop shadow implies the surface sits ABOVE its
    # container; both of these sit below theirs, so a shadow would fight the
    # geometry rather than restore the lost lift.
    ".ev-slip": "a sidebar rail sunk beside .ev-table (--bg-2), not a raised card",
    ".cal-curves": "a chart well sunk inside .an-panel (--bg), not a raised card",
    # Full-bleed panels that span the page gutter. Their separation is the page
    # gutter plus a hairline; a shadow on something whose edges reach the viewport
    # has nothing to cast onto.
    ".ev-filters": "full-width config tile; separated by gutter + --hair",
    ".ev-table": "full-width list surface; separated by gutter + --hair",
    ".bd-filters": "full-width filter tile; separated by gutter + --hair",
    ".bd-tbl-wrap": "full-width board panel; separated by gutter + --hair",
    ".an-panel": "full-width analytics panel; separated by gutter + --hair",
    # The sticky table header. It must be indistinguishable from .bd-tbl-wrap or it
    # bands as you scroll (see FLATTENED_SURFACES' note), so its elevation is not
    # its own decision — it is whatever .bd-tbl-wrap's is.
    ".bd-tbl thead th": "must match .bd-tbl-wrap exactly or the header bands",
    # Cards in a grid. These are the surfaces the spec's replacement lift was
    # actually about, and they are the ones an elevation pass would most plausibly
    # change. Recorded as "no" because that is what ships today, NOT because the
    # question is settled.
    ".bt-card": "summary tile in .bt-summary — no shadow today, pending the pass",
    ".bt-slip": "slip card in .bt-slips-grid — no shadow today, pending the pass",
    # Marketing. Listed because FLATTENED_SURFACES lists them, and the partition
    # has to be total.
    ".pp-card": "carries --shadow-card already, and predates the flattening",
    ".pp-faq details": "an accordion row in a stack; a shadow per row would stripe",
}


def _box_shadows(selector: str) -> list[str]:
    return [v for p, v in _decls_for(selector) if p == "box-shadow"]


def test_every_flattened_surface_has_an_elevation_decision():
    """The cross-check. FLATTENED_SURFACES is the list of surfaces whose gradient
    lift was removed; each one owes an answer about what replaced it."""
    classified = set(CARD_ELEVATION) | set(CARD_NO_ELEVATION)
    undecided = sorted(set(FLATTENED_SURFACES) - classified)
    invented = sorted(classified - set(FLATTENED_SURFACES))
    overlap = sorted(set(CARD_ELEVATION) & set(CARD_NO_ELEVATION))
    assert not undecided, (
        "these surfaces were flattened but have no elevation decision recorded. "
        "The gradient that gave them depth is gone; say whether --shadow-card "
        "replaces it and why, in CARD_ELEVATION or CARD_NO_ELEVATION:\n  "
        + "\n  ".join(undecided)
    )
    assert not invented, (
        "these keys are not flattened surfaces, so classifying them says nothing "
        f"about the flattening: {invented}"
    )
    assert not overlap, f"a surface cannot be in both dicts: {overlap}"


def test_elevated_card_surfaces_carry_shadow_card():
    """Whatever is in CARD_ELEVATION must actually have the token — not a
    hand-rolled shadow, and not --shadow-pop (which is the popover/modal depth)."""
    problems = []
    for selector, why in CARD_ELEVATION.items():
        shadows = _box_shadows(selector)
        if not shadows:
            problems.append(f"{selector} has no box-shadow but is elevated: {why}")
        elif not any("var(--shadow-card)" in squash(s) for s in shadows):
            problems.append(f"{selector} -> box-shadow:{shadows} should be var(--shadow-card)")
    assert not problems, "\n  ".join(problems)


def test_unelevated_card_surfaces_carry_no_shadow():
    """The other half of the pin. Without this, "no shadows on app surfaces" is a
    claim nothing holds — someone adds `box-shadow:var(--shadow-card)` to `.bt-card`
    and every test in this file still passes.

    Scoped to `box-shadow` on the surface rule itself. `--ring` focus shadows live
    on `:focus`/`:focus-visible` selectors, which are different selectors and are
    not matched by `_decls_for`.
    """
    problems = []
    for selector, why in CARD_NO_ELEVATION.items():
        if selector == ".pp-card":
            continue          # explicitly a shadow-carrying exception, see its reason
        for s in _box_shadows(selector):
            problems.append(f"{selector} -> box-shadow:{s}\n      recorded as: {why}")
    assert not problems, (
        "these surfaces are recorded as unelevated but now carry a shadow. If that "
        "is the intended elevation decision, move the key to CARD_ELEVATION with "
        "its reason — do not leave the record disagreeing with the sheet:\n  "
        + "\n  ".join(problems)
    )


def test_shadow_card_is_still_a_live_token():
    """Guards against the empty CARD_ELEVATION reading as "the token is dead".

    It is not: `.cp-modal` and `.pp-card` use it. If those go too, `--shadow-card`
    should be deleted from :root rather than left as a definition nothing reads —
    and this test is where that gets noticed.
    """
    css = style_block(INDEX)
    consumers = sorted(
        selector.strip()
        for selector, decls in rules(css)
        if selector.strip() != ":root" and "var(--shadow-card)" in squash(decls)
    )
    assert consumers == [".cp-modal", ".pp-card"], (
        f"--shadow-card's consumers changed: {consumers}. Adding one to an app "
        "surface is an elevation decision — record it in CARD_ELEVATION. Removing "
        "both leaves a token nothing reads."
    )

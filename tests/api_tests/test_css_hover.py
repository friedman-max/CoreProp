"""CLAUDE.md's "Hover *darkens*" rule, as an invariant instead of a convention.

The rule is written down (`:root` note at `.cp-btn-primary:hover`: "Lightening it
(the obvious instinct) pushed white-on-blue back under 4.5:1 — a hover state that
fails contrast fails while the user is actively pointing at it") and it was
followed everywhere the accent is involved. Nothing checked it, and the cost was
`.bt-slip-place-install`, a live gradient button whose `:hover` lightens both
stops, which survived three phases of design review. `css_helpers.has_accent`
cannot see it — its blues are hand-mixed and share no value with the palette — and
the gradient ban cannot either, for the same reason. Only a luminance comparison
reaches it.

**What this test is honest about.** Only a rule whose base background AND hover
background both resolve to opaque colours can be compared; everything else is
skipped and COUNTED, and the counts are in the assertion message so the test's
coverage is visible rather than implied. A guard that silently examined three of
33 rules and passed is the failure mode here, so the denominators are asserted
too (`test_the_hover_parser_sees_what_it_claims_to_see`).

The skip categories, all of them real in this sheet:

  * **Base sets no background.** `.ev-row-data:hover` tints a row whose base rule
    declares no background at all (it inherits `.ev-table`'s), and
    `.cp-menu-item.is-danger:hover` has no `.cp-menu-item.is-danger` base — only
    `.cp-menu-item`. Resolving those means cascade analysis: walking compound
    selectors up to their less-specific ancestors, which is a different and much
    larger program, and one wrong step there produces a false accusation.
  * **Either side is translucent.** A `rgba(255,255,255,.04)` hover over a
    `transparent` base composites onto an unknown backdrop; its rendered lightness
    depends on what is behind the element, which this parser does not know. That
    is the whole ghost-button/row-tint family (11+ rules) and it is why the guard
    structurally cannot see that they all *lighten* — the same class of stated
    limit as `has_accent`'s "hand-mixed accent-family blues". If those should be
    held to the rule too, it needs a compositing model and a known backdrop per
    element, not a looser matcher here.

Gradients are compared on the LIGHTEST stop of each side ("no part of the hover
surface may be lighter than the lightest part of the base"). Strictest of the
obvious readings, and it is the one that catches a lightened highlight stop on an
otherwise-equal gradient.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import (
    INDEX,
    declarations,
    has_accent,
    rules,
    squash,
    style_block,
)

# ── Colour parsing ────────────────────────────────────────────────────────────
# Deliberately small: hex (3, 6 and 8 digit), comma-form rgb()/rgba(), the two
# keywords this sheet uses, and one level of var() indirection against :root.
# Space-separated CSS Color 4 (`rgb(30 111 176 / 40%)`), hsl() and colour-mix()
# are NOT parsed — there are zero usages, and an unparsed value is *skipped and
# counted*, never silently treated as compliant.

_KEYWORDS = {
    "white": (255, 255, 255, 1.0),
    "#fff": (255, 255, 255, 1.0),
    "black": (0, 0, 0, 1.0),
    # `transparent` is alpha 0: an unknown-backdrop composite, so it lands in the
    # skip bucket rather than reading as black (which would make every ghost
    # button's hover "lighter than its base" and be nonsense).
    "transparent": (0, 0, 0, 0.0),
}


def root_tokens(css: str) -> dict[str, str]:
    m = re.search(r":root\s*\{(.*?)\}", css, re.S)
    assert m, ":root block not found"
    out = {}
    for decl in declarations(m.group(1)):
        if decl.startswith("--"):
            name, _, value = decl.partition(":")
            out[name.strip()] = value.strip()
    return out


def resolve_var(value: str, tokens: dict[str, str], depth: int = 3) -> str:
    """Expand `var(--x)` / `var(--x, fallback)` against :root, recursively.

    `--radius: var(--r-lg)` proves one level is not enough, and `.bt-del-confirm`
    uses the fallback form (`var(--red,#ef4444)`), which a name-only pattern
    misses. The fallback is used only when the name is undefined — matching CSS,
    and matching the intent, since a defined token is what actually renders.
    """
    for _ in range(depth):
        m = re.search(r"var\(\s*(--[\w-]+)\s*(?:,([^)]*))?\)", value)
        if not m:
            return value
        name, fallback = m.group(1), (m.group(2) or "").strip()
        replacement = tokens.get(name, fallback)
        if not replacement:
            return value
        value = value[: m.start()] + replacement + value[m.end() :]
    return value


def parse_color(text: str) -> tuple[int, int, int, float] | None:
    """`(r, g, b, alpha)` or None if this is not a colour we can read."""
    t = squash(text)
    if t in _KEYWORDS:
        return _KEYWORDS[t]
    m = re.fullmatch(r"#([0-9a-f]{3,8})", t)
    if m:
        h = m.group(1)
        if len(h) == 3:
            h = "".join(c * 2 for c in h)
        if len(h) in (6, 8):
            r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
            a = int(h[6:8], 16) / 255 if len(h) == 8 else 1.0
            return (r, g, b, a)
        return None
    m = re.fullmatch(r"rgba?\(([^)]*)\)", t)
    if m:
        parts = [p for p in m.group(1).split(",") if p]
        if len(parts) not in (3, 4):
            return None
        try:
            r, g, b = (int(float(p)) for p in parts[:3])
        except ValueError:
            return None
        a = 1.0
        if len(parts) == 4:
            try:
                a = float(parts[3].rstrip("%")) / (100 if "%" in parts[3] else 1)
            except ValueError:
                return None
        return (r, g, b, a)
    return None


def relative_luminance(rgb: tuple[int, int, int]) -> float:
    """WCAG 2.x relative luminance. The same curve the contrast ratios quoted all
    over index.html were computed with, so "lighter" here means the same thing it
    means in those comments."""
    def channel(c: int) -> float:
        s = c / 255
        return s / 12.92 if s <= 0.04045 else ((s + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


_COLOR_TOKEN = re.compile(
    r"#[0-9a-fA-F]{3,8}|rgba?\([^)]*\)|\bwhite\b|\bblack\b|\btransparent\b"
)


def surface_colors(value: str) -> list[tuple[int, int, int, float]] | None:
    """Every colour stop of a background value, or None if any of it is unreadable.

    A gradient contributes each of its stops; a flat fill contributes one colour.
    Returning None for a partially-readable value is deliberate: a gradient with
    one stop this parser cannot read has an unknown lightest stop, so comparing it
    would be guessing.
    """
    v = value.strip()
    if not v:
        return None
    if re.search(r"(linear|radial|conic)-gradient\(", v, re.I):
        found = _COLOR_TOKEN.findall(v)
        if not found:
            return None
        stops = [parse_color(c) for c in found]
        return None if any(s is None for s in stops) else stops  # type: ignore[return-value]
    # A url()/image background has no single lightness; not comparable.
    if "url(" in squash(v):
        return None
    one = parse_color(v.replace("!important", "").strip())
    return None if one is None else [one]


# ── Rule collection ───────────────────────────────────────────────────────────

_BG_PROPS = ("background", "background-color")


def background_map(css: str) -> dict[str, str]:
    """Last-declared background per single selector, comma-groups split apart.

    Last-wins, not first: `.ev-slip-toggle` and `.bd-page` are each re-declared in
    the density-override and @media blocks at the end of the sheet, and those
    copies are the ones that render. Reading the first declaration would compare a
    hover against a base that the browser never paints.

    A comma group is split so `.bd-f input,.bd-f select` answers for both halves
    independently — a hover on one of them must resolve.
    """
    out: dict[str, str] = {}
    for selector, decls in rules(css):
        values = [
            v.strip()
            for d in declarations(decls)
            for p, _, v in [d.partition(":")]
            if p.strip().lower() in _BG_PROPS
        ]
        if not values:
            continue
        for part in selector.split(","):
            out[" ".join(part.split())] = values[-1]
    return out


def base_of(hover_selector: str) -> list[str]:
    """Candidate base selectors for a `:hover` rule, most literal first.

    Two forms, and the second is not optional: eight rules in this sheet are
    written `:hover:not(:disabled)`, whose base is the plain selector — the
    `:not(:disabled)` half exists to stop the hover applying to a disabled button,
    not to describe a different element. Stripping only `:hover` yields
    `.bt-slip-place:not(:disabled)`, which no rule declares, and every one of those
    eight would land in the "no base" skip bucket for a purely syntactic reason.
    """
    plain = hover_selector.replace(":hover", "")
    stripped = re.sub(r":not\([^)]*\)", "", plain)
    return [plain] if stripped == plain else [plain, stripped]


def hover_backgrounds(css: str) -> list[tuple[str, str]]:
    """`(selector, background value)` for every `:hover` rule that paints."""
    out = []
    for selector, decls in rules(css):
        values = [
            v.strip()
            for d in declarations(decls)
            for p, _, v in [d.partition(":")]
            if p.strip().lower() in _BG_PROPS
        ]
        if not values:
            continue
        for part in selector.split(","):
            sel = " ".join(part.split())
            if ":hover" in sel:
                out.append((sel, values[-1]))
    return out


# The rule is two-part, and only half of it was ever written down.
#
# "Hover darkens" is about COLOURED fills — an accent or semantic surface with a
# label on top, where lightening costs contrast on the label while the user is
# actively pointing at it. That is the reason given at `.cp-btn-primary:hover`,
# and it is why the accent family is held to it strictly.
#
# NEUTRAL surfaces do the opposite, deliberately: on a `#0a0a0d` page there is
# almost nothing below `#1b1b26` to darken *into*, so the affordance has to go
# up. The sheet ships at least eight of these — `.cp-btn-ghost`, `.cp-tab`,
# `.cp-menu-item`, `.cp-modal-x`, `.ev-stepper button`, `.lp-game-mute`,
# `.lp-game-ctap-x`, and Phase 2b's `.ev-row-data` row hover, which replaced a
# blue tint precisely so that hovering a row would not read as selecting it. All
# eight are spelled `rgba(255,255,255,.025-.06)`, so they land in this module's
# translucent skip bucket and the guard never had to have an opinion about them.
#
# `.ev-slip-toggle:hover` is the same convention spelled as an opaque hex, which
# is the only reason it is visible here at all: `#1b1b26` lightened by ~2.5% is
# `#20202c`, i.e. numerically what `rgba(255,255,255,.025)` would have produced.
# Exempting it is therefore consistency, not a carve-out — the alternative is a
# guard that enforces "neutral hovers must lighten via alpha", which is a spelling
# rule masquerading as a contrast rule.
#
# Keys are whole selectors. Each entry must state why the surface is neutral, and
# a coloured fill must never appear here.
#
#   .ev-slip-toggle:hover : the mobile slip drawer's open/close control. Base
#                           `#1b1b26` is a neutral chrome surface on `--bg-2`
#                           with `--text` on it, no accent involved, and the
#                           label's contrast IMPROVES as the fill lightens — the
#                           inverse of the failure mode the rule exists to stop.
NEUTRAL_LIGHTENING_OK = {
    ".ev-slip-toggle:hover",
}


def _analyse() -> tuple[list[str], dict[str, list[str]]]:
    """`(violations, skips_by_reason)`."""
    css = style_block(INDEX)
    tokens = root_tokens(css)
    bgs = background_map(css)
    violations: list[str] = []
    skips: dict[str, list[str]] = {"no resolvable base rule": [], "translucent or unreadable": []}

    for sel, hover_value in hover_backgrounds(css):
        if sel.strip() in NEUTRAL_LIGHTENING_OK:
            continue
        base_value = None
        for candidate in base_of(sel):
            if candidate in bgs:
                base_value = bgs[candidate]
                break
        if base_value is None:
            skips["no resolvable base rule"].append(f"{sel} (hover {hover_value})")
            continue

        hov = surface_colors(resolve_var(hover_value, tokens))
        bas = surface_colors(resolve_var(base_value, tokens))
        if hov is None or bas is None or any(c[3] < 1 for c in hov + bas):
            skips["translucent or unreadable"].append(
                f"{sel}: base {base_value} -> hover {hover_value}"
            )
            continue

        hov_l = max(relative_luminance(c[:3]) for c in hov)
        bas_l = max(relative_luminance(c[:3]) for c in bas)
        # 1e-9, not 0: identical colours must pass, and several state buttons
        # deliberately repeat their base fill on hover (a queued/error CTA does
        # not respond to the pointer). "No lighter" allows equal.
        if hov_l > bas_l + 1e-9:
            violations.append(
                f"{sel}\n      base  {base_value}  (L={bas_l:.4f})"
                f"\n      hover {hover_value}  (L={hov_l:.4f})"
            )
    return violations, skips


def test_hover_backgrounds_never_lighten():
    violations, skips = _analyse()
    total = len(hover_backgrounds(style_block(INDEX)))
    skipped = sum(len(v) for v in skips.values())
    coverage = (
        f"\n\nCoverage: {total} :hover rules set a background; "
        f"{total - skipped} resolved, {skipped} skipped "
        + ", ".join(f"{len(v)} {k}" for k, v in skips.items())
        + ".\nSkipped (not compliant — unverifiable):\n    "
        + "\n    ".join(sorted(x for v in skips.values() for x in v))
    )
    assert not violations, (
        f"{len(violations)} :hover background(s) LIGHTER than their base "
        f"(CLAUDE.md: hover darkens):\n    " + "\n    ".join(sorted(violations)) + coverage
    )


def test_every_neutral_lightening_exemption_still_matches_a_real_site():
    """A dead exemption is how an allowlist quietly becomes policy.

    Same guard `test_css_tokens` and `test_ios_tokens` carry: if the selector is
    renamed or the rule deleted, the entry fails here instead of sitting in the
    file looking like a decision somebody made about the current code.
    """
    live = {sel.strip() for sel, _ in hover_backgrounds(style_block(INDEX))}
    dead = sorted(NEUTRAL_LIGHTENING_OK - live)
    assert not dead, (
        "these NEUTRAL_LIGHTENING_OK entries match no :hover rule that sets a "
        "background — delete them or fix the key:\n    " + "\n    ".join(dead)
    )


def test_the_neutral_lightening_premise_holds():
    """The exemption is only defensible while the surface stays neutral.

    Exact-matching the selector is not enough: if someone repaints
    `.ev-slip-toggle` with the accent, the entry would go on excusing a lightening
    hover on a coloured fill — which is the one thing the rule is actually for.
    So assert the premise, not just the key. `has_accent` is the same helper the
    gradient ban uses, so "accent" means the same thing in both places.
    """
    css = style_block(INDEX)
    bgs = background_map(css)
    for sel in sorted(NEUTRAL_LIGHTENING_OK):
        for candidate in base_of(sel):
            if candidate in bgs:
                base = bgs[candidate]
                assert not has_accent(f"background:{base}"), (
                    f"{sel} is exempt from hover-darkens on the grounds that its "
                    f"surface is neutral, but its base is now {base!r}, which is "
                    "in the accent family. Either revert the fill or remove the "
                    "exemption — a coloured fill must darken on hover."
                )
                break


def test_the_hover_parser_sees_what_it_claims_to_see():
    """Denominators, so this cannot pass on an empty set.

    Floors are under today's counts (33 hover backgrounds, 14 resolvable) but far
    above zero, so a broken selector or colour pattern fails here instead of
    turning the test above into `assert not []`.
    """
    css = style_block(INDEX)
    hovers = hover_backgrounds(css)
    assert len(hovers) >= 25, f"only {len(hovers)} hover backgrounds found"
    violations, skips = _analyse()
    resolved = len(hovers) - sum(len(v) for v in skips.values())
    assert resolved >= 10, (
        f"only {resolved} of {len(hovers)} hover rules could be resolved — the "
        "base-selector or colour parsing regressed, and the test above is now "
        "mostly skips"
    )
    # And the machinery each part depends on.
    tokens = root_tokens(css)
    assert resolve_var("var(--primary)", tokens) == "#1E6FB0"
    assert resolve_var("var(--radius)", tokens) == "16px", "nested var() lost"
    assert resolve_var("var(--nope,#abcdef)", tokens) == "#abcdef", "fallback lost"
    assert parse_color("#195F97") == (25, 95, 151, 1.0)
    assert parse_color("rgba(30,111,176,.10)") == (30, 111, 176, 0.1)
    assert parse_color("#0a0a12ff") == (10, 10, 18, 1.0)
    assert parse_color("hsl(210 50% 40%)") is None, "unreadable must be None, not a colour"
    assert relative_luminance((255, 255, 255)) > relative_luminance((0, 0, 0))
    assert base_of(".bt-slip-place:hover:not(:disabled)")[-1] == ".bt-slip-place"
    stops = surface_colors("linear-gradient(180deg,#1e3a5f,#15304f)")
    assert stops is not None and len(stops) == 2, "gradient stops not extracted"

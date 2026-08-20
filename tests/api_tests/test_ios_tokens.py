"""The iOS design-system contract, as invariants instead of shell greps.

`ios/CoreProp/` is the native SwiftUI client. It carries the same token system as
the web app (`Theme.swift` mirrors the `:root` block of `web/static/index.html`),
but until now it had no equivalent of `tests/api_tests/test_css_tokens.py` —
which is exactly why the web migration stayed migrated and the iOS one did not.
`Theme/Components.swift` was modernized and still shipped `padding: CGFloat = 14`,
`padding(.vertical, 44)`, `VStack(spacing: 6)` and a dozen more off-scale values,
because nothing looked.

Phase 4's definition of done was a list of `grep -rn` commands in
`docs/superpowers/plans/2026-08-19-frontend-modernization-phase-4.md`. A grep in a
planning document guards nothing once the phase closes. This module is that list,
promoted to pytest, and it is deliberately two things at once:

  * a **regression guard** for the four rules already satisfied (no inline hex,
    no accent gradient, no iOS 17 API, decorative-only `text4` at the sites that
    are already correct), and
  * a **worklist generator** for the ones that are not. Tests 4/5/6 fail today,
    and their assertion messages are the remaining migration. That is the same
    way the web phases were driven: `test_css_tokens.py`'s radius test was
    written failing and the commits that followed emptied it.

Do not exempt a value to make this file green. Three of these rules are being
migrated by other agents right now; a pre-emptive exemption permanently hides
their remaining work, which is the one failure mode that costs more than a red
test.

**Scope: `ios/CoreProp/**/*.swift` only.** `ios/CorePropKit/` is excluded on
purpose — it is a Foundation-only Swift package with no `import SwiftUI`, no
`Theme`, and no design tokens of any kind, and it is built and verified
separately (`swift run CorePropKitVerify`). The exclusion is asserted in
`test_the_parser_sees_what_it_claims_to_see` rather than left to the glob root,
because "the tests silently stopped covering half the tree" is precisely the
failure this module exists to prevent.

**On regex-parsing Swift.** There is no Swift parser available here (and adding
one for a styling check would be absurd), so this reads the source as text.
`_mask()` blanks `//` comments, `/* */` blocks and string-literal bodies before
any pattern runs, which removes the three false-positive classes that actually
bite: a doc comment saying "was 14" or "radiusSm 10 -> 12" (Theme.swift has
both), a commented-out call site, and a number inside a user-facing string. Two
approximations remain and are accepted:

  * **String interpolation is blanked with its string.** `"\\(foo)"` has its
    interpolated code masked along with the literal. A `.padding()` inside a
    string interpolation would be missed; there are none, and there is no
    plausible reason to write one.
  * **Argument extraction is paren-balanced, not type-aware.** `_call_args`
    balances `(`/`[`/`{`, so multi-line initialisers and nested calls are read
    correctly, but it cannot tell a `CGFloat` from an `Int` — it only asks
    whether the argument text *is a numeric literal*. That is sufficient here
    because the rule under test is literally "no bare numbers in these
    positions".

One coverage gap, recorded rather than skipped in silence: `BetsView.swift` and
`LinesView.swift` each carry a
`listRowInsets(EdgeInsets(top: 2, leading: 8, bottom: 2, trailing: 8))`, whose
`leading`/`trailing` are spacing-shaped values that no test here checks. That
position is outside the set this module was scoped to, and folding it in would
add four sub-4pt exemption keys for the `top`/`bottom` pair to catch two `8`s
that are already `Theme.s2`'s value. Named here so the next person knows it is a
decision and not an oversight.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_IOS_APP = _ROOT / "ios" / "CoreProp"

# `Theme.swift` is the token source and is therefore the one file allowed to
# contain raw colour literals; several tests key off it by name.
_THEME = _IOS_APP / "Theme" / "Theme.swift"


# ── Source loading and masking ────────────────────────────────────────────────


def _swift_files() -> list[Path]:
    """Every Swift file in the app target, sorted.

    Rooted at `ios/CoreProp` so nothing under `ios/CorePropKit` (the
    Foundation-only package) or `ios/CorePropTests` can be picked up.
    """
    if not _IOS_APP.is_dir():
        pytest.skip(f"{_IOS_APP.relative_to(_ROOT)} is absent (partial checkout)")
    return sorted(_IOS_APP.rglob("*.swift"))


def _mask(src: str) -> str:
    """Return `src` with comments and string-literal bodies replaced by spaces.

    Length and newline positions are preserved, so a match offset into the mask
    indexes the original source too — that is what lets violations be reported
    with real line numbers and real source text while being *found* in a
    comment-free view.

    Blanking rather than deleting matters: deleting would shift every offset
    after the first comment, and the whole point is to report `file:line`.
    """
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c == '"':
            # A single-quoted Swift string. There are no `"""` multi-line
            # literals and no raw (`#"..."#`) strings anywhere in this tree, so
            # the simple scanner is exact rather than merely adequate; a
            # multi-line literal would be caught by the newline break below and
            # left partly unmasked, which fails loud (a stray `"` desyncs the
            # scan) rather than quiet.
            j = i + 1
            while j < n and src[j] != '"':
                if src[j] == "\\":
                    j += 2
                    continue
                if src[j] == "\n":
                    break
                j += 1
            for k in range(i, min(j + 1, n)):
                if out[k] != "\n":
                    out[k] = " "
            i = j + 1
        elif c == "/" and src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j == -1 else j
            for k in range(i, j):
                out[k] = " "
            i = j
        elif c == "/" and src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def _sources() -> list[tuple[str, str, str]]:
    """`(label, original, masked)` per file. `label` is repo-relative."""
    out = []
    for path in _swift_files():
        src = path.read_text(encoding="utf-8")
        out.append((str(path.relative_to(_ROOT)), src, _mask(src)))
    return out


def _line_no(src: str, pos: int) -> int:
    return src.count("\n", 0, pos) + 1


def _fail(kind: str, violations: list[tuple[str, int, str]], remedy: str) -> None:
    """Assert with a de-duplicated, counted worklist sorted by file then line.

    The message is the deliverable — another agent migrates straight from it — so
    it carries one line per site plus the fix, not just a count. Sorting on
    `(label, line)` and not on the rendered string matters more than it looks:
    string order puts `:103` before `:88`, which shuffles a 129-item worklist out
    of file order and makes it unusable for a top-to-bottom pass.
    """
    items = sorted(set(violations))
    assert not items, (
        f"{len(items)} {kind}:\n  "
        + "\n  ".join(f"{label}:{line} -> {text}" for label, line, text in items)
        + f"\n\n{remedy}"
    )


# ── Argument extraction ───────────────────────────────────────────────────────

_NUMERIC = re.compile(r"^-?\d+(?:\.\d+)?$")


def _call_args(masked: str, open_paren: int) -> tuple[list[str], int]:
    """Split the top-level arguments of the call whose `(` sits at `open_paren`.

    Returns `(args, close_paren_index)`; `close_paren_index` is -1 if the call is
    unbalanced. Balances `(`/`[`/`{` so nested calls, array literals and
    multi-line initialisers all read correctly, and stops at the *matching* `)`
    so a trailing view-builder closure (`HStack(spacing: 8) { … }`) is not
    swallowed.
    """
    depth, start, i, n = 0, open_paren + 1, open_paren, len(masked)
    args: list[str] = []
    while i < n:
        c = masked[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                args.append(masked[start:i])
                return [a.strip() for a in args if a.strip()], i
        elif c == "," and depth == 1:
            args.append(masked[start:i])
            start = i + 1
        i += 1
    return [a.strip() for a in args if a.strip()], -1


def _labeled(args: list[str], label: str) -> str | None:
    """Value of `label:` among `args`, or None. Whole-label match, so `padding:`
    does not also answer for a hypothetical `paddingScale:`."""
    for a in args:
        m = re.match(rf"^{re.escape(label)}\s*:\s*(.*)$", a, re.S)
        if m:
            return m.group(1).strip()
    return None


def _label_sites(masked: str, label: str) -> list[tuple[int, int, str]]:
    """Every `label: <value>` argument, as `(label_pos, value_end, value)`.

    The value runs to the next comma or closer at the argument's own nesting
    depth, so `cornerRadius: 12, style: .continuous` yields `12`, and
    `cornerRadius: max(a, b)` yields the whole call.
    """
    out = []
    for m in re.finditer(rf"\b{re.escape(label)}\s*:", masked):
        i, depth, j = m.end(), 0, m.end()
        while j < len(masked):
            c = masked[j]
            if c in "([{":
                depth += 1
            elif c in ")]}":
                if depth == 0:
                    break
                depth -= 1
            elif c == "," and depth == 0:
                break
            j += 1
        out.append((m.start(), j, masked[i:j].strip()))
    return out


# ── 1. No inline colour literals outside Theme.swift ─────────────────────────
# Twenty-two `Color(hex: 0x…)` literals were migrated out of eleven feature
# files onto named tokens (`red2`/`green2`/`amber2`/`blue2`). The point of a
# token is that the palette has one home; a re-inlined hex is invisible to every
# other check in this file because it never mentions `Theme` at all.

_HEX_COLOR = re.compile(r"Color\s*\(\s*hex\s*:")


def test_no_inline_hex_outside_theme():
    violations = []
    for label, src, masked in _sources():
        if label == str(_THEME.relative_to(_ROOT)):
            continue
        for m in _HEX_COLOR.finditer(masked):
            args, close = _call_args(masked, masked.index("(", m.start()))
            text = src[m.start() : close + 1] if close > 0 else src[m.start() : m.end()]
            violations.append((label, _line_no(src, m.start()), text.strip()))
    _fail(
        "inline colour literals outside Theme.swift",
        violations,
        "Add a named token to Theme.swift and use it. The palette has one home; "
        "web's is the `:root` block, iOS's is Theme.swift.",
    )


# ── 2. One gradient, and it is semantic ───────────────────────────────────────
# Every card in the app painted a two-stop grey gradient
# (`cardGradTop` -> `cardGradBot`) on the reading that CLAUDE.md only banned
# gradients on *accent* surfaces. Cards are flat now.
#
# The surviving gradient is the P&L chart's area fill, and the distinction is the
# whole reason this test is not simply "no gradients": that fill is **semantic**
# — it is `[tone.opacity(0.24), .clear]` where `tone` is green or red by the sign
# of the bankroll, so the gradient encodes data. A decorative accent gradient
# encodes nothing and is what was removed. A blanket ban would delete a
# legitimate data encoding; an allowlist by file keeps the one that means
# something and still fails on the next card someone prettifies.

_PNL_CHART = "ios/CoreProp/Features/Account/AnalyticsView.swift"

# The two retired stops. They are deliberately retained in Theme.swift as unused
# constants (with a comment saying so) because Phase 4's rule was "remove
# gradient *usage*, change no hex value" — but they are also the exact material a
# future card gradient would be rebuilt from, so a *reference* to either outside
# Theme.swift is the same regression as a LinearGradient.
_RETIRED_GRAD_STOPS = ("Theme.cardGradTop", "Theme.cardGradBot")


def test_no_gradient_outside_the_pnl_chart():
    violations = []
    for label, src, masked in _sources():
        lines = src.splitlines()
        if label != _PNL_CHART:
            for m in re.finditer(r"\bLinearGradient\b", masked):
                n = _line_no(src, m.start())
                violations.append((label, n, lines[n - 1].strip()))
        if label == str(_THEME.relative_to(_ROOT)):
            continue  # the definitions themselves are fine; references are not
        for stop in _RETIRED_GRAD_STOPS:
            for m in re.finditer(re.escape(stop), masked):
                n = _line_no(src, m.start())
                violations.append((label, n, lines[n - 1].strip()))
    _fail(
        "decorative gradients / references to the retired card-gradient stops",
        violations,
        "Card surfaces are flat `Theme.card`. The only permitted gradient is "
        f"{_PNL_CHART}'s P&L area fill, which is allowed because it is semantic "
        "(its colour is the sign of the bankroll), not decorative accent.",
    )


def test_the_pnl_gradient_is_still_there_and_still_semantic():
    """Guard the premise of the test above.

    An allowlist entry for a file that no longer has a gradient would quietly
    turn `test_no_gradient_outside_the_pnl_chart` into a blanket ban that passes
    for the wrong reason — and if the P&L fill were flattened to a solid, the
    chart would stop encoding direction with no test noticing.
    """
    path = _ROOT / _PNL_CHART
    assert path.exists(), f"{_PNL_CHART} is gone; retire the gradient allowlist"
    masked = _mask(path.read_text(encoding="utf-8"))
    assert "LinearGradient" in masked, (
        f"{_PNL_CHART} no longer has a LinearGradient. If the P&L fill was "
        "intentionally flattened, delete the allowlist so the ban is total."
    )
    assert re.search(r"LinearGradient\s*\(\s*colors\s*:\s*\[\s*tone\b", masked), (
        "the P&L gradient is only allowed because it is keyed on `tone` (green "
        "or red by the sign of the bankroll). A gradient here that is not "
        "derived from the data is decorative and not exempt."
    )


# ── 3. No iOS 17+ API ─────────────────────────────────────────────────────────
# The deployment target is iOS 16. These symbols compile fine locally, because
# the only local typecheck available runs against the **macOS** SDK (there is no
# Xcode or iOS SDK on the dev box — see the Phase 4 plan's "verification
# situation"), so an iOS-17-only availability error appears for the first time in
# CI, minutes later and on someone else's push.

_IOS17_SYMBOLS = (
    "ContentUnavailableView",
    "@Observable",
    ".contentMargins",
    ".scrollTargetBehavior",
    ".containerRelativeFrame",
    ".visualEffect",
    ".symbolEffect",
    ".chartScrollableAxes",
)


def _onchange_sites(masked: str) -> list[tuple[int, list[str], str | None]]:
    """Every `onChange(of:)`, as `(pos, call_args, closure_param_list_or_None)`.

    The closure parameter list is what distinguishes the versions, and it is why
    this is not a one-line regex. The iOS 16 form is
    `onChange(of: x) { newValue in … }`; the iOS 17 form is
    `onChange(of: x) { old, new in … }`. The plan's grep approximated the `of:`
    argument as `[^)]*`, which breaks on any call inside it (`of: vm.load()`), so
    the argument list is paren-balanced here instead. A closure with no `in` at
    all (`{ save($0) }`) is legal iOS 16 and yields None.

    Known approximation: a multi-entry capture list (`{ [weak self, weak vm] v in`)
    contains a comma and would read as two parameters. There is none in this tree,
    and the false positive is loud and one word to fix, so it is not worth
    tokenizing Swift closures to avoid.
    """
    out = []
    for m in re.finditer(r"\.onChange\s*\(", masked):
        args, close = _call_args(masked, m.end() - 1)
        if close < 0:
            continue
        params = None
        # `\{` must be the next non-space thing, and the parameter list cannot
        # span a brace or a newline — that keeps `{ Task { await x } }` from
        # being read as a parameter list.
        tail = re.match(r"\s*\{\s*([^{}\n]*?)\s+in\b", masked[close + 1 :])
        if tail:
            params = tail.group(1).strip()
        out.append((m.start(), args, params))
    return out


def test_no_ios17_api():
    violations = []
    for label, src, masked in _sources():
        lines = src.splitlines()
        for symbol in _IOS17_SYMBOLS:
            for m in re.finditer(re.escape(symbol) + r"\b", masked):
                n = _line_no(src, m.start())
                violations.append((label, n, f"{symbol} in `{lines[n - 1].strip()}`"))
        for pos, args, params in _onchange_sites(masked):
            n = _line_no(src, pos)
            if params and "," in params:
                violations.append(
                    (label, n, f"two-parameter onChange `{{ {params} in` "
                               f"— iOS 17 only, in `{lines[n - 1].strip()}`")
                )
            if _labeled(args, "initial") is not None:
                violations.append(
                    (label, n, f"onChange(initial:) in `{lines[n - 1].strip()}`")
                )
    _fail(
        "iOS 17+ APIs on an iOS 16 deployment target",
        violations,
        "These fail only in CI: the local typecheck runs against the macOS SDK, "
        "which has no iOS availability information. The iOS 16 forms are "
        "`onChange(of:) { newValue in }`, `ObservableObject` + `@Published`, and "
        "a hand-rolled empty state.",
    )


# ── 4. text4 is decorative only ───────────────────────────────────────────────
# `Theme.text4` (#4A4A59) is 2.3:1 on the page background — below every WCAG
# threshold including 1.4.11's 3:1 for non-text UI. It exists for glyphs and
# disabled values, and web has made this exact mistake twice
# (`test_css_tokens.py::test_text4_is_only_on_decorative_glyphs`).
#
# Keyed on the whole stripped source line, matched entire — not on a line number
# (which churns under every neighbouring edit) and not on a substring (which
# would let a new label site inherit an existing permission). Reading the keys
# below tells you exactly what a legitimate site looks like.
#
# Residual risk, stated rather than hidden: because the key is the whole line, an
# unrelated edit to a listed line (a spacing migration on the same statement,
# say) breaks the exemption and fails this test. That is the safe direction —
# it asks for a re-read of a 2.3:1 site — but it is churn, not correctness.
#
# THREE SITES ARE DELIBERATELY ABSENT and are expected to fail until the
# in-flight `Features/**` migrations land: `BetDetailView`'s SIZE/POWER/FLEX
# table header, `LinesView`'s 8pt BEST/FAIR labels, and `SlipCard`'s
# PAYOUT/HITS/PROJ EV footer labels. All three are readable text. Adding them
# here would delete the only record of that work.
TEXT4_ALLOWED = {
    # EmptyStateView's 34pt `tray` glyph. A decorative illustration, not text —
    # and it sits above a `text2` title that carries the actual message.
    "ios/CoreProp/Theme/Components.swift": (
        ".foregroundColor(Theme.text4)",
    ),
    # The disabled state of the slip's EV readout: `text4` is reached only when
    # `ev == nil`, i.e. there is no number to read. The two live branches are
    # `green` / `red2`.
    "ios/CoreProp/Features/Slip/SlipView.swift": (
        "let color: Color = ev == nil ? Theme.text4 : (ev! >= 0 ? Theme.green : Theme.red2)",
    ),
    # Two dashed chart RuleMarks (the P&L zero line and the calibration
    # diagonal). Chart furniture at 2.3:1 is the intent — a grid rule that
    # competed with the series would be the bug.
    "ios/CoreProp/Features/Account/AnalyticsView.swift": (
        ".foregroundStyle(Theme.text4)",
    ),
    # The em-dash placeholder for a book with no price. Web allows the identical
    # case at `.bd-odds-empty`: a single glyph standing in for absent data.
    "ios/CoreProp/Features/Bets/BetDetailView.swift": (
        'Text("—").font(Theme.mono(13)).foregroundColor(Theme.text4)',
    ),
}


def test_text4_only_on_decorative_glyphs():
    violations = []
    for label, src, masked in _sources():
        allowed = TEXT4_ALLOWED.get(label, ())
        for m in re.finditer(r"\bTheme\.text4\b", masked):
            n = _line_no(src, m.start())
            line = src.splitlines()[n - 1].strip()
            if line in allowed:
                continue
            violations.append((label, n, line))
    _fail(
        "Theme.text4 (2.3:1) sites that are not decorative glyphs",
        violations,
        "Readable muted text is `Theme.text3` (AA-safe). `text4` is for glyphs "
        "and disabled values only — see Theme.swift's header.",
    )


def test_every_text4_exemption_still_matches_a_real_site():
    """A stale allowlist entry is a silent permission for whatever moves into its
    place. `test_css_tokens.py` pays for the same guard with its `missing` check.
    """
    seen = set()
    for label, src, masked in _sources():
        for m in re.finditer(r"\bTheme\.text4\b", masked):
            line = src.splitlines()[_line_no(src, m.start()) - 1].strip()
            seen.add((label, line))
    stale = [
        f"{label} | {line}"
        for label, lines in TEXT4_ALLOWED.items()
        for line in lines
        if (label, line) not in seen
    ]
    assert not stale, (
        "TEXT4_ALLOWED entries match nothing — the site moved or was fixed, and "
        "the exemption is now a blank cheque. Delete or update:\n  "
        + "\n  ".join(sorted(stale))
    )


# ── 5 & 6. The spacing and radius scales ──────────────────────────────────────
# `Theme.s1…s12` (4/8/12/16/20/24/32/40/48) and `Theme.rXl/rLg/rMd/rSm`
# (20/16/12/8) mirror web's `--s-*` and `--r-*`. The three legacy radius aliases
# resolve into the scale (`radius`->`rLg`, `radiusSm`->`rMd`, `radiusXs`->`rSm`)
# and stay legal because ~23 call sites use them.
#
# Only *numeric literals* are flagged. A pass-through identifier (`CardModifier`'s
# `padding` parameter, `radius` in the same struct) is not a literal — the literal
# lives at the default value or the call site, and both of those are checked here.
# A `Theme.*` token in the wrong position IS flagged, because using `Theme.rMd`
# as a gap crosses the two scales and would otherwise read as compliant.

_SPACING_TOKENS = frozenset(
    [f"Theme.s{n}" for n in (1, 2, 3, 4, 5, 6, 8, 10, 12)]
    # The focus ring's 4pt. It is a spacing-shaped value with its own name
    # because it must move with `--ring`, not with `--s-1`, even though they are
    # numerically equal today.
    + ["Theme.ringWidth"]
)

_RADIUS_TOKENS = frozenset(
    ["Theme.rXl", "Theme.rLg", "Theme.rMd", "Theme.rSm"]
    # The legacy aliases. Web deleted `--radius-xs` for having no consumers;
    # iOS's `radiusXs` has two, so all three aliases stay and all three resolve
    # into the scale above.
    + ["Theme.radius", "Theme.radiusSm", "Theme.radiusXs"]
)

# Containers whose `spacing:` argument is a gap on the scale. Scoped by name
# rather than matching `spacing:` anywhere, so that a `spacing:` on some future
# container is *reported by the parser guard* instead of being silently skipped —
# see `test_the_parser_sees_what_it_claims_to_see`.
_GAP_CONTAINERS = ("LazyVStack", "LazyHStack", "VStack", "HStack", "Grid", "ScrollView")


def _spacing_sites(src: str, masked: str) -> list[tuple[int, str, str]]:
    """`(pos, value, display_text)` for every spacing-shaped argument."""
    sites: list[tuple[int, str, str]] = []

    # `.padding(n)` and `.padding(.vertical, n)`.
    for m in re.finditer(r"\.padding\s*\(", masked):
        args, close = _call_args(masked, m.end() - 1)
        if close < 0 or not args:
            continue                      # `.padding()` — the SwiftUI default
        value = args[-1] if len(args) == 2 else args[0]
        if len(args) > 2:
            continue                      # not a shape this rule describes
        sites.append((m.start(), value, src[m.start() : close + 1].strip()))

    # `spacing:` on a stack / grid initialiser.
    for m in re.finditer(r"\b(?:" + "|".join(_GAP_CONTAINERS) + r")\s*\(", masked):
        args, close = _call_args(masked, m.end() - 1)
        if close < 0:
            continue
        value = _labeled(args, "spacing")
        if value is None:
            continue
        pos = masked.index("spacing", m.start())
        sites.append((pos, value, f"{m.group(0)[:-1]}(… spacing: {value})"))

    # `cpCard(padding: n)`.
    for m in re.finditer(r"\bcpCard\s*\(", masked):
        args, close = _call_args(masked, m.end() - 1)
        if close < 0:
            continue
        value = _labeled(args, "padding")
        if value is not None:
            sites.append((m.start(), value, f"cpCard(padding: {value})"))

    # The `padding:` DEFAULT, declared twice: once as `CardModifier`'s stored
    # property and once in the `cpCard` extension's signature. It is the single
    # highest-leverage spacing value in the app — it feeds all 13 bare
    # `.cpCard()` sites — and a call-site-only scan misses it entirely, which is
    # how `padding: CGFloat = 14` survived the file's own modernization. Both
    # declarations are reported because they must move together; changing one
    # silently splits the default in two.
    #
    # `[^,)\n]` and not `[^,)]`: a stored property is terminated by a newline,
    # not by `,` or `)`, so an unbounded class runs on into the next declaration
    # and swallows `func body(content: Content` as part of the value.
    for m in re.finditer(r"\bpadding\s*:\s*CGFloat\s*=\s*([^,)\n]+)", masked):
        value = m.group(1).strip()
        sites.append((m.start(), value, f"padding: CGFloat = {value}"))

    return sites


def _radius_sites(src: str, masked: str) -> list[tuple[int, str, str]]:
    """`(pos, value, display_text)` for every corner-radius argument."""
    sites: list[tuple[int, str, str]] = []

    for pos, _end, value in _label_sites(masked, "cornerRadius"):
        sites.append((pos, value, f"cornerRadius: {value}"))

    for m in re.finditer(r"\bcpCard\s*\(", masked):
        args, close = _call_args(masked, m.end() - 1)
        if close < 0:
            continue
        value = _labeled(args, "radius")
        if value is not None:
            sites.append((m.start(), value, f"cpCard(radius: {value})"))

    # And the `radius:` defaults, for the same reason as the padding ones. This
    # also reaches `Theme.swift`'s own `static let radius: CGFloat = rLg`, which
    # is a bonus: re-pointing the legacy alias at a bare literal (its old value
    # was 14) fails here, so the aliases cannot quietly leave the scale they were
    # migrated onto. The `radiusSm`/`radiusXs` aliases are not matched — the
    # label has to be exactly `radius` — and the scale's own `rXl…rSm`
    # declarations are not matched either, which is what keeps this from being
    # circular.
    for m in re.finditer(r"\bradius\s*:\s*CGFloat\s*=\s*([^,)\n]+)", masked):
        value = m.group(1).strip()
        sites.append((m.start(), value, f"radius: CGFloat = {value}"))

    return sites


# Exemptions — the standard is the one the web side used, and it is a standard
# about *kind*, not about size: a value is exempt when it is not spacing.
#
#   1-3pt is optical alignment or a hairline. The scale's floor is 4pt
#   (`--s-1`); below that a value is nudging a glyph onto a baseline or rounding
#   a 3pt-wide bar, and snapping it to 4 changes the drawing, not the rhythm.
#
# Everything at 4pt or above is on or off the scale and gets no pass here —
# notably `.padding(.vertical, 4)` is `Theme.s1`, not an exemption.
#
# Keys are whole `"<repo-relative path> | <display text>"` strings, matched
# entire. Never a prefix and never a bare value: a bare `2` would exempt every
# 2pt gap in the app forever, and a path prefix would exempt a file.
#
# Residual risk, named rather than hidden: two *different* components in the
# same file that share an identical fragment share one exemption
# (`Features/Backtest/SlipCard.swift | VStack(… spacing: 2)` would cover a second
# such stack). `test_every_spacing_exemption_still_matches_a_real_site` catches
# the dead-entry direction; this direction is uncaught, and the mitigation is
# that all six entries are 1-3pt values where the fix is a no-op anyway.
SPACING_LITERAL_OK = {
    # LeaguePill: 3pt above/below an 11pt all-caps label inside a Capsule. 4pt
    # makes the pill taller than the 24pt row rhythm it sits in; this is the
    # capsule's optical inset, the same role as web's `.lp-league` padding.
    "ios/CoreProp/Theme/Components.swift | .padding(.vertical, 3)":
        "capsule optical inset on an 11pt label, not a gap",
    # BookBadgeView: 2pt on a 10pt badge. Same role, one size down — these
    # badges wrap in rows of three or four and 4pt visibly breaks the wrap.
    "ios/CoreProp/Theme/Components.swift | .padding(.vertical, 2)":
        "badge optical inset on a 10pt label, not a gap",
    # SectionHeader: 2pt between a 17pt title and its 13pt subtitle. Two lines of
    # one label; 4pt reads as two separate labels.
    "ios/CoreProp/Theme/Components.swift | VStack(… spacing: 2)":
        "title/subtitle leading inside one label, not a gap between elements",
    # BetRow / LinesView: 3pt between a book badge and its price inside a single
    # inline chip. The pair is one token visually; the gap between *chips* is the
    # spacing value and is on the scale.
    "ios/CoreProp/Features/Bets/BetRow.swift | HStack(… spacing: 3)":
        "intra-chip badge/price gap — the chip is one visual token",
    "ios/CoreProp/Features/Lines/LinesView.swift | HStack(… spacing: 3)":
        "intra-chip badge/price gap — the chip is one visual token",
    # SlipCard: 1pt between a leg's player name and its prop line, and 2pt in the
    # footer's label/value pair. Both are leading within one text block.
    "ios/CoreProp/Features/Backtest/SlipCard.swift | VStack(… spacing: 1)":
        "leading between two lines of one leg label, not a gap",
    "ios/CoreProp/Features/Backtest/SlipCard.swift | VStack(… spacing: 2)":
        "label-above-value leading inside one footer cell, not a gap",
    # SlipView: 2pt, same label-above-value pairing as SlipCard's footer.
    "ios/CoreProp/Features/Slip/SlipView.swift | VStack(… spacing: 2)":
        "label-above-value leading inside one stat block, not a gap",
    # AccountView: 3pt between the account name and the email beneath it.
    "ios/CoreProp/Features/Account/AccountView.swift | VStack(… spacing: 3)":
        "name/email leading inside one identity block, not a gap",
    # SlipCard's WIN/LOSS/PUSH/PENDING status badge: 3pt above/below a 9pt bold
    # label in a Capsule. Same role as LeaguePill's, one type size down.
    "ios/CoreProp/Features/Backtest/SlipCard.swift | .padding(.vertical, 3)":
        "capsule optical inset on a 9pt status label, not a gap",
    # The GOBLIN badge, duplicated in BetRow and BetDetailView: 2pt on a 9pt bold
    # label in a Capsule. (Both copies are listed because the exemption key is
    # per-file — a shared component would need only one, and extracting one is a
    # refactor this phase explicitly deferred.)
    "ios/CoreProp/Features/Bets/BetRow.swift | .padding(.vertical, 2)":
        "capsule optical inset on the 9pt GOBLIN badge, not a gap",
    "ios/CoreProp/Features/Bets/BetDetailView.swift | .padding(.vertical, 2)":
        "capsule optical inset on the 9pt GOBLIN badge, not a gap",
}
# An asymmetry a reader will notice, so: the *horizontal* insets on those same
# badges (5pt and 7pt) are NOT exempt and stay in the worklist. That is the
# stated standard applied honestly rather than per-component — 5 and 7 sit
# between s1(4) and s2(8), so snapping them is a real decision somebody should
# make and record, while 2 and 3 have no scale neighbour below 4 at all.

RADIUS_LITERAL_OK = {
    # SlipCard's 3pt-wide outcome bar. A 2pt radius on a 3pt-wide rectangle is a
    # rounded *end*, not a corner — `Theme.rSm` (8) is wider than the bar and
    # would collapse it to a lozenge. Web's equivalent left bar is a
    # square-ended 3px border with no radius at all, so there is no token to
    # inherit; this is iOS softening a hairline.
    "ios/CoreProp/Features/Backtest/SlipCard.swift | cornerRadius: 2":
        "rounded end of a 3pt-wide bar; the smallest radius token (8) exceeds "
        "the bar's own width",
}


def _scale_violations(
    sites_for, allowed_tokens: frozenset[str], exempt: dict[str, str]
) -> list[tuple[str, int, str]]:
    out: list[tuple[str, int, str]] = []
    for label, src, masked in _sources():
        for pos, value, text in sites_for(src, masked):
            if f"{label} | {text}" in exempt:
                continue
            if _NUMERIC.match(value):
                if float(value) == 0:
                    continue          # `spacing: 0` / `cornerRadius: 0` is a reset
            elif not value.startswith("Theme."):
                continue              # a pass-through identifier or expression
            elif value in allowed_tokens:
                continue
            out.append((label, _line_no(src, pos), text))
    return out


def test_ios_spacing_goes_through_the_scale():
    _fail(
        "spacing literals off Theme.s1…s12",
        _scale_violations(_spacing_sites, _SPACING_TOKENS, SPACING_LITERAL_OK),
        "Use Theme.s1(4) s2(8) s3(12) s4(16) s5(20) s6(24) s8(32) s10(40) "
        "s12(48) — there is deliberately no s7/s9/s11. 14 was the app's most "
        "common literal and maps to s4, so rows get roomier; web accepted the "
        "same ~10% drop in rows-above-fold. `frame(width:height:)` is intrinsic "
        "sizing and is not under test.",
    )


def test_ios_radii_go_through_the_scale():
    _fail(
        "corner-radius literals off the Theme.r* scale",
        _scale_violations(_radius_sites, _RADIUS_TOKENS, RADIUS_LITERAL_OK),
        "Use Theme.rXl(20) rLg(16) rMd(12) rSm(8), or the legacy aliases "
        "Theme.radius/radiusSm/radiusXs which resolve into them. `Capsule()` is "
        "the idiom for a pill — there is no rPill token.",
    )


def test_every_spacing_exemption_still_matches_a_real_site():
    """Same guard as the `text4` one: an exemption whose site is gone is a
    standing permission for whatever lands on that text next."""
    seen = set()
    for label, src, masked in _sources():
        for _pos, _value, text in _spacing_sites(src, masked):
            seen.add(f"{label} | {text}")
        for _pos, _value, text in _radius_sites(src, masked):
            seen.add(f"{label} | {text}")
    declared = set(SPACING_LITERAL_OK) | set(RADIUS_LITERAL_OK)
    stale = sorted(declared - seen)
    assert not stale, (
        "these exemptions match no call site — delete them or fix the key:\n  "
        + "\n  ".join(stale)
    )


# ── Parser guards ─────────────────────────────────────────────────────────────
# The failure mode that matters most here is not a wrong answer, it is a zero
# denominator: a regex that stops matching turns every test above into an
# assertion over an empty list, and the suite goes green while the contract
# rots. `test_payout_table_mirror.py` pays for exactly this guard
# (`test_the_parser_actually_finds_entries_in_every_mirror`) and for the same
# reason. The floors below are well under today's counts, so ordinary migration
# does not trip them, but deleting the app or breaking a pattern does.


def test_the_parser_sees_what_it_claims_to_see():
    files = _swift_files()
    assert len(files) >= 25, f"only {len(files)} Swift files found under ios/CoreProp"
    assert not [p for p in files if "CorePropKit" in p.parts], (
        "the scan reached ios/CorePropKit — that package is Foundation-only, has "
        "no SwiftUI and no tokens, and is verified by `swift run CorePropKitVerify`"
    )

    counts = {"padding": 0, "spacing": 0, "cornerRadius": 0, "cpCard": 0,
              "hex_in_theme": 0, "onChange": 0, "text4": 0}
    raw_spacing_labels = 0
    attributed_spacing = 0
    for label, src, masked in _sources():
        counts["padding"] += len(re.findall(r"\.padding\s*\(", masked))
        counts["cornerRadius"] += len(_label_sites(masked, "cornerRadius"))
        counts["cpCard"] += len(re.findall(r"\bcpCard\s*\(", masked))
        counts["onChange"] += len(_onchange_sites(masked))
        counts["text4"] += len(re.findall(r"\bTheme\.text4\b", masked))
        if label == str(_THEME.relative_to(_ROOT)):
            counts["hex_in_theme"] += len(_HEX_COLOR.findall(masked))
        raw = len(re.findall(r"\bspacing\s*:", masked))
        raw_spacing_labels += raw
        mine = sum(1 for _p, _v, t in _spacing_sites(src, masked) if "spacing:" in t)
        attributed_spacing += mine
        counts["spacing"] += mine

    floors = {"padding": 50, "spacing": 70, "cornerRadius": 15, "cpCard": 15,
              "hex_in_theme": 20, "onChange": 2, "text4": 5}
    short = {k: (counts[k], floors[k]) for k in floors if counts[k] < floors[k]}
    assert not short, (
        "a pattern stopped matching — these tests would now pass on an empty "
        f"denominator: {short} (counted {counts})"
    )

    # Every `spacing:` in the tree must belong to a container this module knows
    # about. If a `Grid(horizontalSpacing:)` or some new stack appears, this fails
    # and names the gap rather than skipping it in silence.
    assert attributed_spacing == raw_spacing_labels, (
        f"{raw_spacing_labels - attributed_spacing} `spacing:` argument(s) are on "
        f"a container not in _GAP_CONTAINERS {_GAP_CONTAINERS} and are therefore "
        "unchecked. Add the container."
    )


def test_the_mask_removes_comments_and_strings_but_keeps_offsets():
    """`_mask` is the load-bearing part of every test above, and its two jobs
    conflict: it must delete text without moving it. Verified against the real
    tree as well as this fixture, because Theme.swift's own doc comments contain
    `was 14`, `10 -> 12` and `radius: 14` — all of which would be reported as
    off-scale literals by an unmasked scan."""
    src = (
        'let a = 1\n'
        '// .padding(99)\n'
        'let b = "a string with .padding(98) in it"\n'
        '/* .padding(97)\n'
        '   still a comment */\n'
        'VStack(spacing: 7) { }\n'
    )
    masked = _mask(src)
    assert len(masked) == len(src)
    assert masked.count("\n") == src.count("\n")
    for hidden in ("99", "98", "97", "a string"):
        assert hidden not in masked, f"{hidden!r} survived masking"
    assert "VStack(spacing: 7)" in masked, "masking ate live code"
    assert _line_no(masked, masked.index("spacing")) == 6

    theme = _THEME.read_text(encoding="utf-8")
    theme_masked = _mask(theme)
    assert "was 14" not in theme_masked, (
        "Theme.swift's radius doc comment survived masking; it would be read as "
        "an off-scale literal"
    )
    # ...but the declarations it documents are still visible.
    assert "static let radius: CGFloat = rLg" in theme_masked

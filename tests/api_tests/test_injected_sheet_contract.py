"""The runtime-injected stylesheet selects on class names it does not own.

`web/static/app-main.jsx` builds a <style> element at module scope and appends it
to <head>. Three rules; two of them target `.ev-row` / `.ev-row-data`, which are
`web/static/ev-page.jsx`'s class names. Rename either one there and compact
density and the +EV row hover go dead — no error, no console warning, no test
failure. The spec flagged exactly this ("the `.ev-row` / `.ev-row-data` class
names must survive Phase 2's row rework or these rules silently go dead") and the
row markup *was* reworked in Phase 2a, so this held on luck.

Two design choices worth the words:

  * **The class set is DERIVED from the sheet, not hardcoded.** Two names today,
    but the sheet is where a third arrives, and a hardcoded pair would keep passing
    while the new rule went unchecked. The derivation is what makes this keep
    working.
  * **`dist/ev-page.js` is checked too, not just the `.jsx`.** dist/ is committed
    and served directly (Render's build env is pip-only and cannot run
    ./build.sh), so a rename plus a rebuild is safe, a rename WITHOUT a rebuild
    means the shipped bundle still carries the old name, and a rename in the source
    only while dist/ is stale would pass a source-only check while production
    breaks. Same reasoning as `test_accent_agrees_in_all_three_copies`.

What this does NOT prove: that the classes are on the right ELEMENTS. `.ev-row`
appearing somewhere in ev-page.jsx does not mean it is still the row wrapper whose
vertical padding the compact rule overrides. That needs a rendered DOM, and nothing
in this suite renders anything. The name surviving is the cheap 90% of the failure
mode.
"""
from __future__ import annotations

import re

from tests.api_tests.css_helpers import (
    APP_MAIN_DIST,
    APP_MAIN_JSX,
    EV_PAGE_DIST,
    EV_PAGE_JSX,
    injected_style_block,
    rules,
)

# Classes in the injected sheet that the APP SHELL renders itself, in
# app-main.jsx's own root <div>. They are not ev-page.jsx's to break, but they are
# listed rather than filtered out by pattern so a new shell class has to be
# classified before this test will pass.
#
# `density-compact` is the interesting one: app-main.jsx composes it at runtime as
# `"density-" + t.density`, so the literal string `density-compact` appears NOWHERE
# in the JSX outside the stylesheet itself. Renaming the TweakRadio option from
# "compact" to "dense" would silently orphan two of the sheet's three rules while
# every literal-string search came up clean. Hence the composed check below.
SHELL_CLASSES = {
    "app": "app-main.jsx's root <div> className, rendered literally",
    "tint-on": "app-main.jsx's root <div>, appended when tableTint === 'on'",
    "density-compact": "COMPOSED as `\"density-\" + t.density` — never a literal",
}
# Matched as a pattern, not a literal: the concatenation sits inside a longer
# string (`"app density-" + t.density + …`), so an exact-substring constant would
# fail on the `app ` prefix rather than on the thing under test.
_DENSITY_COMPOSE = re.compile(r"density-\s*\"\s*\+\s*t\.density\b")
_DENSITY_OPTION = "compact"

_CLASS = re.compile(r"\.(-?[A-Za-z_][\w-]*)")


def injected_selector_classes(path=APP_MAIN_JSX) -> set[str]:
    """Every class name the injected sheet's selectors match on.

    Reads the selectors rather than the whole text, so a class name that only
    appears inside a declaration value (a `content:".ev-row"`, say) is not counted
    as a coupling. `:not(.tint-on)` contributes `tint-on`, which is correct — the
    rule depends on that name existing exactly as much as a positive match would.
    """
    out: set[str] = set()
    for selector, _decls in rules(injected_style_block(path)):
        out.update(_CLASS.findall(selector))
    return out


def _renders_class(source: str, name: str) -> bool:
    """Whether `source` puts `name` in a class position.

    Word-bounded on both sides so `ev-row` does not answer for `ev-row-data`: a
    plain substring search would report `.ev-row` as present in any file that only
    had `ev-row-data`, which is the exact pair this test exists to tell apart.
    Class lists are space-separated inside a string, and the JSX also builds them by
    concatenation (`"ev-row-data " + cls`), so the boundary characters that count
    are quote, space and the template/concat punctuation — `[\\s"'`+{}]` covers all
    of them.
    """
    return bool(re.search(rf"""(?<=[\s"'`+{{]){re.escape(name)}(?=[\s"'`+}}])""", source))


def test_the_injected_sheet_only_selects_classes_someone_renders():
    """Every class in the injected sheet is either ev-page.jsx's or the shell's."""
    found = injected_selector_classes()
    ev_owned = {c for c in found if c.startswith("ev-")}
    unclassified = sorted(found - ev_owned - set(SHELL_CLASSES))
    assert not unclassified, (
        "the injected stylesheet gained selectors on classes nobody has claimed. "
        "Say who renders them — an `.ev-*` name is checked against ev-page.jsx "
        "below, anything else belongs in SHELL_CLASSES with a note on where it is "
        f"rendered:\n  {unclassified}"
    )
    assert ev_owned, (
        "the injected sheet selects on no `.ev-*` class at all. If the density and "
        "hover overrides moved into index.html this module is obsolete and should "
        "be deleted, not left passing on an empty set."
    )


def test_ev_page_still_renders_the_classes_the_injected_sheet_targets():
    jsx = EV_PAGE_JSX.read_text(encoding="utf-8")
    dist = EV_PAGE_DIST.read_text(encoding="utf-8")
    missing = []
    for name in sorted(injected_selector_classes()):
        if not name.startswith("ev-"):
            continue
        if not _renders_class(jsx, name):
            missing.append(f"{name} — not rendered by ev-page.jsx")
        if not _renders_class(dist, name):
            missing.append(
                f"{name} — not in dist/ev-page.js (renamed without ./build.sh? "
                "the shipped bundle is what the sheet has to match)"
            )
    assert not missing, (
        "app-main.jsx's injected stylesheet targets class names ev-page.jsx no "
        "longer renders, so those rules are dead with no other symptom (compact "
        "density stops applying; the +EV row loses its hover tint):\n  "
        + "\n  ".join(missing)
    )


def test_the_shell_classes_the_injected_sheet_targets_are_still_produced():
    """The other half of the coupling, and the one a literal search misses.

    `density-compact` is assembled from a prefix and a radio option value. Both
    halves are pinned: the concatenation in the root <div>, and `compact` still
    being one of the Density options. Either one changing orphans two of the
    sheet's three rules.
    """
    jsx = APP_MAIN_JSX.read_text(encoding="utf-8")
    problems = []
    found = injected_selector_classes()
    for name, note in SHELL_CLASSES.items():
        if name not in found:
            problems.append(
                f"{name} is in SHELL_CLASSES but the sheet no longer selects it — "
                f"delete the entry ({note})"
            )
            continue
        if name == "density-compact":
            if not _DENSITY_COMPOSE.search(jsx):
                problems.append(
                    "app-main.jsx no longer composes the density class as "
                    '`"… density-" + t.density`, so `.density-compact` may never '
                    "appear"
                )
            options = re.search(r"label=\"Density\".*?options=\{\[(.*?)\]\}", jsx, re.S)
            if not options or f'"{_DENSITY_OPTION}"' not in options.group(1):
                problems.append(
                    f'"{_DENSITY_OPTION}" is no longer a Density option, so '
                    "`.density-compact` is unreachable and both compact rules are "
                    "dead"
                )
            continue
        if not _renders_class(jsx, name):
            problems.append(f"app-main.jsx no longer renders `{name}` ({note})")
    assert not problems, "\n  ".join(problems)


def test_the_class_extractor_sees_what_it_claims_to_see():
    """Denominator plus the two ways this parser could silently go blind."""
    found = injected_selector_classes()
    assert len(found) >= 4, f"only {len(found)} classes extracted: {sorted(found)}"
    assert {"ev-row", "ev-row-data"} <= found, (
        f"the two documented couplings are not being extracted: {sorted(found)}"
    )
    # dist must agree with the source, or this test checks the wrong sheet.
    assert injected_selector_classes(APP_MAIN_DIST) == found, (
        "the injected sheet's selectors differ between app-main.jsx and its "
        "committed bundle — ./build.sh was not re-run"
    )
    # The boundary rule, which is the one thing here that is easy to get wrong.
    assert _renders_class('className="ev-row-data is-sel"', "ev-row-data")
    assert not _renders_class('className="ev-row-data is-sel"', "ev-row"), (
        "_renders_class matched a prefix — `ev-row` must not be satisfied by "
        "`ev-row-data`, or a rename of the row wrapper alone would pass"
    )
    assert _renders_class('"ev-row " + (sel ? "is-sel" : "")', "ev-row")

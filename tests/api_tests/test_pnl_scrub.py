"""The draggable P&L chart's interaction contract.

The Analytics P&L curve is scrubbable (drag on touch, hover on desktop, header
tracks the finger — Robinhood-style). Three properties make that feel right
instead of broken, and all three are one edit away from being lost:

  1. **The drag must not scroll the page.** `.pnl-chart.is-scrub` sets
     `touch-action:none`, which is what makes the element claim the gesture. This
     is CSS on purpose: `preventDefault()` on touchmove is treated as passive on
     these listeners and would be ignored.
  2. **…but only for THAT chart.** `.pnl-chart` is shared with the reliability
     chart, which has nothing to scrub. If `touch-action:none` migrates onto the
     shared class, a reader can no longer scroll the page by dragging over the
     reliability chart — it just feels dead. The base class must stay `pan-y`.
  3. **The drag must not select anything.** `user-select:none` (+ the WebKit
     prefix) on the shared class stops the SVG being highlighted and stops iOS
     raising its callout.

Plus the repo's standing build contract: `index.html` loads only
`web/static/dist/*.js`, so a `.jsx` edit without `./build.sh` ships a no-op. The
last test pins the scrub wiring into the compiled bundle so that can't happen
silently here.
"""
from __future__ import annotations

import re
from pathlib import Path

from tests.api_tests.css_helpers import (
    INDEX,
    declarations,
    rules,
    style_block,
    squash,
)

_WEB = Path(__file__).resolve().parents[2] / "web" / "static"
_ANALYTICS_JSX = _WEB / "page-analytics.jsx"
_ANALYTICS_DIST = _WEB / "dist" / "page-analytics.js"


def _decls_for(selector: str) -> list[str]:
    """Every declaration on the rule(s) with exactly this selector."""
    out: list[str] = []
    for sel, decls in rules(style_block(INDEX)):
        if sel == selector:
            out.extend(declarations(decls))
    return out


def _prop(selector: str, prop: str) -> str | None:
    for d in _decls_for(selector):
        name, _, value = d.partition(":")
        if name.strip().lower() == prop:
            return value.strip().lower()
    return None


def test_scrub_chart_claims_the_touch_gesture():
    """Without touch-action:none the page scrolls out from under the drag."""
    assert _decls_for(".pnl-chart.is-scrub"), (
        ".pnl-chart.is-scrub rule is gone — the P&L chart is what the scrub "
        "interaction hangs off; see web/static/page-analytics.jsx::PnLChart."
    )
    assert _prop(".pnl-chart.is-scrub", "touch-action") == "none", (
        "the scrubbable P&L chart must set touch-action:none, otherwise dragging "
        "across it scrolls the page instead of reading the curve."
    )


def test_shared_chart_class_still_lets_the_page_scroll():
    """The reliability chart shares .pnl-chart and has nothing to scrub."""
    base = _prop(".pnl-chart", "touch-action")
    assert base == "pan-y", (
        f"base .pnl-chart touch-action is {base!r}, expected 'pan-y'. Moving "
        "touch-action:none onto the shared class traps the page scroll over the "
        "ReliabilityChart, which cannot be scrubbed — keep it on .is-scrub."
    )


def test_dragging_a_chart_selects_nothing():
    ds = squash(";".join(_decls_for(".pnl-chart")))
    assert "user-select:none" in ds, (
        ".pnl-chart must keep user-select:none — a touch-drag would otherwise "
        "highlight the SVG."
    )
    assert "-webkit-user-select:none" in ds, "keep the -webkit- prefix for iOS Safari."
    assert "-webkit-touch-callout:none" in ds, (
        "keep -webkit-touch-callout:none — iOS raises its callout on a long press "
        "over the chart without it."
    )


def test_no_pointer_events_none_on_the_shared_chart_class():
    """Documented landmine: it is inherited by the child <circle> marks and would
    kill the ReliabilityChart's native <title> tooltips. The P&L crosshair sets
    the property on its own <g> instead."""
    for selector in (".pnl-chart", ".pnl-chart.is-scrub"):
        assert _prop(selector, "pointer-events") != "none", (
            f"{selector} sets pointer-events:none — that is inherited by the "
            "<circle> marks and silently removes ReliabilityChart's hover tooltips."
        )


def test_scrub_wiring_is_in_the_committed_bundle():
    """A .jsx edit without ./build.sh ships nothing (index.html loads dist/)."""
    jsx = _ANALYTICS_JSX.read_text(encoding="utf-8")
    dist = _ANALYTICS_DIST.read_text(encoding="utf-8")

    # Pointer events (one code path for mouse/touch/pen) + capture, so a drag that
    # leaves the chart box keeps tracking.
    for token in ("onPointerDown", "onPointerMove", "setPointerCapture", "is-scrub"):
        assert token in jsx, f"page-analytics.jsx lost the scrub wiring: {token}"
        assert token in dist, (
            f"dist/page-analytics.js is missing {token!r} — run ./build.sh and "
            "commit web/static/dist/ alongside the .jsx change."
        )

    # Touch scrubbing must not be implemented by preventDefault on touchmove:
    # these listeners are passive, so it would be ignored (and it is the CSS
    # above that actually does the job). Matched as CODE tokens, not the bare
    # word — the source comments discuss touchmove precisely to say why it isn't
    # used, and a prose match would fail on its own explanation.
    assert "onTouchMove" not in jsx, (
        "the scrub uses Pointer events + CSS touch-action, not an onTouchMove "
        "handler — React's touchmove listener is passive and cannot block the scroll."
    )
    assert not re.search(r"""addEventListener\(\s*["']touchmove""", jsx), (
        "same: a manually-attached touchmove listener is passive by default here; "
        "the scroll is blocked by .pnl-chart.is-scrub { touch-action: none }."
    )

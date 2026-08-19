# Frontend Modernization — Phase 1 (tokens & shared CSS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the radius/spacing/elevation/type token scales and migrate every hardcoded geometry and elevation declaration in `web/static/index.html` (plus `extension.html`) onto them, flattening card surfaces and removing skeuomorphic glows — a purely visual change with no JSX edit and no bundle rebuild.

**Architecture:** All app CSS lives in one `<style>` block in `web/static/index.html`; there is no `.css` file. Phase 1 therefore touches only static files (`index.html`, `extension.html`) plus three new pytest files and one CI job. Because only 6 of 110 `border-radius` and 2 of 26 `box-shadow` declarations currently read a token, this is a **per-rule rewrite, not a token flip** — editing `:root` alone changes almost nothing on screen. Each mechanical migration is driven by a new invariant test that enumerates its own remaining violations, so the test output *is* the worklist.

**Tech Stack:** Static HTML/CSS (no build step for this phase), pytest (stdlib `re`/`hashlib` only — the CI pytest job is pip-only), GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` (Sections 1, 2, 4a, 6; Phasing item 1).

---

## Scope

**In scope (Phase 1):** `web/static/index.html` `<style>` block, `web/static/extension.html`, new tests, one CI job.

**Explicitly deferred to Phase 2** (all require a `.jsx` edit + `./build.sh`, so they cannot be done here):
- `.density-compact .ev-row` padding and the `.ev-row-data:hover` rule — both live in the runtime-injected `<style>` in `app-main.jsx:256-262`.
- The scoped `.ev-row .cp-tp` 20px true% — coupled to the row restructure.
- Consequence to accept: between Phase 1 and Phase 2, compact density is briefly 9px against a 16px regular row.

**Out of scope entirely:** `tweaks-panel.jsx`, `analytics-preview.html`, `auth-page.jsx` (constraints 7 and 8). `build.sh`'s `FILES` array keeps its current 10 entries.

**Never touch in this phase:** any selector containing `lp-game-` (the hero minigame is a deliberate PrizePicks-board mimic — colors *and* geometry), except `.lp-sk` and `.lp-game-ctap-x`, which do move to `--r-sm`.

---

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `tests/api_tests/css_helpers.py` | Create | Shared CSS parsing helpers (style-block extraction, rule iteration, accent detection). One responsibility: turn CSS text into `(selector, declarations)` pairs. Imported by the two guard test files so neither duplicates a parser. |
| `tests/api_tests/test_css_guards.py` | Create | The CLAUDE.md bans (accent gradients, blur orbs, gradient-clipped text), the three-copy accent mirror, and the `extension.html` palette mirror. |
| `tests/api_tests/test_css_tokens.py` | Create | The Phase-1 migration invariants: radius tokenization, flat surfaces, no white inset, `--row-px` coupling, `--text-4` restriction. Separate file from the bans because these assert *this design's* rules, not CLAUDE.md's permanent ones. |
| `tests/api_tests/test_build_stamp.py` | Create | Bundle-freshness layer 1: the `dist/*.js` digest equals all 11 `?v=` / cache-name stamps. |
| `web/static/index.html` | Modify | The token definitions and every geometry/elevation/type migration. |
| `web/static/extension.html` | Modify | By-value radius pass (it cannot read `:root`). |
| `.github/workflows/tests.yml` | Modify | Bundle-freshness layer 2: a node job that re-runs `./build.sh` and fails on a diff. |

---

## Task 1: CSS helpers + the accent-gradient ban

The only ban that is **red** today (2 violations). Everything else in Task 2 starts green as a regression guard.

**Files:**
- Create: `tests/api_tests/css_helpers.py`
- Create: `tests/api_tests/test_css_guards.py`
- Modify: `web/static/index.html` (2 rules)

- [ ] **Step 1: Write the parsing helpers**

Create `tests/api_tests/css_helpers.py`:

```python
"""CSS parsing helpers for the stylesheet guard tests.

`web/static/index.html` carries the entire app stylesheet in one <style> block
(there is no .css file in the repo). These helpers turn that text into
(selector, declarations) pairs so the guards can assert per-rule facts without
each test re-implementing a parser.

Deliberately regex-based and dependency-free: the CI pytest job is pip-only and
must not grow a CSS-parser dependency.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WEB = REPO / "web" / "static"
INDEX = WEB / "index.html"
EXTENSION = WEB / "extension.html"
APP_MAIN_JSX = WEB / "app-main.jsx"
APP_MAIN_DIST = WEB / "dist" / "app-main.js"

# Accent colors in every notation used in this stylesheet. Both the var() and
# the literal forms are required: `.ev-row-data.is-sel` used a raw rgba(), so a
# var()-only pattern would have passed straight over it.
ACCENT_VARS = ("var(--primary)", "var(--primary-2)", "var(--primary-hi)", "var(--primary-lo)")
ACCENT_HEX = ("#1e6fb0", "#6fbcec", "#195f97")
ACCENT_RGB = ((30, 111, 176), (111, 188, 236), (25, 95, 151))

_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_RULE = re.compile(r"([^{}]+)\{([^{}]*)\}", re.S)


def style_block(path: Path = INDEX) -> str:
    """The contents of the <style> block, comments stripped.

    Comments are removed so commented-out code can neither trip a ban nor
    satisfy an invariant.
    """
    html = path.read_text(encoding="utf-8")
    blocks = re.findall(r"<style>(.*?)</style>", html, re.S)
    assert blocks, f"no <style> block found in {path.name}"
    return _COMMENT.sub("", "\n".join(blocks))


def rules(css: str):
    """Yield (selector, declarations) for every rule.

    @media preludes are skipped naturally: `@media (...)  {` cannot match
    because the text after its `{` contains another `{`, so the engine advances
    and matches the inner rule instead. Any selector still starting with `@`
    (e.g. @keyframes steps) is filtered out.
    """
    for selector, decls in _RULE.findall(css):
        selector = selector.strip()
        if not selector or selector.startswith("@"):
            continue
        yield selector, decls


def declarations(decls: str) -> list[str]:
    """Split a rule body into individual `prop:value` declarations."""
    return [d.strip() for d in decls.split(";") if d.strip()]


def squash(text: str) -> str:
    """Lowercase and remove all whitespace, for whitespace-insensitive matching."""
    return re.sub(r"\s+", "", text).lower()


def has_accent(value: str) -> bool:
    """True if a declaration value names an accent color in any notation."""
    v = squash(value)
    if any(squash(tok) in v for tok in ACCENT_VARS):
        return True
    if any(h in v for h in ACCENT_HEX):
        return True
    return any(f"({r},{g},{b}" in v for r, g, b in ACCENT_RGB)


def is_minigame(selector: str) -> bool:
    """The hero minigame is exempt from the gradient/orb bans.

    It is a deliberate near-clone of the PrizePicks large card (see the comment
    at index.html:453) and `.lp-game-flash` is exactly a border-radius:50%
    radial-gradient glow disc that stays. Matched on the literal class prefix so
    the exemption cannot silently widen.
    """
    return "lp-game-" in selector


def gradient_declarations(decls: str):
    """Yield declarations whose value contains any CSS gradient function."""
    for decl in declarations(decls):
        if re.search(r"(linear|radial|conic)-gradient\(", decl, re.I):
            yield decl
```

- [ ] **Step 2: Write the failing accent-gradient test**

Create `tests/api_tests/test_css_guards.py`:

```python
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


def test_no_accent_gradients():
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
                violations.append(f"{selector} -> {decl.strip()}")
    assert not violations, "accent gradients found:\n  " + "\n  ".join(violations)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_guards.py::test_no_accent_gradients -q`

Expected: **FAIL**, listing exactly 2 violations — `.pnl-range-btn.is-custom.is-on` (a `var(--primary-2)`→`var(--primary)` gradient) and `.ev-row-data.is-sel` (a `rgba(30,111,176,…)` gradient).

- [ ] **Step 4: Fix the two accent gradients**

In `web/static/index.html`, find `.pnl-range-btn.is-custom.is-on` (~line 1111). Replace its `background:linear-gradient(...)` with a flat fill, and drop the white inset from its `box-shadow` in the same edit (that inset is on Task 8's list anyway):

```css
.pnl-range-btn.is-custom.is-on{background:var(--primary);color:#fff}
```

Find `.ev-row-data.is-sel` (~line 723). Replace its `background:linear-gradient(90deg,rgba(30,111,176,.10),rgba(30,111,176,.02))` with a flat tint:

```css
.ev-row-data.is-sel{border-left:2px solid var(--primary);background:var(--primary-lo);padding-left:calc(clamp(18px,2vw,24px) - 2px)}
```

**Why `--primary-lo` and not `--primary-hi`.** The spec's Section 2 says "`--primary-hi` tint", but `--primary-hi` is `rgba(30,111,176,.22)` — tuned for focus rings and small badges, where it always sits under an explicit `color:var(--primary-2)` (7.16:1). Behind a *row* it is inherited by `.ev-time` (`color:var(--text-3)`, 12px), which measures **4.45:1 at the top of the table — under the 4.5:1 AA floor**. The gradient it replaces never dropped below 5.03:1, so shipping `--primary-hi` here would be an accessibility regression, not a like-for-like flatten. Add a new token in the same edit:

```css
  /* Row-level accent tint. --primary-hi (.22) is for focus rings and badges,
   * where an explicit --primary-2 sits on top. Behind a row the tint is
   * inherited by .ev-time's --text-3 and measures 4.45:1, under the AA floor
   * the :root note above documents. .10 is exactly the most-tinted stop of the
   * gradient this replaces, so the flat fill holds the identical worst case
   * (5.03:1). Enforced by tests/api_tests/test_css_guards.py::
   * test_primary_hi_never_backs_inherited_text. */
  --primary-lo: rgba(30,111,176,.10);
```

Register the new token in `ACCENT_VARS` too, or the guard cannot see a gradient built from it.

Leave the `border-left` at 2px and the `calc()` as-is for now — Task 9 moves the bar to 3px and re-derives the padding from `--row-px`. Changing them here would split one coupled edit across two commits.

- [ ] **Step 5: Run the test to verify it passes**

Run: `python -m pytest tests/api_tests/test_css_guards.py::test_no_accent_gradients -q`
Expected: **PASS**

- [ ] **Step 6: Run the full suite to confirm nothing regressed**

Run: `python -m pytest tests/ -q`
Expected: `380 passed` (378 existing + 2 new: the accent-gradient ban plus the `--primary-hi` contrast guard).

- [ ] **Step 7: Commit**

```bash
git add tests/api_tests/css_helpers.py tests/api_tests/test_css_guards.py web/static/index.html
git commit -m "test(css): ban accent gradients, and remove the two that existed"
```

---

## Task 2: The remaining bans and the token mirrors

All four start **green** — they lock in facts that are already true, including two mirrors currently maintained by a comment alone.

**Files:**
- Modify: `tests/api_tests/test_css_guards.py`

- [ ] **Step 1: Add the four remaining assertions**

Append to `tests/api_tests/test_css_guards.py`:

```python
import re

from tests.api_tests.css_helpers import (
    APP_MAIN_DIST,
    APP_MAIN_JSX,
    EXTENSION,
    declarations,
    squash,
)


def test_no_blurred_decorative_orbs():
    """A glow orb is `border-radius:50%` plus a blur or a radial-gradient.

    Matching a bare `filter:blur(` substring would flag `backdrop-filter:blur()`
    on the sticky nav and the modal scrim, which are legitimate material blurs
    on full-bleed surfaces, not decoration. So the matcher requires the round +
    glow *pair* instead. Zero today; this guards against re-adding the removed
    `.lp-orb` discs.
    """
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if is_minigame(selector):          # .lp-game-flash is exactly this pair, and stays
            continue
        body = squash(decls)
        round_ = "border-radius:50%" in body
        glow = re.search(r"(?<!backdrop-)filter:blur\(", body) or "radial-gradient(" in body
        if round_ and glow:
            violations.append(selector)
    assert not violations, f"blurred decorative orbs found: {violations}"


def test_no_gradient_clipped_text():
    """Zero instances today; regression guard."""
    violations = [
        selector
        for selector, decls in rules(style_block(INDEX))
        if "background-clip:text" in squash(decls)
    ]
    assert not violations, f"gradient-clipped text found: {violations}"


def _root_token(css: str, name: str) -> str:
    """Read a custom property's value out of the :root block."""
    m = re.search(rf"{re.escape(name)}\s*:\s*([^;}}]+)", css)
    assert m, f"{name} not found in :root"
    return m.group(1).strip().lower()


def test_accent_agrees_in_all_three_copies():
    """`--primary`, TWEAK_DEFAULTS.accent in the .jsx, AND in the committed bundle.

    An effect writes TWEAK_DEFAULTS.accent back as an *inline* style on
    document.documentElement, which beats the stylesheet. dist/app-main.js is
    what the browser actually runs and carries its own copy of the literal, so a
    .jsx-only assertion would pass while a stale bundle keeps writing the old
    accent everywhere. That is exactly the failure CLAUDE.md's rule exists to
    prevent, and it has happened before.
    """
    token = _root_token(style_block(INDEX), "--primary")

    jsx = APP_MAIN_JSX.read_text(encoding="utf-8")
    # The bundle is minified to a bare `accent:` where the source has `"accent":`.
    pattern = r"[\"']?accent[\"']?\s*:\s*[\"']([#0-9a-fA-F]+)[\"']"
    jsx_m = re.search(pattern, jsx)
    assert jsx_m, "TWEAK_DEFAULTS.accent not found in app-main.jsx"

    dist_m = re.search(pattern, APP_MAIN_DIST.read_text(encoding="utf-8"))
    assert dist_m, "accent literal not found in dist/app-main.js"

    assert token == jsx_m.group(1).lower() == dist_m.group(1).lower(), (
        f"accent disagrees: :root={token} jsx={jsx_m.group(1)} dist={dist_m.group(1)}"
    )


def test_extension_palette_mirror_matches():
    """extension.html restates the palette under different names.

    It is deliberately standalone (no React, no shared stylesheet) so it still
    renders for an expired session, which means it cannot read :root — its own
    comment says "Keep these values in step with those tokens" and nothing
    enforced that until now. Radii are deliberately NOT asserted: they are
    hand-copied numbers, and pinning them would make a future scale change fail
    in two places instead of one.
    """
    index_css = style_block(INDEX)
    ext_css = style_block(EXTENSION)
    for ext_name, index_name in (
        ("--bg", "--bg"),
        ("--panel", "--card"),
        ("--fg", "--text"),
        ("--muted", "--text-3"),
        ("--accent", "--primary-2"),
    ):
        assert _root_token(ext_css, ext_name) == _root_token(index_css, index_name), (
            f"extension.html {ext_name} != index.html {index_name}"
        )
```

- [ ] **Step 2: Run the new tests to verify they pass**

Run: `python -m pytest tests/api_tests/test_css_guards.py -q`
Expected: **6 passed** (2 from Task 1 + 4 here). If `test_extension_palette_mirror_matches` fails, the mirror has already drifted — fix `extension.html`'s `:root` to match rather than relaxing the test.

- [ ] **Step 3: Commit**

```bash
git add tests/api_tests/test_css_guards.py
git commit -m "test(css): guard blur orbs, clipped text, the 3-copy accent, and the extension palette mirror"
```

---

## Task 3: Bundle-freshness layer 1 — stamp consistency

**Files:**
- Create: `tests/api_tests/test_build_stamp.py`

- [ ] **Step 1: Write the test**

The digest recipe below was verified against the working tree: SHA-1 over all 11 `dist/*.js` in sorted-filename order yields `8b18457b39`, which matches all 10 `?v=` tags plus the `sw.js` cache name.

Create `tests/api_tests/test_build_stamp.py`:

```python
"""Bundle-freshness layer 1: the cache-bust stamps match the bundles.

build.sh derives its build id as `cat web/static/dist/*.js | shasum | cut -c1-10`
and stamps it into every `?v=` token in index.html and the sw.js cache name. The
app is 10 plain global scripts whose cross-file globals only line up when every
bundle is from the SAME build, so a mixed set renders a blank page.

This catches a rebuild committed without its stamps, a hand-edited bundle, and a
partial dist/ commit. It CANNOT catch a .jsx edited with ./build.sh never run —
dist/ and the stamps are both unchanged then, and this test still passes. That
case needs the node CI job (see .github/workflows/tests.yml).

Note the glob is all of dist/*.js — 11 files, including the orphan auth-page.js
— and NOT build.sh's 10-entry FILES array, because build.sh's own
`cat "$OUT_DIR"/*.js` hashes everything in the directory. Using FILES would
produce a different digest.
"""
from __future__ import annotations

import hashlib
import re

from tests.api_tests.css_helpers import INDEX, WEB


def _expected_build_id() -> str:
    digest = hashlib.sha1()
    for path in sorted((WEB / "dist").glob("*.js")):
        digest.update(path.read_bytes())
    return digest.hexdigest()[:10]


def test_index_and_sw_stamps_match_the_bundles():
    expected = _expected_build_id()

    stamps = set(re.findall(r"/static/dist/[a-z0-9-]+\.js\?v=([a-z0-9]+)", INDEX.read_text(encoding="utf-8")))
    assert stamps, "no ?v= stamped script tags found in index.html"
    assert stamps == {expected}, f"index.html stamps {stamps} != bundle digest {expected}"

    sw = (WEB / "sw.js").read_text(encoding="utf-8")
    cache_names = set(re.findall(r"coreprop-shell-([A-Za-z0-9]+)", sw))
    assert cache_names == {expected}, f"sw.js cache {cache_names} != bundle digest {expected}"


def test_every_bundle_is_stamped():
    """All 10 script tags carry a stamp — an unstamped tag would be cached forever."""
    html = INDEX.read_text(encoding="utf-8")
    tags = re.findall(r"/static/dist/([a-z0-9-]+\.js)(\?v=[a-z0-9]+)?", html)
    unstamped = [name for name, stamp in tags if not stamp]
    assert not unstamped, f"dist script tags with no ?v= stamp: {unstamped}"
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `python -m pytest tests/api_tests/test_build_stamp.py -q`
Expected: **2 passed**

- [ ] **Step 3: Commit**

```bash
git add tests/api_tests/test_build_stamp.py
git commit -m "test(build): pin the cache-bust stamps to the dist/ bundle digest"
```

---

## Task 4: Bundle-freshness layer 2 — the node CI job

The only check that catches `.jsx` edited with `./build.sh` never run. Needed before Phase 2, which is all `.jsx` work.

**Files:**
- Modify: `.github/workflows/tests.yml`

- [ ] **Step 1: Read the existing workflow**

Run: `cat .github/workflows/tests.yml`
Note the existing job's name and indentation so the new job is a sibling, not a nested key.

- [ ] **Step 2: Verify the check passes locally first**

Run:
```bash
./build.sh && git diff --exit-code web/static/dist web/static/index.html web/static/sw.js
```
Expected: build output, then **no diff** and exit code 0. If there is a diff, the committed bundles are already stale — rebuild and commit that first, or the new CI job will be red on arrival.

- [ ] **Step 3: Add the job**

Append to `.github/workflows/tests.yml` as a new top-level entry under `jobs:` (match the existing two-space indentation):

```yaml
  bundles:
    # Layer 2 of the bundle-freshness guard: catches a .jsx edited with
    # ./build.sh never run. The pytest stamp test cannot see that case — dist/
    # and the stamps are both unchanged — and Render's build env is pip-only, so
    # a stale committed bundle ships a silent no-op to production. GitHub
    # runners have node; build.sh pins esbuild via `npx --yes`.
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "20"
      - name: Rebuild the bundles
        run: ./build.sh
      - name: Fail if the committed output differs
        run: git diff --exit-code web/static/dist web/static/index.html web/static/sw.js
```

- [ ] **Step 4: Validate the YAML parses**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/tests.yml')); print('yaml ok')"`
Expected: `yaml ok`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/tests.yml
git commit -m "ci: fail when committed dist/ bundles don't match the .jsx sources"
```

---

## Task 5: Define the token scales

Adding the tokens changes almost nothing on screen — that is expected, not a bug. The migrations in Tasks 6–11 are what make them visible.

**Files:**
- Modify: `web/static/index.html` (`:root`, ~lines 55-100)
- Create: `tests/api_tests/test_css_tokens.py`

- [ ] **Step 1: Write the failing token-presence test**

Create `tests/api_tests/test_css_tokens.py`:

```python
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **4 failed** — none of the new tokens exist yet.

- [ ] **Step 3: Add the tokens to `:root`**

In `web/static/index.html`, inside `:root`, replace the three existing radius lines and the `--shadow-card` line with the block below. Keep every color token exactly as it is — no hex value changes in this design.

```css
  /* Radius scale. Five steps replace 18 distinct literal sizes. The old names
   * are kept as aliases because six landing rules still reference them; note
   * this is source compatibility, not visual identity (--radius goes 14→16px,
   * --radius-sm 10→12px). Mirror any change in ios/CoreProp/Theme/Theme.swift. */
  --r-xl:      20px;   /* .pp-card, .pp-card-glow, .lp-cta-card only */
  --r-lg:      16px;   /* cards, panels, table wrappers, modals */
  --r-md:      12px;   /* inputs, inner tiles, menus, row action buttons */
  --r-sm:      8px;    /* small chips, badges, skeletons, icon buttons */
  --r-pill:    999px;  /* buttons, filter chips */
  --radius:    var(--r-lg);
  --radius-sm: var(--r-md);
  --radius-xs: var(--r-sm);

  /* Spacing scale. There was none, which is why padding drifted. */
  --s-1:  4px;   --s-2:  8px;   --s-3: 12px;  --s-4: 16px;  --s-5: 20px;
  --s-6: 24px;   --s-8: 32px;   --s-10: 40px; --s-12: 48px;
  /* Derived: the ONLY definition of the +EV row's horizontal padding. Four
   * rules must share it (.ev-row, .ev-row-hd, .is-sel, .is-logged) — a stale
   * literal in one of their calc()s once shipped a 6px misalignment. */
  --row-px: clamp(var(--s-5), 2vw, var(--s-6));

  /* Elevation. The `0 2px 0 rgba(255,255,255,.03) inset` highlight that used to
   * lead --shadow-card is gone: that inset is the skeuomorphic tell. */
  --shadow-card: 0 12px 32px -18px rgba(0,0,0,.7);
  --shadow-pop:  0 16px 40px -12px rgba(0,0,0,.6);
  --ring:        0 0 0 4px var(--primary-hi);
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **4 passed**

- [ ] **Step 5: Sanity-check the page still renders**

Run: `python main.py` and open `http://127.0.0.1:8000`. Confirm the page loads and looks essentially unchanged (the six alias consumers on the landing page gain 2px of corner radius). Stop the server.

- [ ] **Step 6: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "feat(css): add the radius, spacing and elevation token scales"
```

---

## Task 6: Migrate the 104 literal radii

The test enumerates its own worklist: run it, get every violating selector, apply the spec's mapping, re-run. Commit per batch so a mistake is bisectable.

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html` (~104 declarations)

- [ ] **Step 1: Write the failing radius invariant test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
# Selectors whose radii are deliberately literal (spec Section 1).
#   lp-game-*  : the PrizePicks-board mimic keeps its geometry (22/26/14/7px).
#                The 26px stage sits 4px outside the 22px card so the padding
#                reads as a concentric frame, and .lp-game-wash's inset:0 only
#                aligns because it repeats the card's 22px exactly.
#   lp-sk-photo: must always equal .lp-game-photo-fb (the skeleton stands in
#                for the photo).
#   the rest   : 1-4px radii on decorative bars/keys/swatches whose short
#                dimension is 3-12px, where 8px would round them into lozenges.
RADIUS_EXEMPT = (
    "lp-game-",
    "lp-sk-photo",
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
```

- [ ] **Step 2: Run to verify it fails and capture the worklist**

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_every_radius_goes_through_a_token -q 2>&1 | tee /tmp/radius-worklist.txt`
Expected: **FAIL**, listing **74** declarations. Keep `/tmp/radius-worklist.txt` — it is the batch checklist.

(74, not 104, and both numbers are right: there are 110 `border-radius` declarations, 6 already read `var(--radius*)`, leaving 104 literals. Of those, 22 sit under `RADIUS_EXEMPT` selectors and 8 more are `50%`/`0`, both of which this test skips — so 74 is the worklist and 104 is the literal count.)

- [ ] **Step 3: Batch A — the `--r-sm` group (5, 6, 7, 8, 9px → 8px)**

The largest batch, ~40 declarations. Replace each `border-radius:{5,6,7,8,9}px` with `border-radius:var(--r-sm)`. Includes: `.ev-add-btn`, `.ev-check`, `.ev-leg-i`, `.ev-leg-x`, `.ev-empty-slip`, `.ev-sharp`, `.ev-stepper`, `.ev-clear`, `.bd-odds.is-best`, `.bd-pag-btn`, `.bd-clear`, `.bt-slip-badge`, `.bt-leg-i`, `.bt-leg-actual`, `.bt-leg-side`, `.bt-slip-del`, `.bt-slip-ev`, `.obs-pill`, `.obs-mult-cell`, `.cp-link`, `.cp-skel`, `.cp-menu-item`, `.cp-modal-x`, `.cp-seg-btn` (9px), `.cp-book` (4px → 8px: it is a text badge, not a bar), `.pp-book-logo`, `.pp-pay-chip`, `.pnl-range-btn`, `.lp-bk-pill`, `.lp-sk`, `.lp-game-ctap-x`, and the global `:where(button,a,summary,input,select,[tabindex]):focus-visible` fallback radius (~L137).

Two easy-to-miss areas: the compact-slip block at ~L1297-1392 carries its own 4/5/6/7px badge radii that the base `.bt-*` rules do not cover, and the `:focus-visible` fallback applies to every focusable element on the page.

**Expected visible change:** the 5/6/7px elements grow 1-3px. That is intended on roughly 30 elements 15-24px tall — it is the single most reviewable part of this task.

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_every_radius_goes_through_a_token -q`
Expected: still FAIL, with the count down to **32** (43 rewrites).

```bash
git add web/static/index.html
git commit -m "refactor(css): move the 5-9px radii onto --r-sm"
```

- [ ] **Step 4: Batch B — the `--r-md` group (10, 12px → 12px)**

Replace with `var(--r-md)`: `.cp-menu`, `.cp-seg`, `.cp-input`, `.cp-btn-save`, `.pnl-custom`, `.bd-filters`, `.bd-tbl-wrap`, `.bt-card`, `.pp-faq details`, `.cal-curves`, `.lp-bk-row`.

Run the test again; the count drops further.

```bash
git add web/static/index.html
git commit -m "refactor(css): move the 10-12px radii onto --r-md"
```

- [ ] **Step 5: Batch C — the `--r-lg` group (14, 16, 18px → 16px)**

Replace with `var(--r-lg)`: `.ev-filters` (a raw `14px`, not the alias — it must be retokenized explicitly), `.ev-table`, `.bt-slip`, `.an-panel`, `.cp-modal` (18px → 16px).

```bash
git add web/static/index.html
git commit -m "refactor(css): move the 14-18px card radii onto --r-lg"
```

- [ ] **Step 6: Batch D — `--r-xl`, the pill promotions, and the three derived radii**

`--r-xl` has exactly three consumers:

```css
.pp-card{border-radius:var(--r-xl)}
.pp-card-glow{border-radius:calc(var(--r-xl) + 1px)}
.lp-cta-card{border-radius:var(--r-xl)}
```

`.lp-cta-card` is promoted out of the `var(--radius)` alias group (14px → 20px) because it is the landing page's hero CTA card. `.pp-card-glow` is `inset:-1px` over `.pp-card` and must stay exactly 1px larger or the two rings diverge by 5px at each corner and read as a double-outline artifact — express it as `calc()`, never a new literal.

Section 2 puts filter chips on the pill shape, so **`.ev-chip` and `.bd-chip` take `--r-pill`, not `--r-sm`** (per-element assignments override the value mapping). Convert the nine existing `999px` literals to `var(--r-pill)` as well: `.cp-btn`, `.cp-tab`, `.pp-toggle`, `.pp-tg-btn`, `.pp-save`, `.pp-card-trial`, `.bd-badge` (leave `.lp-game-evchip` / `.lp-game-minbar` alone — minigame).

The two mobile derived radii, in the `max-width:820px` block:

```css
.ev-slip-toggle{border-radius:0 0 calc(var(--r-lg) - 1px) calc(var(--r-lg) - 1px)}
.ev-slip.ev-slip-mobile{border-radius:0 0 var(--r-lg) var(--r-lg)}
```

The toggle subtracts 1px because it bleeds edge-to-edge *inside* the tile; the mobile slip is a bordered sibling and matches `.ev-filters` exactly.

- [ ] **Step 7: Run to verify the invariant now passes**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **5 passed** (zero remaining literal radii).

- [ ] **Step 8: Verify the minigame was not touched**

Run: `git diff web/static/index.html | grep -E "^[-+].*lp-game-" | grep -v "lp-game-ctap-x"`
Expected: **no output** except possibly `.lp-sk`. Any other `lp-game-` line in the diff is an exemption violation — revert it.

- [ ] **Step 9: Commit**

```bash
git add web/static/index.html
git commit -m "refactor(css): finish the radius migration (--r-xl, pills, derived radii)"
```

---

## Task 7: Flatten the twelve neutral card gradients

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html` (12 rules + 1 sticky-header fill)

- [ ] **Step 1: Write the failing flat-surface test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
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
        seen.add(selector.strip())
        body = squash(decls)
        if "linear-gradient" in body:
            violations.append(f"{selector} still has a gradient")
        elif squash(target) not in body:
            violations.append(f"{selector} should use {target}")
    missing = set(FLATTENED_SURFACES) - seen
    # .cal-curves is dead (no markup references it) and may have been deleted.
    missing.discard(".cal-curves")
    assert not violations and not missing, f"violations={violations} missing={sorted(missing)}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_neutral_card_surfaces_are_flat -q`
Expected: **FAIL**, naming the rules that still carry gradients.

- [ ] **Step 3: Flatten each surface**

Replace each `background:linear-gradient(180deg,…)` with the flat value from the table above. Two rules need extra care:

- **`.bd-tbl-wrap`** — also change the `.bd-tbl thead th` sticky-header fill (a hardcoded `#13131c`, ~L970) to `var(--card)`, or the sticky header stops matching its wrapper.
- **`.ev-slip.ev-slip-mobile`** (~L1476, a *second copy* inside the mobile `@media` block) — must become `var(--card)` to match `.ev-filters` (L635), **not** `var(--bg-2)` like the desktop `.ev-slip`. Its comment requires the "same surface as the tile" so the drawer reads as falling out of the filter tile. Editing L775 alone puts a visible seam across a borderless join on the phone +EV screen.

`.cal-curves` is referenced by no JSX, bundle, or HTML — flatten it to `var(--bg)` or delete the dead rule; both are acceptable.

**Expected visible change:** every card and panel lightens by one shade and loses its top-to-bottom falloff. That is the intended effect.

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **6 passed**

- [ ] **Step 5: Confirm the state fills survived**

Run: `grep -cE "linear-gradient\(180deg" web/static/index.html`
Expected: a non-zero count (~20). These are the shimmers, outcome bars, result tints, button state fills and minigame gradients, all of which stay. A count of 0 means the flattening was applied with a blanket replace — revert and redo per-selector.

- [ ] **Step 6: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "refactor(css): flatten the twelve neutral card gradients"
```

---

## Task 8: Flatten the buttons — white inset, glows, focus rings, heights

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html` (9 rules + the already-done token)

- [ ] **Step 1: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
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
```

Add `has_accent` to the imports at the top of `test_css_tokens.py`:

```python
from tests.api_tests.css_helpers import INDEX, declarations, has_accent, rules, squash, style_block
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **FAIL** on both new tests — 9 hardcoded insets and the accent glows.

- [ ] **Step 3: Remove the nine hardcoded insets**

Delete the `0 2px 0 rgba(255,255,255,.03) inset` segment from each: `.cp-btn-save` (~152), `.cp-tab.is-active` (~205), `.cp-seg-btn.is-active` (~243), `.pp-tg-btn.is-on` (~839), `.pnl-range-btn.is-custom.is-on` (~1111, already done in Task 1 — verify), `.ev-prefs-save` (~1201) and its `:hover` (~1204), `.bt-slip-place` (~1223) and its `:hover` (~1226).

Where removing the inset empties the `box-shadow`, delete the whole declaration rather than leaving `box-shadow:;`.

- [ ] **Step 4: Remove the accent glows**

On `.ev-prefs-save` and `.bt-slip-place` (and their `:hover` rules), remove the blue drop-glow `0 6px 14px -6px rgba(30,111,176,.55)`. Also remove the `transform:translateY(-1px)` hover lift on those two if present — flat buttons do not lift; the existing `.cp-btn:active{transform:translateY(1px)}` press remains the only motion.

Also remove the green glow on `.cp-btn-save` (`0 6px 16px -8px rgba(22,163,74,.5)` or similar). The automated test will not flag it because it is green, not accent — but "flat buttons, no glow" applies to it too. Note it in the commit message so the omission from the test is a recorded decision, not an oversight.

- [ ] **Step 5: Consolidate the two 3px focus rings onto `--ring`**

`--ring` (defined in Task 5) is the existing 4px ring from `.cp-input:focus`. Two rules use a 3px variant of the same effect and consolidate onto the token:

- `.ev-auto input:focus-visible ~ .ev-check` (~L788)
- `.pnl-custom-input:focus` (~L1121)

Replace each ring `box-shadow` with `box-shadow:var(--ring)`. Also point `.cp-input:focus` itself at the token so there is one definition:

```css
.cp-input:focus{border-color:var(--primary);box-shadow:var(--ring)}
```

Leave the 2px `outline` on the global `:where(button,a,summary,input,select,[tabindex]):focus-visible` fallback (~L137) alone — it is a different mechanism (`outline`, not `box-shadow`) and must keep working when a `box-shadow` is suppressed.

Run: `grep -n "0 0 0 3px\|0 0 0 4px" web/static/index.html`
Expected: **no output** — every ring now goes through `--ring`.

- [ ] **Step 6: Standardize the button heights**

Give `.cp-btn` and its size variants consistent heights of 32 / 38 / 44px, using the spacing scale for the vertical padding and keeping the existing pill shape and font sizes:

```css
.cp-btn-sm{padding:var(--s-2) 14px;font-size:13px}    /* 34px */
.cp-btn{padding:10px var(--s-4);font-size:14px}        /* 39px */
.cp-btn-lg{padding:13px 22px;font-size:15px}           /* 46px */
```

The measured boxes are **34 / 39 / 46px**, not the 32/38/44 an earlier draft of
this plan claimed — that draft's comments didn't match the CSS beside them. Keep
the CSS and the real numbers: 34px is the better landing anyway, because it puts
`.cp-btn-sm` on the same baseline as `.cp-input-sm` and the 34px filter-bar
controls, so a small button beside an input matches natively instead of being
flex-stretched.

**Filter-bar buttons are exempt from this scale.** The 34px filter-bar height is a load-bearing alignment contract: `.bd-clear`, `.ev-clear` and the pager buttons sit beside the 34px `.bd-badge`, `.bd-pag` and `.bd-f select`, so pulling them to 32px or 38px visibly breaks the row. Leave their heights at 34px and only change their radius/colors.

- [ ] **Step 7: Run to verify the tests pass**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **8 passed**

- [ ] **Step 8: Verify the filter bars are still aligned**

Run `python main.py`, open the Boards tab and the +EV filter tile, and confirm every control in each filter bar still shares one baseline. Stop the server. This is the exact regression the exemption in Step 6 exists to prevent.

- [ ] **Step 9: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "refactor(css): flat buttons — drop the inset and glows, tokenize focus rings, standardize heights

Filter-bar buttons deliberately stay at 34px, exempt from the 32/38/44 scale,
because they align against the 34px badge/pager/select in the same row.
The green glow on .cp-btn-save is removed too; the accent-glow test cannot
flag it because it is green, so this notes it explicitly."
```

---

## Task 9: Migrate the spacing, including the four coupled +EV padding sites

The highest-risk task: a stale literal in one `calc()` once shipped a 6px content misalignment at wide viewports.

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html`

- [ ] **Step 1: Write the failing coupling test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
def test_ev_row_horizontal_padding_is_always_row_px():
    """Four rules must share the +EV row's horizontal padding, or the selected
    and logged rows fall out of alignment with the header row.

    .ev-row and .ev-row-hd are each declared TWICE — a base rule and an
    unconditional re-declaration in the density-overrides block that wins on
    source order. Editing only the base rule ships a no-op.
    """
    css = style_block(INDEX)
    found = {".ev-row": 0, ".ev-row-hd": 0, ".ev-row-data.is-sel": 0, ".ev-row-data.is-logged": 0}
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
    # Both copies of .ev-row / .ev-row-hd must be migrated, not just the base.
    assert found[".ev-row"] >= 2, f".ev-row: expected both copies, found {found['.ev-row']}"
    assert found[".ev-row-hd"] >= 2, f".ev-row-hd: expected both copies, found {found['.ev-row-hd']}"
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **FAIL** — the four sites still carry `clamp(18px,2vw,24px)` literals and `.is-sel`'s bar is 2px.

- [ ] **Step 3: Migrate the four coupled sites**

Before editing, list every copy: `grep -n "\.ev-row\b\|\.ev-row-hd\b\|is-sel\|is-logged" web/static/index.html`

Then apply:

```css
/* base (~L697) and the density-overrides copy (~L1191) — BOTH */
.ev-row{padding:var(--s-4) var(--row-px)}
/* base (~L709) and the density-overrides copy (~L1192) — BOTH */
.ev-row-hd{padding:var(--s-4) var(--row-px)}
/* ~L723 — bar goes 2px -> 3px so both states share one compensation value.
   The background stays var(--primary-lo): --primary-hi (.22) is inherited by
   .ev-time's --text-3 and measures 4.45:1, under the AA floor. Task 1 fixed
   that; do not "restore" --primary-hi here. Guarded by
   test_primary_hi_never_backs_inherited_text. */
.ev-row-data.is-sel{border-left:3px solid var(--primary);background:var(--primary-lo);padding-left:calc(var(--row-px) - 3px)}
/* ~L743 — keep the existing !important; it is load-bearing against the
   runtime-injected hover rule in app-main.jsx */
.ev-row-data.is-logged{border-left:3px solid var(--red);padding-left:calc(var(--row-px) - 3px)}
```

Vertical padding goes `13px → --s-4` (16px), a genuine increase. Horizontal **keeps its fluid clamp** via `--row-px` (20-24px, up from 18-24px) — collapsing it to a flat 16px would *reduce* wide-viewport padding from 24px to 16px, the opposite of the airy goal.

Update the comment at ~L717-722 to cite `--row-px` instead of the clamp, so the next editor sees the coupling.

- [ ] **Step 4: Migrate the remaining duplicate-declaration surfaces**

For each row of the table below, apply the spacing tokens to **every** copy. The density-override copy is unconditional (not in a media query) and wins on source order, so a base-only edit is a silent no-op.

| Selector | base | density override | `@media` |
|---|---|---|---|
| `.cp-nav` | :171 | :1180 | :1416 (820) |
| `.cp-tab` | :198 | :1182 | :1428 (820) |
| `.ev-main` | :632 | :1170 | :1442, :1536 (900) |
| `.ev-filters` | :635 | :1185 | :1457 (900), :1507 (560) |
| `.ev-slip` | :775 | :1171 | :1474 (900) |
| `.pp` | :827 | :1176 | — |
| `.bd-page` | :928 | :1168 | :1535 (900) |
| `.bd-filters` | :929 | :1186 | :1507 (560) |
| `.bd-tbl thead th` | :970 | :1189 | — |
| `.bd-tbl tbody td` | :976 | :1190 | — |

Keep every `clamp()` fluid and tokenize its endpoints. Example:

```css
.bd-tbl tbody td{padding:var(--s-3) clamp(var(--s-4),1.4vw,var(--s-5))}
```

Do **not** flatten a `clamp()` to a single token.

The `9px` values on `.ev-leg`, `.lp-bk-row`, `.cp-tab`, `.cp-menu-item`, `.ev-clear` and `.cp-seg-btn` move to `--s-3` as their own elements dictate. None of them is the +EV row.

- [ ] **Step 5: Run to verify it passes**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **10 passed**

- [ ] **Step 6: Verify the alignment by eye — this is the regression this task exists to prevent**

Run `python main.py`, open `http://127.0.0.1:8000`, sign in, go to +EV Bets, and at a **1440px** viewport confirm: a plain row, a selected row and a logged row all have their player names starting at the same x-position. Then narrow to **375px** and confirm the same. Stop the server.

- [ ] **Step 7: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "refactor(css): put spacing on the scale, with --row-px coupling the four +EV padding sites"
```

---

## Task 10: Typography

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Reduce the micro-label letter-spacing**

Find every `letter-spacing:.1em` in the `<style>` block:

Run: `grep -n "letter-spacing:\.1em" web/static/index.html`

Change each to `letter-spacing:.04em`. The all-caps-everywhere idiom with wide tracking is the strongest dated-dashboard signal in the current UI.

- [ ] **Step 2: Keep uppercase only on true headers**

Run: `grep -n "text-transform:uppercase" web/static/index.html`

Keep `text-transform:uppercase` **only** on true section headers and table headers (`.ev-row-hd`, `.bd-tbl thead th`, `.an-section-h`, `.pnl-title`). Remove it from inline micro-labels (stat-card labels, sub-captions, chip labels). Removing `text-transform` never changes layout width in a way that breaks a grid — these are all `auto`-width labels — but re-check the Boards filter bar at Step 4.

- [ ] **Step 3: Step the chart total down**

Find `.pnl-total` (~L1091) and change `font-size:38px` to `font-size:34px`. It sits better against the flattened panel.

- [ ] **Step 4: Verify no filter-bar control left the 34px baseline**

The 34px filter-bar height is a load-bearing alignment contract across filter bars, and it applies to **every** control in the bar including buttons (filter-bar buttons are exempt from the 32/38/44 button height scale).

Run `python main.py`, open the Boards tab, and confirm the filter inputs, selects, chips, the count badge, the clear button and the pager all still share one baseline. Stop the server.

- [ ] **Step 5: Run the full suite**

Run: `python -m pytest tests/ -q`
Expected: `396 passed` (378 existing + 18 new: 6 guards + 2 stamp + 10 tokens). Task 10 adds no test of its own — the uppercase/tracking change is verified visually in Step 4.

- [ ] **Step 6: Commit**

```bash
git add web/static/index.html
git commit -m "refactor(css): calmer micro-labels, uppercase only on real headers"
```

---

## Task 11: The `--text-4` contrast fix

`--text-4` is 2.3:1 and is for decorative glyphs only. `.cp-input::placeholder` currently uses it — placeholder text is text a user reads, and it is the only hint of what to type in the +EV prop search.

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html` (1 rule)

- [ ] **Step 1: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
# --text-4 is 2.3:1 — decorative/disabled glyphs only, never readable text.
# These six are single-glyph decorations: a middot, two em dashes, a close X
# (which carries an aria-label), an arrow, and an empty heat cell.
TEXT4_ALLOWED = (
    ".ev-meta-dot",
    ".bd-odds-empty",
    ".bd-edge-cell",
    ".bt-slip-del",
    ".pnl-custom-arrow",
    ".obs-heat-cell.is-empty",
)


def test_text4_is_only_on_decorative_glyphs():
    violations = []
    for selector, decls in rules(style_block(INDEX)):
        if "var(--text-4)" not in squash(decls).replace(" ", ""):
            continue
        if any(allowed in selector for allowed in TEXT4_ALLOWED):
            continue
        violations.append(selector.strip())
    assert not violations, (
        "--text-4 (2.3:1) used on readable text: " + ", ".join(violations)
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_text4_is_only_on_decorative_glyphs -q`
Expected: **FAIL**, naming `.cp-input::placeholder`.

- [ ] **Step 3: Fix the placeholder**

Find `.cp-input::placeholder` (~L166) and change `var(--text-4)` to `var(--text-3)`:

```css
.cp-input::placeholder{color:var(--text-3)}
```

This is the same correction already applied to `.lp-foot-disc` and `.pp-respo`. The `--text-4` token *value* is unchanged.

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **11 passed**

- [ ] **Step 5: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "fix(a11y): placeholder text uses --text-3, not the 2.3:1 --text-4"
```

---

## Task 12: The `extension.html` radius pass

The one served surface outside `index.html`. It stays standalone — no React, no shared stylesheet, so it still renders for an expired session — which means it cannot read `:root` and its radii are hand-copied numbers.

**Files:**
- Modify: `web/static/extension.html`

- [ ] **Step 1: Read the current values**

Run: `grep -nE "border-radius|:root|--(bg|panel|line|fg|muted|accent)" web/static/extension.html`

- [ ] **Step 2: Adopt the scale by value**

Its radii are 10 / 9 / 5 / 4px today. Apply the scale as literals (it has no tokens to reference):
- the `.status` panel and the `.dl` button → `12px` (the `--r-md` value)
- `code` and the focus-visible fallback → `8px` (the `--r-sm` value)

Leave the five mirrored color roles exactly as they are — `test_extension_palette_mirror_matches` pins them to `index.html`, and no token hex changed in this design. `.dl` keeps its dark text (`#04121f`) on `--accent`: that accent is `--primary-2`, which must never carry white text.

Do not touch the copy, the status-beacon script, or the download link.

- [ ] **Step 3: Verify the mirror test still passes**

Run: `python -m pytest tests/api_tests/test_css_guards.py -q`
Expected: **5 passed**

- [ ] **Step 4: Verify the page renders**

Run `python main.py`, open `http://127.0.0.1:8000/extension`, confirm the panel and download button render with the new corners. Stop the server.

- [ ] **Step 5: Commit**

```bash
git add web/static/extension.html
git commit -m "refactor(css): adopt the radius scale by value on the extension page"
```

---

## Task 13: Phase-1 verification

**Files:** none modified — this task is verification only.

- [ ] **Step 1: Full suite**

Run: `python -m pytest tests/ -q`
Expected: `397 passed` (378 existing + 19 new: 6 in `test_css_guards.py`, 2 in `test_build_stamp.py`, 11 in `test_css_tokens.py`). No failures, no skips.

- [ ] **Step 2: Confirm no `.jsx` or `dist/` file was touched**

Run: `git diff --stat main..HEAD -- 'web/static/*.jsx' web/static/dist`
Expected: **no output.** Phase 1 is static-file-only; any hit here means work leaked in from Phase 2 and the bundles are now stale.

- [ ] **Step 3: Confirm the payout tables and marketing copy are untouched**

Run: `python -m pytest tests/engine_tests/test_payout_table_mirror.py tests/api_tests/test_landing_claims.py -q`
Expected: all pass.

- [ ] **Step 4: Contrast check**

Confirm by inspection of `:root` and the rules changed: white on `--primary` (`#1E6FB0`) is ≥ 4.5:1; `--text-3` (`#8a8a9b`) is ≥ 4.5:1 on both `--bg` and `--card`; `--text-4` appears only on the six decorative selectors (already asserted by `test_text4_is_only_on_decorative_glyphs`). No token hex changed, so the first two hold by construction — record that rather than re-deriving.

- [ ] **Step 5: Mandatory visual captures**

Run `python main.py` and capture before/after at **375px** and **1440px**. These four are mandatory because no test covers them:

1. The +EV row in **plain, selected and logged** states, **with hover on each** — the alignment regression this phase risks most.
2. The +EV slip toggle in **both closed and open** states at ≤820px — the derived-radius trap; a 1-5px corner step here reads as a rendering bug.
3. The true% column showing the **54-75% green ramp still varying** with the value (it is an inline `oklch()` and no CSS can touch it, but confirm it survived).
4. The Boards filter bar with **every control on the 34px baseline**.

Also spot-check the ~30 small elements that grew 1-3px in Task 6 Batch A: book tags, leg-index squares, pagination buttons.

- [ ] **Step 6: Confirm the minigame is visually unchanged**

Open the landing page and confirm the hero minigame card looks exactly as before — same 22px card corners, 26px stage, colors, and the green idle nudge. It is a deliberate PrizePicks-board mimic and is exempt from this entire phase.

- [ ] **Step 7: Push and confirm CI**

```bash
git push origin main
```

Then confirm both CI jobs pass, including the new `bundles` job:

```bash
gh run list --branch main --limit 3
```

---

## Definition of done (Phase 1)

- `pytest tests/ -q` → 397 passed (378 existing + 19 new).
- Zero literal `border-radius` declarations outside the documented exemptions.
- Zero neutral card gradients; zero white inset highlights; zero accent gradients; zero accent glows.
- Every focus ring goes through `--ring`; button heights are 32/38/44 **except** filter-bar buttons, which stay at 34px.
- The four +EV horizontal-padding sites all read `var(--row-px)`; both left bars are 3px.
- `.cp-input::placeholder` uses `--text-3`.
- No `.jsx` or `dist/` file modified.
- `extension.html` radii updated; its palette mirror still matches.
- CI green, including the new `bundles` job.
- The six mandatory visual captures reviewed and accepted.

## Accepted trades (decided at Task 13, not defaults)

- **+EV row density drops ~10.7%** — 13.16 → 11.75 rows above the fold at
  1440×1200, because `.ev-row`'s vertical padding went `13px → --s-4` (16px).
  **Kept deliberately.** The user chose the "airy list" direction specifically,
  and roomier rows are the point of it rather than a side effect; `--s-3` (12px)
  would recover the density but undo the change. Revisit only if the taller rows
  actually read as wasteful in use.
- **One radius token spans two perceptual tiers.** At `--r-sm` (8px), elements
  ≤18px tall read as pills or circles (`.cp-book` book tags, the 18px
  `.ev-leg-i` / `.bt-leg-i` index squares) while 24-28px elements read as
  rounded rectangles. Inherent to a five-step scale, not a defect: the numbered
  leg indices reading as circles suits a step list. `.ev-check` was exempted
  because the same effect would have made a checkbox look like a radio button.
- **Semantic-colour surfaces keep their gradients** while every neutral surface
  is flat, so on desktop a flat blue selected row sits above a still-gradient red
  logged row. That asymmetry is the spec's intent (Phases 2-3 flatten the ones
  the visual design reaches) but it is not written as a rule anywhere, and it is
  the one place the pass looks unfinished. Worth flattening
  `.ev-row-data.is-logged{,:hover,.is-sel}` to the flat tints the ≤560px block
  already uses.

## Deferred to Phase 2 (found during Phase 1, needs a `.jsx` edit or a design call)

- `--primary-lo` is not written by `app-main.jsx`'s accent effect, which writes
  `--primary` and `--primary-hi` as inline styles. Latent only (the tweaks panel
  needs a host `postMessage`, and the default accent equals the token), but it is
  a new accent-derived token sitting outside the one place that keeps accent
  tokens in step.
- `.bt-card` nested in `.an-panel` is same-tone-on-same-tone, separated by a
  single 6%-alpha hairline now that flattening removed the tiles' top-edge lift.
  Not a regression (both carried the identical gradient before) and it reads
  acceptably, but `--card-2` exists for exactly this.
- `.bd-tbl thead th`/`.bd-tbl tbody td` right-cluster controls share a *centre*,
  not a baseline, with the bottom-aligned `.bd-f` columns — a 9.5px offset,
  pre-existing.
- `.pp-card-hd` has no flex `gap`, so its tag and pill still collide at 360px and
  320px. Pre-existing; the collision threshold improved from 414px to 375px.
- `--shadow-pop`, `--s-1` and `--radius-xs` are defined but unused. `--shadow-pop`
  is scaffolding the spec assigns to Phase 2's menus/modals; the other two are
  scale completeness. Use or drop them.

**Next:** Phase 2 (web component/markup work) gets its own plan. It starts with the airy +EV row and must open by re-checking the three deferred items listed under Scope, plus the five above.

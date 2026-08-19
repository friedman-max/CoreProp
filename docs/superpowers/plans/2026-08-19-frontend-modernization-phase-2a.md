# Frontend Modernization — Phase 2a (the +EV screen) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the +EV screen's 9-column table row with the approved "airy list" row — two zones, wrapping book chips, the true% as the hero number — and finish the flattening the design calls for on that screen. Still purely visual: no handler, API call, or state logic changes.

**Architecture:** Unlike Phase 1, this phase edits `.jsx`, so **`./build.sh` + committed `dist/` is mandatory** and CI-enforced by the `bundles` job. The CSS lives in `web/static/index.html`'s single `<style>` block; the markup in `web/static/ev-page.jsx` and `web/static/app-main.jsx`. Nearly every rule this phase touches is **declared twice** — a base rule and an unconditional "density overrides" copy later in source order that wins — so a base-only edit is a silent no-op.

**Tech Stack:** React 18 (no module system — plain global scripts), esbuild via `./build.sh`, pytest, GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` (Sections 2, 3 "+EV Bets"; Phasing item 2).
**Predecessor:** `docs/superpowers/plans/2026-08-19-frontend-modernization-phase-1.md` (merged; token scales + CSS-only migration).

---

## Scope

**In scope:** the +EV screen only — `web/static/ev-page.jsx`, the `.ev-*` CSS in `index.html`, `app-main.jsx`'s injected stylesheet, and the two shared state-button rules the same edit must fix.

**Phase 2b (separate plan):** Boards table, Backtest cards, Analytics panels/charts, the account menu, `.an-panel .bt-card` tone, the shared state classes, and `--red-2`.

**Out of scope entirely:** `tweaks-panel.jsx`, `analytics-preview.html`, `auth-page.jsx`. Any selector containing `lp-game-`. `components.jsx` should not need to change — `TruePct`, `LeaguePill` and `BookBadge` are all consumed as-is.

---

## Decisions this plan makes (the spec leaves these open)

1. **The header row goes.** The airy design has no table header. Delete the markup *and* the CSS, and amend `test_ev_row_horizontal_padding_is_always_row_px` to stop requiring `.ev-row-hd` in the same commit. Keeping dead CSS solely to satisfy a test is rot.
2. **Logged-row tint = the flat values already shipping on mobile** (`rgba(239,68,68,.16)` / `:hover .22` / `.is-logged.is-sel .26`), not `--red-hi` (.10). This both flattens the desktop gradient *and* makes the two surfaces agree, which is the point; `--red-hi` at .10 would materially weaken a signal that means "already logged, won't be picked."
3. **`.is-sel` keeps its blue on hover.** Today the injected white hover rule overrides it, so a selected row loses its tint under the cursor. Add `.ev-row-data.is-sel:hover` with `!important` (same mechanism and comment as `.is-logged`).
4. **Player names un-truncate at all widths.** The identity line now owns the full row width, so the `@media (max-width:560px)` un-truncation is promoted and the `title` attribute stays.
5. **`is-hov` stays.** It is a dead class driving a re-render per mousemove, but removing state is a behavior change and this phase is visual-only. Documented for a later cleanup.
6. **`--primary-lo` stays outside the accent-sync effect.** `TweaksPanel` never mounts in production, and adding a third `setProperty` means hand-computing an alpha-composited value. Documented instead.
7. **`.ev-table{overflow-x:auto}` stays** as a safety net after `min-width:620px` goes; wrapping chips could still overflow a very narrow viewport.
8. **The dead `@media (max-width:1100px) .ev` rule stays dead.** Fixing it changes a layout breakpoint, which is not a styling change. Documented.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `web/static/index.html` | Modify | All `.ev-*` CSS: row layout, states, chips, add button, mobile block |
| `web/static/ev-page.jsx` | Modify | Row markup restructure, GD badge radius, empty state |
| `web/static/app-main.jsx` | Modify | The injected stylesheet's compact padding + row hover |
| `web/static/dist/*.js` + `index.html` `?v=` + `sw.js` | Regenerate | `./build.sh` output — committed, never hand-edited |
| `tests/api_tests/test_css_tokens.py` | Modify | Amend the `.ev-row-hd` pin; add the airy-row invariants |

---

## Task 1: Amend the header test pin, then delete the header

The test currently requires two `.ev-row-hd` padding declarations. The header is being removed, so the pin must move first — otherwise the suite blocks the deletion.

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/ev-page.jsx` (delete the header block)
- Modify: `web/static/index.html` (delete `.ev-row-hd`, `.ev-th-sort`, `.ev-arrow` rules and the padding copy)

- [ ] **Step 1: Establish the baseline**

Run: `python -m pytest tests/ -q`
Expected: **397 passed**. If not, stop — something is wrong before you started.

- [ ] **Step 2: Amend the coupling test**

In `tests/api_tests/test_css_tokens.py`, find `test_ev_row_horizontal_padding_is_always_row_px`. Remove `.ev-row-hd` from the `found` dict and delete its assertion, and update the docstring to record why:

```python
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
    assert found[".ev-row"] >= 2, f".ev-row: expected both copies, found {found['.ev-row']}"
    assert found[".ev-row-data.is-sel"] >= 1
    assert found[".ev-row-data.is-logged"] >= 1
```

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **11 passed** (still green — the header CSS still exists at this point).

- [ ] **Step 3: Delete the header markup**

In `web/static/ev-page.jsx`, delete the `<div className="ev-row ev-row-hd">` block and its nine child `<span>`s (around lines 661-671). None of them has an `onClick` — the ↓ arrow is decorative, and the only real sort control is the filter-bar toggle, which stays.

- [ ] **Step 4: Delete the header CSS**

In `web/static/index.html`, delete the `.ev-row-hd` rule (~:825), the `.ev-th-sort` and `.ev-arrow` rules (~:852-853), and the `.ev-row-hd{padding:var(--s-4) var(--row-px)}` copy in the density block (~:1382).

- [ ] **Step 5: Rebuild and verify**

Run:
```bash
./build.sh
python -m pytest tests/ -q
```
Expected: build succeeds, **397 passed**. `./build.sh` re-stamps `index.html` and `sw.js`; `test_build_stamp.py` will fail if you skip it.

- [ ] **Step 6: Confirm no stale reference to the deleted classes**

Run: `grep -rn "ev-row-hd\|ev-th-sort\|ev-arrow" web/static/ tests/`
Expected: no hits outside `dist/` (which is regenerated) — if a `.jsx` or a test still names them, fix it.

- [ ] **Step 7: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/ev-page.jsx web/static/index.html web/static/dist web/static/sw.js
git commit -m "refactor(ev): drop the table header ahead of the airy row

The airy list has no column header, so the header row and its CSS go. The
padding-coupling test pinned .ev-row-hd, so that pin is amended in the same
commit — the coupling that still matters is row <-> selected <-> logged."
```

---

## Task 2: The airy row layout (CSS)

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Replace the 9-track grid**

The current rule (~:799):

```css
.ev-row{
  display:grid;
  grid-template-columns:1.5fr .55fr 1.25fr .5fr .6fr .65fr .95fr minmax(86px,.8fr) .45fr;
  gap:var(--s-3);
  align-items:center;
  padding:var(--s-4) var(--row-px);
  border-bottom:1px solid var(--hair);
  font-size:13.5px;
  cursor:default;
  transition:background .12s ease;
}
```

Becomes a two-zone flex row. Keep the padding, the hairline, the font-size, the cursor and the transition exactly as they are:

```css
/* Airy row: one flex line with an identity/meta zone that grows and a fixed
 * right zone for the hero true% + add button. This replaced a 9-track grid; the
 * 9 columns now live as two stacked lines inside .ev-row-main (see ev-page.jsx).
 * Padding stays on --row-px — and the copy in the density-overrides block below
 * is the one that actually wins, so both must carry it. */
.ev-row{
  display:flex;
  align-items:flex-start;
  gap:var(--s-4);
  padding:var(--s-4) var(--row-px);
  border-bottom:1px solid var(--hair);
  font-size:13.5px;
  cursor:default;
  transition:background .12s ease;
}
.ev-row-main{flex:1;min-width:0;display:flex;flex-direction:column;gap:6px}
.ev-row-side{display:flex;align-items:center;gap:var(--s-3);flex-shrink:0}
.ev-row-meta{display:flex;align-items:center;flex-wrap:wrap;gap:6px;font-size:12.5px;color:var(--text-3)}
```

- [ ] **Step 2: Keep `.ev-row>span{min-width:0}` working**

That rule (~:822) targeted direct `<span>` children of the grid. The airy row's direct children are `.ev-row-main` and `.ev-row-side` divs. Replace it with `.ev-row-main>*{min-width:0}` so long player names still shrink instead of overflowing.

- [ ] **Step 3: Verify the padding copy still wins correctly**

Run: `grep -n "^\.ev-row{" web/static/index.html`
Expected: two hits — the base rule you just edited and the density copy. Confirm the density copy still reads `padding:var(--s-4) var(--row-px)`.

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/ -q`
Expected: **397 passed**. (CSS-only so far — no rebuild needed for this task, but running `./build.sh` is harmless and idempotent.)

- [ ] **Step 5: Commit**

```bash
git add web/static/index.html
git commit -m "feat(ev): two-zone flex layout for the airy row"
```

---

## Task 3: The row markup restructure

The marquee change. Every handler, the key expression, and the class string must survive byte-identical.

**Files:**
- Modify: `web/static/ev-page.jsx`

- [ ] **Step 1: Read the current row block**

It is the `bets.map(...)` block (~:672-716). Read it in full before editing. The verbatim current structure:

```jsx
{bets.map((b, i) => {
  const key = b.id || (b.player + b.prop + b.line);
  const isSel = selected.find(p => p.key === key);
  return (
    <div
      key={key + i}
      className={"ev-row ev-row-data "
        + (isSel ? "is-sel " : "")
        + (hovered === key ? "is-hov " : "")
        + (b.inBacktest ? "is-logged " : "")}
      onMouseEnter={() => setHovered(key)}
      onMouseLeave={() => setHovered(null)}
      onClick={() => toggleBet(b)}
      style={{ animationDelay: (i * 14) + "ms" }}
      title={b.inBacktest ? "Already logged — won't be picked for new slips" : undefined}
    >
      ... nine <span> columns ...
    </div>
  );
})}
```

- [ ] **Step 2: Restructure the children only**

Keep the outer `<div>` and every one of its attributes **exactly as they are**. Replace only its children:

```jsx
      <div className="ev-row-main">
        <div className="ev-player">
          <span className="ev-player-n" title={b.player}>{b.player}</span>
          {b.isGreenDevil && <span className="ev-gd" title="Green devil (PrizePicks goblin) — discounted, higher-hit-rate line" style={{ marginLeft: 6, padding: "1px 6px", borderRadius: "var(--r-sm)", background: "#16a34a", color: "#fff", fontSize: 11, fontWeight: 700 }}>GD</span>}
        </div>
        <div className="ev-row-meta">
          <LeaguePill league={b.league} />
          <span className="ev-prop">{b.prop}</span>
          <span className={"ev-side " + (b.side === "OVER" ? "is-over" : "is-under")}>{b.side}</span>
          <span className="ev-line">{b.line}</span>
          <span className="ev-meta-dot">·</span>
          <span className="ev-time">{fmtGameTime(b.startTime)}</span>
        </div>
        <div className="ev-books">
          {b.books.map(([bk, od], j) => <BookBadge key={j} book={bk} odds={od} />)}
        </div>
      </div>
      <div className="ev-row-side">
        <TruePct value={b.truePct} />
        <span className="ev-add">
          <span className={"ev-add-btn " + (isSel ? "is-sel" : "")}>{isSel ? "✓" : "+"}</span>
        </span>
      </div>
```

Note the changes and why:
- `borderRadius: 6` → `borderRadius: "var(--r-sm)"`. It must be a **string** — React appends `px` to numbers. There is no `.ev-gd` CSS rule, so this inline value is the only source and no test can see it.
- The meta line order is `LEAGUE · Prop · SIDE line · game time` per the spec, so `.ev-side` now precedes `.ev-line` (reads "OVER 25.5").
- `.ev-meta-dot` is reused as the separator before the game time. It is on `TEXT4_ALLOWED`, so it must stay a bare `·` glyph and keep `--text-4`.
- Move the two JSX comments that explain the absent LOGGED badge and the `title=` escape hatch along with the markup — do not drop that reasoning.

- [ ] **Step 3: Verify nothing else in the file references the old structure**

Run: `grep -n "ev-player\|ev-books\|ev-time\|ev-add" web/static/ev-page.jsx`
Confirm every hit is either the row you just wrote or the slip builder (which has its own markup).

- [ ] **Step 4: Rebuild**

Run: `./build.sh`
Expected: `ev-page.js` rebuilt, a new cache-bust id stamped.

- [ ] **Step 5: Run the tests**

Run: `python -m pytest tests/ -q`
Expected: **397 passed**. `test_build_stamp.py` proves you rebuilt.

- [ ] **Step 6: Look at it**

Run `python main.py` with stubbed Supabase env plus `DISABLE_PERSISTENCE=true DISABLE_AUTO_BACKTEST=true`. Sign in, open +EV Bets, and confirm at **1440px and 375px**:
- All nine original fields are present: player, GD badge (if any), league, prop, side, line, game time, book chips, add button, true%.
- **No horizontal scroll at 375px** (that is the UX win; `min-width:620px` is removed in Task 5, so scroll may persist until then — note it and re-check after Task 5).
- The true% still shows its **green ramp varying with the value** (54-75%). It is an inline `oklch()`; if every row is the same green, you broke it.

- [ ] **Step 7: Commit**

```bash
git add web/static/ev-page.jsx web/static/dist web/static/index.html web/static/sw.js
git commit -m "feat(ev): airy two-line row markup

Nine grid columns become an identity line, a meta line and wrapping book
chips on the left, with the true% and add button as a fixed right zone. Every
handler, the key expression and the class string are unchanged; the GD badge's
inline borderRadius moves onto --r-sm (it has no CSS rule, so the inline value
is the only source)."
```

---

## Task 4: Hero true%, book chips, and the add button

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Scope the hero true% to the row**

`.cp-tp` (~:335) is the **only** rule for that class and it is shared with the slip-builder legs. Do **not** edit it. Add a scoped rule instead:

```css
/* 20px only in the +EV row. .cp-tp's base 14px also styles the slip-builder
 * legs (.ev-leg-pct -> <TruePct>), which stay at 14px. The color is an inline
 * oklch() ramp from TruePct in components.jsx and is deliberately untouchable
 * from CSS — see the note there. */
.ev-row .cp-tp{font-size:20px}
```

- [ ] **Step 2: Flip the book chips to a wrapping row**

Current (~:895-898):
```css
.ev-books{display:flex;flex-direction:column;gap:3px;align-items:flex-start}
.ev-books .cp-odd{margin-right:0}
.ev-books .cp-odd-n{min-width:44px;text-align:right}
```

The `min-width:44px;text-align:right` existed only to align a stacked column — in a wrapping row it makes the chips gap raggedly. Replace with:

```css
.ev-books{display:flex;flex-direction:row;flex-wrap:wrap;gap:6px;align-items:center}
.ev-books .cp-odd{margin-right:0}
/* 11px mono, scoped: .cp-book's 9.5px base is shared with the slip builder. */
.ev-row .cp-book{font-size:11px;font-family:"JetBrains Mono",ui-monospace,monospace}
```

- [ ] **Step 3: Grow the add button**

Current (~:900): 24px at `--r-sm`. Change to 30px at `--r-md`, keeping everything else (it is a `<span>` with no `onClick`; the row's click is the only trigger):

```css
.ev-add-btn{width:30px;height:30px;border-radius:var(--r-md);background:#0a0a12;border:1px solid var(--hair-2);display:grid;place-items:center;color:var(--text-3);font-size:15px;transition:.15s}
```

- [ ] **Step 4: Run the tests**

Run: `python -m pytest tests/ -q`
Expected: **397 passed**. Any new radius that is not a `var(--r-*)` token fails `test_every_radius_goes_through_a_token`.

- [ ] **Step 5: Verify in the browser**

Confirm the chips wrap rather than truncate, the true% reads as the hero number, and the 30px button is comfortably tappable at 375px.

- [ ] **Step 6: Commit**

```bash
git add web/static/index.html
git commit -m "feat(ev): hero true%, wrapping book chips, bigger add button"
```

---

## Task 5: Flatten the logged/selected states and delete the mobile overrides

**Files:**
- Modify: `tests/api_tests/test_css_tokens.py`
- Modify: `web/static/index.html`

- [ ] **Step 1: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
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
        ".ev-row-data.is-logged",
        ".ev-row-data.is-logged:hover",
        ".ev-row-data.is-logged.is-sel",
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **2 failed** — three gradient state tints plus the legend swatch, and the `min-width:620px`.

- [ ] **Step 3: Flatten the base state rules**

Replace the three logged rules (~:864-885). **Keep every `!important`** — `app-main.jsx`'s injected `.app:not(.tint-on) .ev-row-data:hover` is (0,4,0) and beats these; dropping it means logged rows lose their tint on hover. Use the values the mobile block already ships, so both viewports finally agree:

```css
/* Flat, and the same values the mobile block used to override these with — the
 * row read two different ways by viewport before. !important is load-bearing
 * against app-main.jsx's injected hover rule at (0,4,0); do not drop it. */
.ev-row-data.is-logged{
  background:rgba(239,68,68,.16) !important;
  border-left:3px solid var(--red);
  padding-left:calc(var(--row-px) - 3px);
}
.ev-row-data.is-logged:hover{background:rgba(239,68,68,.22) !important}
.ev-row-data.is-logged.is-sel{
  background:rgba(239,68,68,.26) !important;
  border-left:3px solid var(--red);
}
```

- [ ] **Step 4: Match the legend swatch**

The swatch (~:787-794) is documented as a miniature of the logged row. Change its `linear-gradient(90deg,rgba(239,68,68,.30),rgba(239,68,68,.10))` to `background:rgba(239,68,68,.16)`, and update its comment from "same gradient" to "same flat tint". **Keep `border-radius:3px`** — `ev-legend-swatch` is in `RADIUS_EXEMPT`.

- [ ] **Step 5: Delete the mobile overrides**

Inside `@media (max-width:900px)`, delete:
- `.ev-row{min-width:620px}` — the only cause of horizontal scroll
- the three `.ev-row-data.is-logged*` flat-tint overrides
- the explanatory comment that goes with them

**Keep** `.ev-table{overflow-x:auto;-webkit-overflow-scrolling:touch}` as a safety net, and add a short comment saying the 620px minimum is gone and this is belt-and-braces.

- [ ] **Step 6: Promote the name un-truncation**

Move the `@media (max-width:560px)` `.ev-player-n{white-space:normal;overflow:visible;text-overflow:clip;line-height:1.3;overflow-wrap:anywhere}` rule out of the media query so it applies at all widths — the identity line now owns the full row, so truncating is unnecessary. Keep `title={b.player}` in the JSX regardless.

- [ ] **Step 7: Run the tests**

Run: `python -m pytest tests/ -q`
Expected: **399 passed** (397 + 2 new).

- [ ] **Step 8: Verify at 375px**

Confirm: **no horizontal scroll**, logged rows read the same as they did, and the legend swatch matches the row it explains.

- [ ] **Step 9: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "fix(ev): flat logged tints, and no more 620px mobile scroll

The logged row kept a desktop gradient while mobile already overrode it flat,
so it read two ways by viewport. The mobile values are promoted to the base
rules and the overrides deleted, along with min-width:620px — the airy row
stacks, so nothing needs the horizontal scroll those tints were compensating
for. Legend swatch follows the row it mirrors."
```

---

## Task 6: Keep the selection visible on hover

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Reproduce the bug**

In the browser, select a row (click it) and then hover it. The blue tint is replaced by a neutral white wash: `app-main.jsx`'s injected `.app:not(.tint-on) .ev-row-data:hover{background:rgba(255,255,255,.025)}` is (0,4,0) and there is no `.is-sel:hover` rule at all. Confirm you see it before fixing it.

- [ ] **Step 2: Add the rule**

Next to the `.is-sel` rule, add:

```css
/* Selected rows keep their tint under the cursor. Without this, app-main.jsx's
 * injected hover rule (0,4,0) replaces --primary-lo with a neutral wash and the
 * only remaining selection cue is the 3px bar. !important for the same reason
 * .is-logged carries it. */
.ev-row-data.is-sel:hover{background:var(--primary-lo) !important}
```

- [ ] **Step 3: Verify**

Hover a plain row (neutral wash — unchanged), a selected row (stays blue), a logged row (stays red), and a logged+selected row (stays deep red). All four must be distinguishable.

- [ ] **Step 4: Run the tests + commit**

Run: `python -m pytest tests/ -q` → **399 passed**.

```bash
git add web/static/index.html
git commit -m "fix(ev): a selected row keeps its tint on hover"
```

---

## Task 7: The injected stylesheet

**Files:**
- Modify: `web/static/app-main.jsx`

- [ ] **Step 1: Read the current block**

At `app-main.jsx:255-262`, appended to `<head>` **last**, so it wins every equal-specificity tie:

```js
const styleEl = document.createElement("style");
styleEl.textContent = `
.density-compact .ev-row{padding-top:9px;padding-bottom:9px;font-size:13px}
.density-compact .ev-row-data{font-size:12.5px}
.app:not(.tint-on) .ev-row-data:hover{background:rgba(255,255,255,.025)}
`;
document.head.appendChild(styleEl);
```

- [ ] **Step 2: Put compact density on the scale**

Change `9px` to `var(--s-3)` (custom properties resolve fine here). **Keep the `padding-top`/`padding-bottom` longhands** — switching to the `padding:` shorthand would drop the row's horizontal `var(--row-px)`.

```js
styleEl.textContent = `
.density-compact .ev-row{padding-top:var(--s-3);padding-bottom:var(--s-3);font-size:13px}
.density-compact .ev-row-data{font-size:12.5px}
.app:not(.tint-on) .ev-row-data:hover{background:rgba(255,255,255,.025)}
`;
```

Add a comment above the template literal recording that these selectors are coupled to `ev-page.jsx`'s class names and to `index.html`'s `.ev-row` padding, that the longhands are deliberate, and that this sheet wins ties because it is appended last.

- [ ] **Step 3: Rebuild — this is the one that matters**

Run: `./build.sh`

`test_css_guards.py::test_accent_agrees_in_all_three_copies` reads `TWEAK_DEFAULTS.accent` from `app-main.jsx` **and** `dist/app-main.js`, so this file's bundle is checked. Skipping the rebuild fails CI's `bundles` job.

- [ ] **Step 4: Verify compact density still differs from regular**

The tweaks panel only mounts after a host `postMessage`, so verify in the console instead:

```js
document.querySelector(".app").classList.add("density-compact")
```
Then confirm rows get visibly tighter (12px vertical vs 16px) and their horizontal padding is unchanged. Remove the class afterwards.

- [ ] **Step 5: Run the tests + commit**

Run: `python -m pytest tests/ -q` → **399 passed**.

```bash
git add web/static/app-main.jsx web/static/dist web/static/index.html web/static/sw.js
git commit -m "refactor(ev): compact density on the spacing scale

The injected sheet's 9px becomes --s-3, keeping the padding longhands so the
row's horizontal --row-px survives. This sheet is appended to <head> last, so
it wins equal-specificity ties against index.html — noted in the file."
```

---

## Task 8: The empty state

**Files:**
- Modify: `web/static/ev-page.jsx`
- Modify: `web/static/index.html`

- [ ] **Step 1: Replace the inline style with a class**

The empty row (~:717-721) is `<div className="ev-row" style={{display:"block",textAlign:"center",padding:"40px 18px",color:"var(--text-3)",fontSize:13}}>`. Its `display:block` was fighting the grid, which no longer exists, and its padding is off-scale. Change the markup to:

```jsx
<div className="ev-empty-row">
```

Keep both copy strings byte-identical (`"No green devils available right now."` / `"No bets match your filters."`).

- [ ] **Step 2: Add the rule**

```css
.ev-empty-row{padding:var(--s-10) var(--row-px);text-align:center;color:var(--text-3);font-size:13px}
```

- [ ] **Step 3: Verify both empty states**

Filter to something with no matches (e.g. a prop-search string that matches nothing) and confirm the message is centered and readable. Then toggle Green Devils on with no goblins available to see the other string.

- [ ] **Step 4: Rebuild, test, commit**

```bash
./build.sh && python -m pytest tests/ -q
git add web/static/ev-page.jsx web/static/index.html web/static/dist web/static/sw.js
git commit -m "refactor(ev): give the empty row a class instead of five inline styles"
```

---

## Task 9: Slip rail leg chips

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Move the leg chips onto `--r-md`**

`.ev-leg` (~:959) and `.ev-leg-i` (~:960) are `--r-sm`; the spec puts the slip's inner tiles at `--r-md`.

**Do not touch `.ev-leg-i`'s `color:var(--primary-2)`.** It sits on `background:var(--primary-hi)`, and that explicit color is exactly what makes it pass `test_primary_hi_never_backs_inherited_text`. Removing it fails the suite and reintroduces a contrast bug.

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/ -q` → **399 passed**.

- [ ] **Step 3: Verify**

Add two legs to a slip and confirm the numbered chips and leg rows look right in both the desktop rail and the mobile drawer (`slipBody` renders twice — check both).

- [ ] **Step 4: Commit**

```bash
git add web/static/index.html
git commit -m "refactor(ev): slip leg chips on --r-md"
```

---

## Task 10: Flatten the state-button fills, with the specificity fix

The trap that bit Phase 1: flattening these to a solid `background` without raising specificity turns a just-saved **green** button accent-**blue** on hover.

**Files:**
- Modify: `web/static/index.html`

- [ ] **Step 1: Understand the cascade first**

`.ev-prefs-save:hover:not(:disabled){background-color:#195F97}` is (0,3,0). The state fills (`.ev-prefs-save-saved`, `-error`) are single-class (0,1,0) and survive today only because they set a `background-image` (a gradient), which a `background-color` cannot override. Flatten them to a solid colour and the hover wins.

- [ ] **Step 2: Flatten and raise specificity in the same edit**

Double the class on each state rule so it beats the hover:

```css
.ev-prefs-save.ev-prefs-save-saved{background:#16a34a}
.ev-prefs-save.ev-prefs-save-error{background:#dc2626}
```

Do the same for the `.bt-slip-place` states (`-queued`, `-error`, `-sending`). Leave `-install` alone — Phase 2b handles it, and note that its `:hover` currently *lightens* against the hover-darkens rule.

- [ ] **Step 3: Prove the trap is closed**

In the browser, trigger the saved state (change a preference and save) and **hover the green button**. It must stay green. Then trigger the error state and hover — must stay red. Sample the computed background if you are unsure.

- [ ] **Step 4: Run the tests + commit**

Run: `python -m pytest tests/ -q` → **399 passed**.

```bash
git add web/static/index.html
git commit -m "refactor(ev): flat state-button fills, with the specificity raised

The state fills only survived hover because they were background-images. Going
flat means the (0,3,0) hover rule would win, so each state rule doubles its
class — verified by hovering a saved button and watching it stay green."
```

---

## Task 11: Phase 2a verification

**Files:** none modified.

- [ ] **Step 1: Full suite**

Run: `python -m pytest tests/ -q`
Expected: **399 passed**, no failures, no skips.

- [ ] **Step 2: Prove the bundles are fresh**

Run:
```bash
./build.sh && git diff --exit-code web/static/dist web/static/index.html web/static/sw.js
```
Expected: **no diff, exit 0.** A diff means a `.jsx` edit shipped without its rebuild — the exact failure the `bundles` CI job exists to catch.

- [ ] **Step 3: Confirm the guards still hold**

Run: `python -m pytest tests/api_tests/test_css_guards.py tests/api_tests/test_css_tokens.py tests/api_tests/test_build_stamp.py -q`
Expected: all pass. These pin: no accent gradients, no blur orbs, no gradient-clipped text, the three-copy accent, the extension mirror, `--primary-hi` never behind inherited text, every radius on a token, flat card surfaces, no white inset, no accent glow, `--row-px` coupling, `--text-4` restricted, and the new flat-state/no-scroll invariants.

- [ ] **Step 4: The behavior checklist — verify each in the browser, signed in**

- [ ] Clicking a row adds it to the slip; clicking again removes it. The `+` becomes `✓`.
- [ ] The slip builder shows the legs with their true% and break-even, in **both** the desktop rail and the mobile drawer.
- [ ] Sort toggle still reverses the order; "unlogged first" still groups logged rows last.
- [ ] Every filter works: league chips, prop search, Min True % stepper (clamped 50-80), side chips, Green Devils chip. `Clear` resets five of them and deliberately **not** Green Devils.
- [ ] The meta bar shows count, freshness with its live pulse, the legend when relevant, and the "N of M" count.
- [ ] Logged rows render red from all three sources: signed-in server flag, the 30s key poll, and the optimistic mark right after saving a slip.
- [ ] Both auto-log checkboxes persist across a reload.
- [ ] Min Leg % still gates saving a slip.
- [ ] Both empty-state strings appear under the right conditions.

- [ ] **Step 5: Visual review at 375px and 1440px**

Establish a noise floor first with an after-vs-after control — the landing page has two stable render states and a naive diff shows a phantom ~5% change. A deterministic `/tmp` harness that inlines the real `<style>` block was byte-identical across runs in Phase 1 and is the only way to see `.ev-*` rules signed out.

Mandatory captures:
- The row in plain, selected, logged, and logged+selected states, **each hovered** — four distinguishable looks.
- **No horizontal scroll at 375px.**
- The true% column showing the green ramp varying with value.
- The filter bar with every control on the 34px baseline.
- The book chips wrapping rather than truncating.

- [ ] **Step 6: Confirm nothing outside scope moved**

Run: `git diff --stat main..HEAD`
Expected: only `web/static/index.html`, `web/static/ev-page.jsx`, `web/static/app-main.jsx`, `web/static/sw.js`, `web/static/dist/*`, `tests/api_tests/test_css_tokens.py`, and this plan. No `components.jsx`, no `page-*.jsx`, no `lp-game-` rule.

---

## Definition of done (Phase 2a)

- `pytest tests/ -q` → 399 passed.
- `./build.sh` produces no diff — bundles and stamps committed together.
- The +EV row is the airy two-zone layout, all nine original fields present.
- No horizontal scroll at 375px; `min-width:620px` gone.
- Logged tints flat and identical across viewports; the legend swatch matches.
- Plain / selected / logged / logged+selected are distinguishable **including on hover**.
- The true% green ramp still varies with value; `TruePct` untouched.
- Compact density still tighter than regular, with horizontal padding intact.
- State buttons stay their own colour on hover.

**Next:** Phase 2b — Boards table, Backtest cards, Analytics panels and charts, account menu, `.an-panel .bt-card` tone, the shared state classes, and `--red-2`.

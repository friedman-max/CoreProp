# Frontend Modernization — Phase 2b (Boards, Backtest, Analytics, account menu) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the web half of the modernization on the three remaining screens — Boards, Backtest, Analytics — plus the account menu, and resolve the carry-overs Phase 1 and 2a deferred. Still purely visual: no handler, API call, or state logic changes.

**Architecture:** Same contract as 2a. Editing a `.jsx` requires `./build.sh` and committing `dist/` + `index.html` + `sw.js`, enforced by CI's `bundles` job. All CSS is one `<style>` block in `web/static/index.html`. **Most rules this phase touches are declared twice** — a base rule and a later "density overrides" copy that wins — so a base-only edit is a silent no-op.

**Tech Stack:** React 18 (plain global scripts, no module system), esbuild via `./build.sh`, pytest, GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` (Sections 2, 3 "Boards"/"Backtest"/"Analytics"/"Account menu"; Phasing item 2).
**Predecessors:** Phase 1 (merged) and Phase 2a (branch `feat/frontend-modernization-phase-2a`, verified — this plan stacks on it).

> **Line numbers:** Phases 1 and 2a rewrote large parts of `index.html`, so any line number below is indicative only. **Grep for the selector, and grep for a SECOND declaration of it before editing.**

---

## Scope

**In scope:** `page-boards.jsx`, `page-backtest.jsx`, `page-analytics.jsx`, the `.bd-*` / `.bt-*` / `.an-*` / `.pnl-*` / `.cp-menu*` CSS, and the shared state classes.

**Out of scope:** `landing.jsx` / `pricing.jsx` (Phase 3), iOS (Phase 4), `tweaks-panel.jsx`, `analytics-preview.html`, `auth-page.jsx`, any `lp-game-` selector.

**`components.jsx` is in scope for exactly one item (Task 9)** and nothing else.

---

## Decisions this plan makes

1. **Boards stays a table**, and **keeps its all-caps headers.** Phase 1 deliberately retained uppercase on true table headers; reversing that here would undo a recorded decision. Only the tracking was already reduced to `.04em`.
2. **Boards' right cluster switches to `align-self:flex-end`** to match `.ev-clear`, closing the documented 9.5px offset against the bottom-aligned filter fields.
3. **Pending unifies on blue** (`#60A5FA` bar / `#93C5FD` badge). Two of the three sites are already blue; only the compact card background is amber, and amber implies caution where pending means "not settled yet."
4. **One left bar on Backtest cards, not two.** Keep the 3px `::before`, flattened to a solid colour; drop the 4px `inset` box-shadow and the blurred glow half. Cards take `--r-lg` on **both** copies.
5. **`.an-panel .bt-card{background:var(--card-2)}`** — a descendant rule, because changing either base background fails `test_neutral_card_surfaces_are_flat`. This resolves the Phase 1 carry-over where nested stat tiles sat same-tone-on-same-tone.
6. **Shared state classes** `.cp-state` / `.cp-state-err` replace the seven near-identical inline loading/error/empty divs, and `--red-2: #FCA5A5` becomes a token (that literal appears in ~10 CSS rules and 3 inline styles).
7. **Analytics grid lines use `var(--hair)`** on the SVG presentation attribute; the duplicated chart pads collapse to one shared const.
8. **The account menu's destructive red hover is scoped to the sign-out item.** It currently paints `PushToggle` — a non-destructive control — red on hover.
9. **`TruePct`'s ramp domain is widened** to match what the page actually shows. This is the one `components.jsx` edit, and it is a *fix to* a value→color encoding rather than a removal of one — see Task 9 for why that is in scope.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `web/static/index.html` | Modify | All `.bd-*` / `.bt-*` / `.an-*` / `.cp-menu*` CSS, `--red-2`, `.cp-state*` |
| `web/static/page-boards.jsx` | Modify | Replace the one inline style; no structural change |
| `web/static/page-backtest.jsx` | Modify | Inline state divs → classes |
| `web/static/page-analytics.jsx` | Modify | Grid-line colour, shared pads, inline state divs |
| `web/static/components.jsx` | Modify | **Task 9 only** — `TruePct`'s ramp domain |
| `tests/api_tests/test_css_tokens.py` | Modify | `--red-2` token, the nested-tile rule, no-inline-state invariant |

---

## Task 1: `--red-2` and the shared state classes

Seven near-identical inline divs render loading/error/empty states across two screens, and `#FCA5A5` is repeated in ~10 CSS rules with no token.

**Files:** `tests/api_tests/test_css_tokens.py`, `web/static/index.html`, `web/static/page-backtest.jsx`, `web/static/page-analytics.jsx`

- [ ] **Step 1: Baseline**

Run: `python -m pytest tests/ -q` → **399 passed**. If not, stop.

- [ ] **Step 2: Inventory before editing**

Run:
```bash
grep -n "FCA5A5" web/static/index.html web/static/*.jsx
grep -nE 'style=\{\{[^}]*(padding|color)' web/static/page-backtest.jsx web/static/page-analytics.jsx
```
Paste both lists in your report. The first is the `--red-2` worklist; the second is the state-div worklist. If either differs materially from "≈10 CSS rules and ≈7 inline divs", say so before proceeding.

- [ ] **Step 3: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
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
```

- [ ] **Step 4: Run to verify it fails**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **1 failed** — `--red-2` is not defined, and the hardcoded sites are listed.

- [ ] **Step 5: Add the token and migrate the CSS sites**

In `:root`, next to `--red`:

```css
  /* The lighter error/miss red, for TEXT on a red tint. --red (#EF4444) is the
   * semantic red for bars, borders and fills; this one is what reads on top of
   * one (8.5-10.7:1 over the logged-row tints, where --text-3 measures 3.6-4.6
   * and fails AA). Not interchangeable — see the .is-logged note. */
  --red-2: #FCA5A5;
```

Then replace every `#FCA5A5` outside `:root` with `var(--red-2)`. Work from your Step 2 list.

- [ ] **Step 6: Add the state classes**

```css
/* The seven loading / error / empty blocks across Backtest and Analytics were
 * near-identical inline style objects. One pair of classes instead, so they can
 * never drift apart again. */
.cp-state{padding:var(--s-8) var(--s-5);text-align:center;color:var(--text-3);font-size:13px}
.cp-state-err{padding:var(--s-8) var(--s-5);text-align:center;color:var(--red-2);font-size:13px}
```

- [ ] **Step 7: Migrate the inline divs**

In `page-backtest.jsx` and `page-analytics.jsx`, replace each inline loading/error/empty `style={{…}}` with `className="cp-state"` or `className="cp-state-err"`.

**Keep every copy string byte-identical** — `tests/api_tests/test_landing_claims.py` does not cover these, but changing user-facing copy is out of scope for a visual phase.

**One exception:** the `.pnl-chart` div in `page-analytics.jsx` carries `style={{minHeight:160}}` and a `ref`. Leave both — the `ref` feeds a ResizeObserver and the `minHeight` stops the chart collapsing before first measure. It is not a state div.

- [ ] **Step 8: Rebuild, test, commit**

```bash
./build.sh && python -m pytest tests/ -q
```
Expected: **400 passed** (399 + 1 new).

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html web/static/page-backtest.jsx web/static/page-analytics.jsx web/static/dist web/static/sw.js
git commit -m "refactor(css): tokenize the error red and share the state blocks"
```

---

## Task 2: The Boards table

**Files:** `web/static/index.html`, `web/static/page-boards.jsx`

- [ ] **Step 1: Find every copy of what you are about to change**

Run: `grep -n "bd-tbl-wrap\|bd-tbl thead th\|bd-tbl tbody td\|bd-clear\|bd-badge\|bd-pag\|bd-empty" web/static/index.html`

Note which selectors appear twice. The later copy wins.

- [ ] **Step 2: The wrapper and header**

- `.bd-tbl-wrap` → `--r-lg` (it is a card-class surface; Phase 1 left it at `--r-md`).
- **Keep** `text-transform:uppercase` and `letter-spacing:.04em` on `.bd-tbl thead th` — Phase 1 deliberately retained caps on true table headers. Do not "modernize" this away.
- The sticky header's fill and `.bd-tbl-wrap`'s must stay equal or the header bands on scroll. Both are `var(--card)` and `test_neutral_card_surfaces_are_flat` pins the pair — do not break it.

- [ ] **Step 3: Neutral row hover**

`.bd-tbl tbody tr:hover` is a faint accent blue (`rgba(30,111,176,.04)`). Boards is a comparison table, not a selectable list — an accent hover implies selection. Change it to a neutral `rgba(255,255,255,.025)`, matching the +EV row's neutral hover.

- [ ] **Step 4: Fix the filter-bar alignment**

`.bd-clear`, `.bd-badge` and `.bd-pag` use `align-self:center` while the `.bd-f` fields bottom-align, leaving the right cluster **9.5px above** the fields' baseline. Change all three to `align-self:flex-end` to match `.ev-clear`.

**Do not change any of their heights.** The 34px filter-bar contract is load-bearing and three shipped bugs are recorded in comments around those rules. `.bd-pag-btn` stays 28px inside the 34px `.bd-pag`.

- [ ] **Step 5: The empty row**

`.bd-empty{padding:40px 14px}` is off-scale → `var(--s-10) var(--s-4)`.

`page-boards.jsx`'s `BoardEmptyRow` has one inline style: `style={{ marginLeft: 10 }}` on its Clear button. Replace with a class or `var(--s-3)`.

**Do not touch the `cols` props.** `BoardEmptyRow` is called with hardcoded `12`, `6` and `9` matching the three tables' column counts; a mismatch collapses the empty/loading/error row.

- [ ] **Step 6: Verify the sort affordance still works**

`page-boards.jsx` builds `"sortable " + (sort.col === col ? "is-active is-" + sort.dir : "")`. There is no `.is-desc` rule — descending is the default arrow. Click a few headers on all three boards and confirm the arrow and the active colour still appear.

- [ ] **Step 7: Rebuild, test, commit**

```bash
./build.sh && python -m pytest tests/ -q
```
Expected: **400 passed**.

```bash
git add web/static/index.html web/static/page-boards.jsx web/static/dist web/static/sw.js
git commit -m "refactor(boards): roomier table, neutral hover, aligned filter bar"
```

---

## Task 3: Backtest slip cards

**Files:** `web/static/index.html`

- [ ] **Step 1: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
def test_backtest_cards_have_one_left_bar_and_flat_fills():
    """Each outcome card carried TWO left bars — a 3px ::before gradient and a
    4px `inset 4px 0 0` box-shadow — plus a gradient background and a blurred
    glow. One 3px bar, flat fill, no glow.
    """
    css = style_block(INDEX)
    violations = []
    for selector, decls in rules(css):
        s = selector.strip()
        if not s.startswith(".bt-slip-compact.is-"):
            continue
        body = squash(decls)
        if "inset4px00" in body:
            violations.append(f"{s} still has the 4px inset bar")
        if "linear-gradient" in body:
            violations.append(f"{s} still has a gradient fill")
    assert not violations, "\n  ".join(violations)
```

- [ ] **Step 2: Run to verify it fails**

Expected: **1 failed**, listing the four `.bt-slip-compact.is-*` rules.

- [ ] **Step 3: Flatten the four outcome rules**

For each of `.bt-slip-compact.is-win`, `.is-loss`, `.is-push`, `.is-pending`:
- Replace the `linear-gradient(180deg,…)` background with its **top stop** as a flat colour (the convention Phase 2a used for the state buttons).
- Delete the `inset 4px 0 0 <colour>` from the `box-shadow`, and delete the blurred `0 6px 20px -10px` glow half. If that empties the `box-shadow`, delete the declaration rather than leaving `box-shadow:;`.
- Keep the border tint.

The 3px `.bt-slip::before` bar stays and is the single left bar.

- [ ] **Step 4: Unify pending on blue**

`.bt-slip-compact.is-pending` uses amber (`rgba(251,191,36,…)`) while `.bt-slip.is-pending::before` is `#60A5FA` and `.bt-slip-badge.is-pending` is `#93C5FD`. Move the card's tint to the blue family so all three agree. Pending means "not settled yet", not "warning".

- [ ] **Step 5: Card radius**

`.bt-slip` is `--r-lg` but the compact copy that actually renders is `--r-md`. Put **both** on `--r-lg` — these are cards.

- [ ] **Step 6: Roomier leg rows**

Edit the **later** copy of each of `.bt-slip-leg`, `.bt-leg-name`, `.bt-leg-prop`, `.bt-leg-pct`, `.bt-leg-actual` (grep for two declarations of each — the base copies do not render).

Three things must survive:
- **`min-width:50px` on `.bt-leg-pct` and `min-width:71px` on `.bt-leg-actual`.** A comment records the 9-18px ragged-column bug these fix. Do not drop them and do not convert the grid to flex.
- **`.bt-slip-compact`'s `grid-template-columns` override** (`1fr auto auto`). The base rule has a vestigial `18px` track for an index square the JSX never renders; removing the override brings back a phantom empty column.
- `.bt-leg-pct`'s hardcoded `rgba(30,111,176,.10)` / `.22` are `--primary-lo` / `--primary-hi` by value — tokenize them, and if you use `--primary-hi` as a background confirm the rule also sets an explicit light `color`, or `test_primary_hi_never_backs_inherited_text` fails.

- [ ] **Step 7: Nested stat tiles**

Add the descendant rule Phase 1 deferred:

```css
/* Analytics nests .bt-card inside .an-panel and both are var(--card), so the
 * tiles were held apart by a single 6%-alpha hairline once flattening removed
 * their top-edge lift. A descendant rule, NOT a change to either base
 * background — test_neutral_card_surfaces_are_flat pins both to var(--card) by
 * exact selector. */
.an-panel .bt-card{background:var(--card-2)}
```

- [ ] **Step 8: Rebuild, test, commit**

Expected: **401 passed** (400 + 1 new).

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "refactor(backtest): one left bar, flat outcome fills, blue pending"
```

---

## Task 4: Analytics panel and charts

**Files:** `web/static/index.html`, `web/static/page-analytics.jsx`

- [ ] **Step 1: Panel spacing**

`.an-panel`'s `padding:18px 20px` is off-scale → `var(--s-5) var(--s-5)`. `.an-section-h`'s `margin:20px 0 10px` → `var(--s-5) 0 var(--s-3)`. Keep `.an-section-h`'s uppercase — it is a true section header (Phase 1's rule).

- [ ] **Step 2: Tokenize the hardcoded well colours**

`#0a0a12` (×2), `#1f1f2e` and `#14141e` appear as literals in the Analytics block. `#14141e` is exactly `--card`; `#0a0a12` is the `--controlBg`-style recessed well and `#1f1f2e` is the active-segment fill. Replace what has an exact token; leave the rest with a comment saying no token matches. **Do not invent tokens** beyond `--red-2` from Task 1.

- [ ] **Step 3: The chart grid lines**

Three SVG `<line>` elements hardcode `stroke="rgba(255,255,255,.06)"`, which is exactly `--hair`. Change all three to `stroke="var(--hair)"`. SVG presentation attributes resolve `var()` in-document.

Verify visually that the grid lines still render — if a browser ever failed to resolve it the lines would vanish silently.

- [ ] **Step 4: Collapse the duplicated chart pads**

`const padL = 56, padR = 28, padT = 18, padB = 34;` is declared **twice** — once in `PnLChart`, once in `ReliabilityChart`. Hoist to one module-scope const and reference it from both.

**Do not change the values.** `padL = 56` is sized for the widest right-anchored Y-axis label at `x={padL-10}`; shrinking it clips the labels. The tick text sits at `y={H-10}` and `y={H-26}`, so changing `padB` without moving those detaches the X labels.

- [ ] **Step 5: Do NOT add `pointer-events:none` to `.pnl-chart`**

A comment in `index.html` warns about this: `.pnl-chart` is shared by both charts, and `ReliabilityChart`'s `<circle>` elements carry `<title>` tooltips that die if the SVG stops hit-testing. Phase 1 already hit this. If you find yourself wanting it, stop.

- [ ] **Step 6: Leave the payout constants alone**

`page-analytics.jsx`'s `AN_LEG_BE_PCT` and `page-backtest.jsx`'s payout tables are mirrored in five places and pinned by `test_payout_table_mirror.py`. Do not touch them.

- [ ] **Step 7: Rebuild, test, commit**

Expected: **401 passed**.

```bash
git add web/static/index.html web/static/page-analytics.jsx web/static/dist web/static/sw.js
git commit -m "refactor(analytics): panel spacing on the scale, tokenized grid lines, one pad const"
```

---

## Task 5: The account menu

**Files:** `web/static/index.html`

- [ ] **Step 1: Use `--shadow-pop`**

`.cp-menu` hardcodes `box-shadow:0 12px 32px rgba(0,0,0,.5)`. `--shadow-pop` was defined in Phase 1 for exactly this and currently has **zero usages** — one of the three "defined but unused" carry-overs. Point `.cp-menu` at it.

Also fix its stale fallback: `background:var(--card,#13131c)` — `--card` is `#14141e`, so the fallback is a second, wrong source of truth. Drop the fallback.

- [ ] **Step 2: `.cp-menu-item` radius**

`--r-sm` → `--r-md` (inner tiles).

- [ ] **Step 3: Scope the destructive hover — this is a real bug**

`.cp-menu-item:hover` is a destructive red (`rgba(239,68,68,.12)` / `var(--red-2)` after Task 1). That class is **shared with `PushToggle`** (`components.jsx` renders it as a `.cp-menu-item`), so hovering the non-destructive "Slip alerts" toggle turns it red.

Scope the red to the sign-out item only, and give the base item a neutral hover:

```css
.cp-menu-item:hover{background:rgba(255,255,255,.04)}
.cp-menu-item.is-danger:hover{background:rgba(239,68,68,.12);color:var(--red-2)}
```

Then add `is-danger` to the sign-out item in `components.jsx`… **wait.** `components.jsx` is out of scope except for Task 9, and adding a class there means a rebuild. Two options — pick one and say which:
- (a) Add `is-danger` in `components.jsx` (a one-word change, rebuild required, and Task 9 rebuilds that file anyway), or
- (b) Target the sign-out item structurally, e.g. `.cp-menu-item:last-child:hover`, if it is reliably last.

Verify whichever you choose by hovering **both** the sign-out item and the Slip-alerts toggle.

- [ ] **Step 4: Test and commit**

CSS-only unless you chose (a). Expected: **401 passed**.

```bash
git commit -m "fix(nav): the destructive hover no longer paints the push toggle red"
```

---

## Task 6: The unused tokens

**Files:** `web/static/index.html`

- [ ] **Step 1: Check what is still unused**

Run: `for t in --shadow-pop --s-1 --radius-xs; do echo -n "$t: "; grep -c "var($t)" web/static/index.html; done`

Task 5 should have given `--shadow-pop` a consumer.

- [ ] **Step 2: Resolve each**

- `--shadow-pop` — used by Task 5. Done.
- `--s-1` (4px) — if genuinely unused, find the 4px gaps/paddings that should be on it (grep `gap:4px`, `padding:4px`) and migrate them; if none exist, delete the token rather than leave a scale hole nobody reads.
- `--radius-xs` — a Phase 1 alias for `--r-sm` with zero consumers. Delete it, and confirm `test_legacy_radius_aliases_point_at_the_new_scale` does not assert it. **If that test does assert it, amend the test in the same commit** and record why, exactly as Phase 2a did for `.ev-row-hd`.

- [ ] **Step 3: Test and commit**

Expected: **401 passed** (or one fewer assertion if you amended the alias test — say so).

---

## Task 7: Phase 2b verification

**Files:** none modified.

- [ ] **Step 1: Full suite** → **401 passed**, no failures, no skips.

- [ ] **Step 2: Bundles fresh**

```bash
./build.sh && git diff --exit-code web/static/dist web/static/index.html web/static/sw.js
```
Expected: no diff, exit 0.

- [ ] **Step 3: Guards**

Run: `python -m pytest tests/api_tests/test_css_guards.py tests/api_tests/test_css_tokens.py tests/api_tests/test_build_stamp.py -q` → all pass.

- [ ] **Step 4: Behavior checklist (browser)**

There is no `.env` on this box, so you cannot sign in through the UI. Use Phase 2a's method: serve a harness over HTTP that inlines the real `<style>` block and mounts the real page components from the committed `dist/*.js`, with data proxied from a running `python main.py` (stubbed Supabase + `DISABLE_PERSISTENCE=true DISABLE_AUTO_BACKTEST=true`). Hand-applied classes on a live React page get reconciled away — drive real interactions or use the harness.

- [ ] Boards: all three tabs render; sorting works on each and the active arrow shows; the 402/subscription branch and every `BoardEmptyRow` state string still render; best-odds pills still green.
- [ ] Backtest: all four outcome cards (win/loss/push/pending) render with **one** left bar each; the leg columns stay aligned across cards (that is what the two `min-width`s protect); delete-modal Escape/Enter still work; the place-slip CTA still opens PrizePicks.
- [ ] Analytics: both charts render with visible grid lines; the ReliabilityChart's dot `<title>` tooltips still appear on hover; the range buttons and the custom date pair work; the stat grid still shows its column counts at each breakpoint.
- [ ] Account menu: sign-out hover is red, **Slip alerts hover is not**; the menu's shadow reads as a popover.

- [ ] **Step 5: Visual review at 375px and 1440px**

Establish an after-vs-after noise floor first (Phase 2a's harness reached 0 bytes). Capture before/after for: the Boards filter bar (right cluster now baseline-aligned), a Boards table scrolled so the sticky header floats, all four Backtest outcome cards in one frame, the Analytics panel with its nested stat tiles (the `--card-2` change), and the account menu open.

- [ ] **Step 6: Scope check**

Run: `git diff --stat <phase-2a-tip>..HEAD` — expect only the files in the File Structure table. `landing.jsx`, `pricing.jsx` and any `lp-game-` rule must be untouched.

---

## Task 8 (optional, judgment): the true% ramp domain

Phase 2a's visual review measured this and it is the one substantive finding it could not fix in scope.

**The problem:** `TruePct` computes `t = (value - 54) / 18` clamped to 0-1, so the ramp spans 54→72%. But the default +EV view spans roughly **50→59%**, which maps to a colour range of **6/255** — every row from 50-54% is the identical clamped-low green, and Phase 2a promoted this near-static encoding to a 20px hero number.

**Why this is in scope for a visual phase:** the non-goal forbids *removing or recolouring* a value→color encoding. This does neither — it makes an existing encoding actually encode, over the range the page displays.

**Why it is optional:** it is the only `components.jsx` edit in the phase, it changes what users see per-row, and it is a judgment call about the right domain. If you are not confident, **skip it and report why** — that is an acceptable outcome.

- [ ] **Step 1: Measure the real distribution**

Start the server, fetch `/api/bootstrap/core`, and report the actual min/max/percentiles of `truePct` in both the default view and the green-devil view. Do not guess the domain.

- [ ] **Step 2: Pick a domain from the data**

Something like p5→p95 of the displayed distribution, so the ramp spends its range where the rows actually are. State your choice and the resulting colour spread in sRGB (Phase 2a measured the current spread at 6/255 — beat that substantially or the change is not worth making).

- [ ] **Step 3: Keep the hue and the shape**

Only the domain moves. The hue stays 145 (green) and the lightness/chroma expressions keep their form — this is a fix to the mapping, not a new colour system.

- [ ] **Step 4: Rebuild, test, verify, commit**

`components.jsx` feeds every screen, so run the full suite and check the slip-builder legs (which use the same component at 14px) as well as the +EV rows.

---

## Definition of done (Phase 2b)

- `pytest tests/ -q` → 401 passed (± any amended assertion, stated).
- `./build.sh` produces no diff.
- Boards: table kept, all-caps headers kept, neutral hover, right cluster baseline-aligned, sticky header still matching its wrapper.
- Backtest: one left bar per card, flat outcome fills, pending blue, cards `--r-lg`, leg columns still aligned.
- Analytics: panel spacing on the scale, grid lines tokenized and still visible, one pad const, tooltips alive.
- Account menu: `--shadow-pop` in use, destructive hover scoped to sign-out.
- `--red-2` tokenized with no hardcoded `#FCA5A5` outside `:root`; the seven inline state divs replaced.
- `.an-panel .bt-card` on `--card-2`.
- No unused tokens left undocumented.

**Next:** Phase 3 (landing + pricing) and Phase 4 (iOS) each get their own plan. Phase 4 inherits the Phase 1 note that `Theme.swift` still carries the old 14/10/8 radius triple.

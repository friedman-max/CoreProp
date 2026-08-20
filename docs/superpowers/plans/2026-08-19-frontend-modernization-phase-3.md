# Frontend Modernization — Phase 3 (landing + pricing) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the signed-out marketing surface on the spacing and type scales, fix a real narrow-viewport collision on the pricing card, and close the four token leftovers Phase 2b deferred. Purely visual: **no copy changes, no functional changes.**

**Architecture:** Phases 1-2b already ran the token/flattening pass over the shared CSS, so on these two pages **every radius is already tokenized, every card is already flat, and no gradients, orbs, glows or white insets remain.** What is left is spacing (~80 off-scale literals), the type scale on pricing (one `clamp()` vs landing's seven), and four small items. **~95% of this phase is CSS-only in `index.html`'s `<style>` block** and needs no rebuild; only Task 5 touches `.jsx`.

**Tech Stack:** React 18 (plain global scripts), esbuild via `./build.sh`, pytest, GitHub Actions.

**Source spec:** `docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` (Section 4 "Marketing"; Phasing item 3).
**Predecessors:** Phase 1, 2a, 2b — this stacks on `feat/frontend-modernization-phase-2b`.

> **Line numbers are indicative.** Grep for the selector, and **grep for a second declaration** before editing.

---

## Scope

**In scope:** `web/static/landing.jsx`, `web/static/pricing.jsx`, and the `.lp-*` / `.pp-*` CSS in `index.html`.

**Out of scope:** iOS (Phase 4), `tweaks-panel.jsx`, `analytics-preview.html`, `auth-page.jsx`, and all app-screen CSS (`.ev-*`, `.bd-*`, `.bt-*`, `.an-*`, `.pnl-*`, `.cp-*`) except where a rule is shared.

---

## Decisions this plan makes

1. **The 22px gutter becomes one `--lp-px` token** on `.lp`, mirroring how `--lp-gap` already works. It is repeated in six section rules and is the single largest off-scale item on the landing page.
2. **Off-scale spacing rounds to the nearest `--s-*` step, ties up** — except in the hero, where it rounds **down or stays** (see trap 5: the hero's vertical budget is measured so the minigame card clears the fold at 1280×800, and increasing hero spacing pushes it under).
3. **`.pp-card-hd` gets `gap` + `flex-wrap`**, and `.pp-card-trial` gets `white-space:nowrap`. The recon measured the collision extending to **390px** (iPhone-12 class), clearing by only ~6px at 414px — worse than Phase 1's "360 and 320" estimate.
4. **`.pp-card`'s padding and `.pp-price-d`'s 64px become fluid.** At 320px the card has 208px of content width and spends 64px of it on a single number; the padding spends another 64px. Both are the crowding.
5. **`.pp-faq-h` joins `.lp-h2`'s scale** so the two section headings on the signed-out surface match.
6. **`.lp-game-err` migrates to `var(--red-2)`** and its test exemption is deleted. `#FCA5A5` was never a PrizePicks-clone token — it is CoreProp's error red, this is the only `lp-game-*` rule using a site-palette colour, and the element renders *outside* the PP card as a `role="alert"` status line. Phase 2b's exemption comment explicitly asks Phase 3 to do this. **This is the one place the "never touch `lp-game-`" rule and the token rule disagree, and it is being resolved deliberately.**
7. **`#0a0a12` is NOT tokenized.** It has 14 sites across six screens and only two are in Phase 3's scope; tokenizing two of fourteen leaves the same colour spelled two ways, which is worse than today. The recon also found the real "sunken surface" family is **four** distinct hexes (`#0a0a12`, `#0c0c14`, `#0e0e16`, historically `#13131c`), making a single token a genuine design decision rather than cleanup. Deferred with a written reason.
8. **The `--radius` / `--radius-sm` aliases stay.** They have five live landing consumers and re-pointing them buys nothing; churning them would mean editing the alias test for no gain.
9. **`.pp-card-glow` stays.** The recon found `.pp-card{overflow:hidden}` clips its outer pixel, so it renders as a second inner hairline rather than the outer ring it looks like — possibly redundant, but deciding that is a visual judgement and removing it is a JSX change. Recorded as a follow-up.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `web/static/index.html` | Modify | All `.lp-*` / `.pp-*` spacing + type; `--lp-px`; `.pp-cta-sub.is-err` |
| `web/static/landing.jsx` | Modify | **Task 5 only** — one SVG `fill` literal → token |
| `web/static/pricing.jsx` | Modify | **Task 5 only** — replace one inline error colour with a class |
| `tests/api_tests/test_css_tokens.py` | Modify | New marketing-spacing invariant; delete `RED2_EXEMPT` |

---

## Task 1: The spacing invariant (the worklist generator)

Spacing has no test, so this phase would otherwise be unverifiable and immediately regressable. This test **is** the worklist: run it, get every off-scale site, fix them, re-run.

**Files:** `tests/api_tests/test_css_tokens.py`

- [ ] **Step 1: Baseline**

Run: `python -m pytest tests/ -q` → **402 passed**. If not, stop.

- [ ] **Step 2: Write the failing test**

Append to `tests/api_tests/test_css_tokens.py`:

```python
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
#   lp-vig-svg / lp-books-bar : SVG and animation geometry, not layout spacing.
MARKETING_SPACING_EXEMPT = (
    "lp-game",          # covers `.lp-game` and every `.lp-game-*`
    "lp-sk",
    "lp-hero",
    "lp-vig-svg",
    "lp-books-bar",
)

# Lengths that are legal without a token: zero, auto-centring, and the two
# derived layout vars the landing page defines on `.lp`.
_SPACING_OK = re.compile(
    r"^(0|auto|var\(--s-\d+\)|var\(--lp-(?:gap|px)\)|clamp\([^)]*\))$"
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
```

- [ ] **Step 3: Run to verify it fails, and capture the worklist**

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_marketing_spacing_goes_through_the_scale -q 2>&1 | tee /tmp/mkt-spacing.txt`
Expected: **FAIL** with roughly 70-85 declarations. Keep the file — it is the checklist for Tasks 2-4.

**Do not commit yet** — the test must go green in the same commit as the migration, or the suite is red between commits.

---

## Task 2: The `--lp-px` gutter and the landing sections

**Files:** `web/static/index.html`

- [ ] **Step 1: Add the gutter token**

`.lp` already carries `--lp-gap`. Add its horizontal sibling:

```css
.lp{
  --lp-gap:clamp(56px,7vw,96px);
  /* The section gutter, previously a literal 22px repeated in six rules. 22px is
   * on no step (--s-5 is 20, --s-6 is 24), so it becomes a fluid clamp between
   * them — same pattern as --lp-gap, and it lets narrow viewports keep 20px
   * while wide ones get 24px. */
  --lp-px:clamp(var(--s-5),2.5vw,var(--s-6));
  --lp-max:1120px;
}
```

- [ ] **Step 2: Point all six gutters at it**

Replace the horizontal `22px` in `.lp-hero`, `.lp-cov`, `.lp-how`, `.lp-method`, `.lp-cta` and `.lp-foot` with `var(--lp-px)`.

`.lp-hero` is on the spacing-exempt list for its *vertical* budget, but its **horizontal** gutter should still use the token — edit only the horizontal component and leave its vertical `clamp()`s alone.

- [ ] **Step 3: Migrate the landing section rules**

Work the `/tmp/mkt-spacing.txt` list for `.lp-cov*`, `.lp-how`, `.lp-steps`, `.lp-step*`, `.lp-method*`, `.lp-cta*`, `.lp-foot*`, `.lp-h1`, `.lp-h2`, `.lp-section-hd`, `.lp-sub`, `.lp-cta-row`.

Round to the nearest step, ties up: `13→12 (--s-3)`, `14→16 (--s-4)`, `17→16`, `18→20 (--s-5)`, `20→--s-5`, `22→24 (--s-6)`, `26→24`, `28→32 (--s-8)`, `32→--s-8`, `36→32`, `40→--s-10`, `48→--s-12`, `60→64`… **there is no `--s-16`**, so values above 48px (60px, 80px) stay literal — add a short comment at each saying the scale tops out at 48.

Small values: `1px`, `2px`, `3px`, `6px`, `7px`, `9px`, `10px`, `11px` — round to `--s-1` (4) or `--s-2` (8) where that reads the same, and **leave anything ≤3px literal** with a comment (a 1-3px nudge is optical alignment, not spacing; the same reasoning Phase 1 used for its 1-4px decorative radii).

- [ ] **Step 4: Note the two dead rules you will pass**

While in this block you will see `.lp-h1`'s `font-size:clamp(34px,4.4vw,54px)` and `.lp-sub`'s `margin`/`font-size`. Both are **overridden** — the only `.lp-h1` and `.lp-sub` in the markup are inside `.lp-hero-hd`, so `.lp-hero .lp-h1` and `.lp-hero-hd .lp-sub` win. Editing the base rules to change the hero's look is a no-op. Add a one-line comment at each recording that, so the next editor does not try.

- [ ] **Step 5: Verify the hero still clears the fold**

This is the one measurement that matters here. Run the app and check at **1280×800** and **390×844** that the minigame card is still visible without scrolling. If your gutter change pushed it down, revert the hero's horizontal token and keep the literal.

- [ ] **Step 6: Run the tests**

Run: `python -m pytest tests/api_tests/test_css_tokens.py::test_marketing_spacing_goes_through_the_scale -q`
Expected: still FAIL, but the count should be roughly halved (the pricing page is untouched).

- [ ] **Step 7: Commit**

```bash
git add web/static/index.html
git commit -m "refactor(landing): one gutter token, and section spacing on the scale"
```

---

## Task 3: The landing sub-components

**Files:** `web/static/index.html`

- [ ] **Step 1: Migrate the remaining `.lp-*` rules**

From the worklist: `.lp-why-h`/`.lp-books-h`, `.lp-why-steps` (+ `li`, `div span`), `.lp-why-n`, `.lp-vig*`, `.lp-books` (+ its `@media` copy), `.lp-bk-*`, `.lp-books-verdict`, `.lp-books-bars`, `.lp-books-barrow`, `.lp-books-barlbl`, `.lp-books-barpct`, `.lp-books-vig`, `.lp-books-close`, `.lp-cov-cell`.

`.lp-books`'s `16px 17px` and its media copy's `15px 16px` are on no scale at all — the clearest drift on the page.

- [ ] **Step 2: Do NOT simplify `.lp-why-steps div span`**

That selector is deliberately `div span`, not `span`. A comment above it records the exact regression: a bare `.lp-why-steps span` at (0,1,1) beat `.lp-why-n` at (0,1,0) and pushed the step digits out of their circles — the bug fixed in commit `2d9540d`. Change its *values* if the worklist names it; never its shape.

- [ ] **Step 3: Leave the PrizePicks colour leaks alone**

Six non-`lp-game-` rules in the reveal panel deliberately carry PP's colours so the game and its receipt read as one system: `.lp-vig-key.is-vig` (`#FF4A4A`), `.lp-books-verdict.is-good` / `.is-bad` (`#6EFF00` / `#FF4A4A`), `.lp-books-barfill` (`#4B4C5C`) and `.is-fav` (`#6EFF00`), `.lp-bk-q.is-fav` (`#6EFF00`). You are changing spacing, not colour — but if you find yourself tempted, `#6EFF00` is PP's selected green and substituting `--green` desynchronizes the verdict from `.lp-game-btn.is-sel`.

- [ ] **Step 4: Do not touch `.lp-books-barfill`'s animation**

It animates `transform:scaleX(var(--p,0))` with `--p` set inline from JSX. A comment bans converting it to `width` (layout per frame). Its `gap`/`margin` are fair game; the `transform` is not.

- [ ] **Step 5: Tests + commit**

The spacing test should now be down to the pricing page's declarations only.

```bash
git add web/static/index.html
git commit -m "refactor(landing): hero panel and reveal-panel spacing on the scale"
```

---

## Task 4: The pricing page

The recon's summary: "essentially the whole file" is off-scale, and it has one `clamp()` against landing's seven.

**Files:** `web/static/index.html`

- [ ] **Step 1: Find the copies that win before you edit anything**

Run: `grep -n "^\.pp{\|^\.pp-card-wrap{" web/static/index.html`

Both are declared **twice**, and the later copies (in the density-overrides block) win for `max-width` and `padding`. Editing the base rule's padding ships a no-op. Confirm which is which before touching either.

- [ ] **Step 2: Migrate the spacing**

Work the worklist for every `.pp-*` rule. Same rounding as Task 2. The five `4px` sites Phase 2b deferred (`.pp-toggle` gap, `.pp-price-per` and `.pp-books-plus` margin-left, `.pp-pay-chip` padding, `.pp-ft-item em` margin-top) all become `var(--s-1)` — value-identical, so they are free.

- [ ] **Step 3: Fix the `.pp-card-hd` collision**

This is a real bug, not polish. The rule is `display:flex;justify-content:space-between;align-items:center` with **no `gap` and no `flex-wrap`**, and two children whose intrinsic widths sum to ~296px. Measured: collides at **320, 360 and 390px**, clears by only ~6px at 414px.

```css
.pp-card-hd{display:flex;justify-content:space-between;align-items:center;
            gap:var(--s-3);flex-wrap:wrap;margin-bottom:var(--s-5)}
```

And make the wrap deterministic by adding `white-space:nowrap` to `.pp-card-trial`, so the badge drops to its own line instead of both texts wrapping into ragged two-line blocks.

- [ ] **Step 4: Make the card's padding and the price fluid**

At 320px the card has 208px of content width; `padding:36px 32px 28px` spends 64px of it and `.pp-price-d`'s `font-size:64px` spends most of the rest.

```css
.pp-card{... padding:clamp(var(--s-6),4vw,var(--s-8)) clamp(var(--s-5),4vw,var(--s-8)) var(--s-8) ...}
.pp-price-d{font-size:clamp(44px,8vw,64px)}
```

Keep the desktop end of each clamp at today's value so nothing changes above ~800px — this is a narrow-viewport fix, not a redesign.

- [ ] **Step 5: Put the FAQ heading on the shared scale**

`.pp-faq-h` is a fixed `22px` while `.lp-h2` is `clamp(24px,2.6vw,32px)`. Two section headings on the same signed-out surface should not be on different scales. Point `.pp-faq-h` at the same clamp.

- [ ] **Step 6: The spacing test should now pass**

Run: `python -m pytest tests/api_tests/test_css_tokens.py -q`
Expected: **all pass**, including the new spacing invariant (403 total in the file's count).

If violations remain, they are either a genuine miss or a value that deserves an exemption — if you add an exemption, it must name the selector literally (not a prefix) and carry a written reason, following `RED2_EXEMPT`'s precedent.

- [ ] **Step 7: Verify the narrow viewports**

Check **320, 360, 390 and 414px**: the card header must not collide, the price must fit, and nothing may clip. Report the measured clearance at each.

- [ ] **Step 8: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html
git commit -m "refactor(pricing): spacing and type on the scale, and fix the card-header collision"
```

---

## Task 5: The four token/JSX leftovers

The only task that touches `.jsx`, so it is the only one needing `./build.sh`.

**Files:** `web/static/index.html`, `web/static/landing.jsx`, `web/static/pricing.jsx`, `tests/api_tests/test_css_tokens.py`

- [ ] **Step 1: Migrate `.lp-game-err` and delete its exemption**

Change its `color:#FCA5A5` to `var(--red-2)`, then delete `RED2_EXEMPT` and the `or selector.strip() in RED2_EXEMPT` clause that consumes it, so `test_error_red_is_tokenized` becomes unconditional.

Record in the commit **why this one `lp-game-` rule is fair game**: `#FCA5A5` is CoreProp's error red, not one of PrizePicks' clone tokens (`#FF4A4A` is PP's loss red); this is the only `lp-game-*` rule using a site-palette colour; the element renders *outside* the PP card as a `role="alert"` status line; and Phase 2b's own exemption comment asks Phase 3 to do exactly this.

- [ ] **Step 2: Give the pricing error line a class**

`pricing.jsx` has an inline `style={{color:"#FCA5A5"}}` on the checkout-error div. Do **not** reuse `.cp-state-err` — it also carries padding and a font-size that would change this line's geometry. Add:

```css
.pp-cta-sub.is-err{color:var(--red-2)}
```

and change the JSX to `className="pp-cta-sub is-err"`, dropping the inline style.

- [ ] **Step 3: Fix the accent literal in the SVG**

`landing.jsx` has `fill="#1E6FB0"` on one of the vig-visualizer bars — the accent typed as a literal. Its paired legend key (`.lp-vig-key.is-fair`) correctly uses `var(--primary)`, so the bar and its own legend can silently disagree, and nothing pins it (the three-copy accent test only covers `:root`, `app-main.jsx` and `dist/app-main.js`).

Change it to `fill="var(--primary)"`. **This works** — Phase 2b verified in Chromium that `var()` in an SVG presentation attribute computes byte-identically to the literal (it was used for the chart grid lines). Verify it renders rather than assuming.

- [ ] **Step 4: Move the last inline spacing into CSS**

`pricing.jsx` has a `style={{ marginTop: 4 }}` on the "Manage subscription" link. Replace with a class using `var(--s-1)`.

- [ ] **Step 5: Rebuild — mandatory**

```bash
./build.sh
python -m pytest tests/ -q
```

`./build.sh` rewrites `index.html`'s ten `?v=` tokens and `sw.js`'s cache name, so **finish all `<style>` edits before running it**. Commit the `.jsx`, `dist/`, `index.html` and `sw.js` together.

Note `test_landing_claims.py` scans `dist/landing.js` and `dist/pricing.js` as well as the sources — a rebuild can surface a banned literal that the readable source hid, so run the full suite after building, not before.

- [ ] **Step 6: Commit**

```bash
git add tests/api_tests/test_css_tokens.py web/static/index.html web/static/landing.jsx web/static/pricing.jsx web/static/dist web/static/sw.js
git commit -m "refactor(marketing): tokenize the last four hardcoded values"
```

---

## Task 6: Phase 3 verification

- [ ] **Step 1: Full suite** → **403 passed** (402 + the new spacing invariant). No failures, no skips.

- [ ] **Step 2: The copy contract**

Run: `python -m pytest tests/api_tests/test_landing_claims.py -q` → **8 passed**.

This is the test that matters most on this surface. It bans twelve invented statistics as **substrings** (`2,847`, `4,200`, `58.4`, `1,284`, `11.7`, `14,823`, `1,287`, `54.07`, `0.5407`, `30 seconds`, `30 sec`, `five seasons`), seven inactive league names, three invented testimonials, a hardcoded trial length, and a typed break-even. **Because they are substrings, a numeric value typed into a JSX inline style can trip it** — `11.7` and `0.5407` are the plausible ones. If it fails, check what you typed rather than the test.

- [ ] **Step 3: Confirm no copy changed**

Run: `git diff main..HEAD -- web/static/landing.jsx web/static/pricing.jsx | grep -E "^[-+]" | grep -vE "^[-+][-+]" | grep -iE "[a-z]{4,} [a-z]{4,}"`

Read the output: every line should be a class name, a style object or an attribute — **no sentence of user-facing prose**. Paste it in your report.

- [ ] **Step 4: Bundles fresh**

```bash
./build.sh && git diff --exit-code web/static/dist web/static/index.html web/static/sw.js
```
Expected: no diff, exit 0.

- [ ] **Step 5: Guards**

Run: `python -m pytest tests/api_tests/test_css_guards.py tests/api_tests/test_css_tokens.py tests/api_tests/test_build_stamp.py -q` → all pass.

- [ ] **Step 6: Visual review**

Establish an after-vs-after noise floor first — **the landing page has two stable render states** and a naive diff shows a phantom ~5% change plus a page-height flip. Earlier phases reached byte-identical captures with a deterministic harness; the pricing page was reliably 0px.

Capture before/after at **320, 360, 390, 414, 768 and 1440px**:
- The pricing card header at every narrow width — the collision must be gone, with measured clearance.
- The pricing card overall — the price must fit and nothing may clip.
- The landing hero at **1280×800**: the minigame card must still clear the fold.
- Each landing section's rhythm — the gutter change is subtle and should read as more even, not different.
- **The minigame card must be pixel-identical** apart from the error line's colour (which is value-identical anyway).

- [ ] **Step 7: Scope check**

Run: `git diff --stat feat/frontend-modernization-phase-2b..HEAD`
Expect only the four files in the File Structure table plus `dist/` and `sw.js`. No app-screen CSS, no iOS.

---

## Definition of done (Phase 3)

- `pytest tests/ -q` → 403 passed.
- `test_landing_claims.py` → 8 passed, and no prose line in the JSX diff.
- The spacing invariant passes with every exemption named literally and reasoned.
- One `--lp-px` token replaces six gutters; `--s-1` gains the five pricing sites.
- `.pp-card-hd` no longer collides at 320/360/390px; the card padding and price are fluid.
- `.pp-faq-h` shares `.lp-h2`'s scale.
- `.lp-game-err` on `--red-2` with `RED2_EXEMPT` deleted; the pricing error line and the SVG accent are tokenized; no inline spacing left on either page.
- The minigame is otherwise untouched; the hero still clears the fold at 1280×800.
- `./build.sh` produces no diff.

## Deferred, with reasons (do not silently drop these)

- **`#0a0a12` / `#1f1f2e` / `#0c0c14` / `#0e0e16`** — a four-hex "sunken surface" family with 14+ sites across six screens. A token is coherent only as one cross-screen commit; two-of-fourteen would be worse than today. Needs its own pass.
- **`.pp-card-glow`** — `.pp-card{overflow:hidden}` clips its outer pixel, so it renders as a second inner hairline, not the outer ring it appears to be. Possibly redundant; deciding is a visual call and removing it is a JSX change.
- **`analytics-preview.html`** — an unreferenced harness holding a stale verbatim copy of `.pnl-*` CSS and the palette. No test covers it. Delete or update in a dedicated change.

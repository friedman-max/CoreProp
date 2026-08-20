# Frontend Modernization — Completeness Audit and Dispositions

**Date:** 2026-08-20
**Scope audited:** Phases 1, 2a, 2b (web app screens) against
`docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` and the three
phase plans. Phases 3 (marketing) and 4 (iOS) were in flight at audit time and
were verified separately by their own definitions of done.

**Why this document exists.** Each phase was reviewed task-by-task *at the time*,
by a reviewer holding one task's context. Nobody had checked the three phases
**as a whole against the spec**, which is a different question and finds different
things: a requirement that was dropped between spec and plan is invisible to a
per-task reviewer, because no task ever claimed it.

Six such items existed. All six are now resolved. The audit also found nine
coverage gaps — load-bearing invariants that nothing pinned — and those turned out
to matter more than the defects, because they are what let the defects survive
three rounds of review.

---

## The six findings

| # | Finding | Disposition |
|---|---|---|
| F1 | Spec required flattened cards to carry `--shadow-card`; it landed on **zero** new surfaces (both consumers pre-dated the phase) | **Decided against, on measurement** — see below |
| F2 | `.bt-slip-place-install` kept a gradient and a `:hover` that **lightens**; handed 2a→2b and dropped | Fixed — flat fill, darkening hover |
| F3 | The micro-label tracking pass grepped `.1em` and missed the `.10em` spelling (2 sites, 1 live) | Fixed both; census run |
| F4 | `.pnl-custom-input{background:#14141e}` — the one Analytics literal *with* an exact token — silently skipped | Fixed → `var(--card)` |
| F5 | `.bt-card{padding:14px 16px}`, the spec's own headline example of padding drift, never migrated | Fixed in the 133-declaration spacing sweep |
| F6 | The three CSS bans read only `index.html`, not the four-file set the spec named | Fixed — all four sources, including the injected sheet |

### F1 in detail, because "we chose not to" needs its reasoning recorded

`--shadow-card` is `0 12px 32px -18px rgba(0,0,0,.7)`. Measured in Chromium
against the real stylesheet:

| separation mechanism | contrast ratio |
|---|---|
| `--shadow-card` peak, on `--bg` | **1.018:1** |
| the `--hair` border these surfaces already carry | **1.240:1** |
| `--card-2` vs `--card` (Phase 2b's actual fix) | 1.061:1 |

The shadow moves the pixel under a card edge from `rgb(10,10,13)` to
`rgb(7,7,10)` and decays to 1/255 within 16px. **The hairline every candidate
already has is ~13× that step.** A drop shadow is a light-mode mechanism; on
`#0a0a0d` a black shadow has nothing to darken.

Two facts settled it. First, `.pp-card`'s *shipped* shadow measures the identical
3/255 — so the token is not "working on the heroes and missing elsewhere", it
barely works anywhere at this palette, which is why nobody ever reported its
absence as a symptom. Second, the one flatness complaint four phases of review did
produce (`.bt-card` inside `.an-panel`) was fixed with a **tone step**, which
measures stronger than a shadow would and needs no scoping. This palette's
separation mechanism is the elevation ladder, not elevation.

Recorded at the token with all nine considered surfaces named, and pinned: the
`CARD_ELEVATION` set is deliberately empty, `CARD_NO_ELEVATION` holds all twelve
flattened surfaces, and a partition assertion means a newly-flattened surface
cannot be added without an elevation decision being made about it.

This also keeps `ios/CoreProp/Theme/Theme.swift`'s comment true, which asserts
that web puts `--shadow-card` on exactly two surfaces.

---

## The nine coverage gaps — all closed

These are the reason the six findings survived. Each is now an invariant.

| Gap | Now pinned by |
|---|---|
| No app-screen spacing invariant (marketing had one; the four app screens did not) | `test_app_screen_spacing_goes_through_the_scale` — 234 declarations, and a test proving the two scopes *partition* the sheet |
| Nothing pinned hover-darkens | `test_css_hover.py` — 33 hover rules, 14 resolved, 19 **skipped and counted** |
| Nothing pinned the 34px filter-bar contract (three shipped misalignment bugs) | 5 tests over 10 controls; the two padding-derived ones declared unguarded rather than faked |
| Nothing pinned the three derived radii offsets | exact expressions, with why each offset is what it is |
| Nothing pinned `--shadow-card` on cards | the empty-set partition above |
| Nothing pinned the markup↔injected-stylesheet class contract | `test_injected_sheet_contract.py` — classes derived from the sheet, checked in `.jsx` **and** `dist/` |
| `letter-spacing` had no invariant | normalises `.1em`/`0.1em`/`.10em` — the exact bug that let F3 survive |
| `TEXT4_ALLOWED` matched by substring and admitted it was unsafe | exact matching plus a **premise** guard |
| CI ran on `main` only, so `bundles` (the build-contract check) never fired on a feature branch | both workflows now trigger on `feat/**` |

---

## Beyond the audit

Work the audit did not ask for but which followed from it:

- **The semantic colour pairs were three-quarters missing.** Web had `--red-2` and
  no `--green-2` / `--amber-2` / `--blue-2`, while `Theme.swift` asserted all four
  existed in web. 21 literal sites migrated; the family is now parameterized in
  one test, plus a general "no literal hex that has a token" guard that needs
  exactly **one** exemption against 21 guarded tokens.
- **iOS had no invariants at all.** `test_ios_tokens.py` (12 tests) pins the token
  scales, the no-gradient rule, the `text4` allowlist and the iOS 16 API floor.
  Its worklist immediately found that `cpCard`'s own `padding: 14` default had
  survived the file's modernization — because that phase worked from a
  hand-written checklist rather than an invariant, which is the same failure mode
  as F5.
- **`ios/localcheck/`** typechecks all 31 Swift files locally by shimming the
  iOS-only API, up from a measured clean closure of 10. It covers the seven
  account/auth files that no local gate could reach and that were the riskiest in
  Phase 4.

---

## Open items, deliberately not resolved

- **`web/static/analytics-preview.html`** holds a second, badly stale `:root` —
  `--primary` is the pre-rebrand indigo `#6366F1`, the radii are the pre-scale
  literals including the deleted `--radius-xs`, and 27 of its 30 spacing
  declarations are off the scale, including the exact two the spec names. It is
  referenced by no code but **is served** at `/static/analytics-preview.html`. Its
  header comment claimed the CSS was "copied verbatim from index.html"; that claim
  is replaced with the measured divergence list, and `CLAUDE.md` no longer says
  there is no second palette source. **It wants deleting or re-pointing — a human
  decision, since deleting a served page is not a styling change.**
- **20 of the 133 migrated spacing declarations are on dead CSS** (all `.obs-*`,
  all `.cal-*`, `.bd-chip(s)`, `.ev-sharp`, `.an-panel-h`, `.an-feed-count`),
  verified against every `.jsx` and bundle. Migrated anyway on `.ev-sharp`'s own
  recorded precedent, and noted in the CSS — but a meaningful share of the
  Analytics delta is invisible in today's product.
- **The four-hex sunken-surface family** (`#0a0a12` ×14, `#0c0c14`, `#0e0e16`,
  historically `#13131c`) is still literal. Naming a token requires first deciding
  whether the other three are mistakes or deliberate depths — coherent only as one
  cross-screen commit, which is why two phases deferred it.
- **`.pp-card-glow`** renders as an inner hairline, not the outer ring it appears
  to be, because `.pp-card{overflow:hidden}` clips its outer pixel. Possibly
  redundant; deciding is a visual call and removing it is a JSX change.

---

## What no gate here can establish

- **iOS CI has never run on this work.** `xcodebuild` needs a push, which was not
  available in this session. Local gates cover parse, the 109-assertion package
  verifier, and a typecheck of all 31 files — but not linking, codegen, the asset
  catalogue, `Info.plist`, entitlements, signing, `@objc` selector spelling, or
  real iOS availability (the macOS/iOS target pairing is a convention, not a
  contract).
- **Nothing in iOS has been seen rendered.** No Xcode, no simulator, no
  screenshot. Every iOS claim is a parser, a typechecker or pytest.
- **The four web app screens have not been seen in the running product** — they
  are behind auth. Every measurement of them comes from the real `<style>` block
  applied to hand-written markup with real class names in headless Chromium. That
  substitute was wrong once during this work (a class on a `<td>` where production
  puts it on a `<span>`) and was caught by re-reading the JSX, which is the
  failure mode to assume rather than discount.
- **The signed-out surfaces *were* verified in the running app** at 1440px and
  390px: the accent renders `#1E6FB0`, the vig visualizer's `fill="var(--primary)"`
  paints, the step numerals stay centred, and the break-even reads 54.66%, which
  is `37.5^(-1/6)` — i.e. computed from the payout table rather than typed.

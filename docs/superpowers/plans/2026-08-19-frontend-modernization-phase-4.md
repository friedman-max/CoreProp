# Frontend Modernization — Phase 4 (iOS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port the design system to the native iOS app: the radius/spacing/elevation scales, flat card surfaces, tokenized colours, the three P&L chart parity fixes, and three contrast corrections. Purely visual — no handler, API, or state changes.

**Architecture:** `ios/CoreProp/` is a SwiftUI app; `ios/CorePropKit/` is a Foundation-only Swift package. `Theme.swift` and `Components.swift` are the design system, in the **app target** (not the package — deliberately, so the package stays SwiftUI-free and its verifier runs on bare Command Line Tools). **This phase touches nothing under `ios/CorePropKit/`.**

**Tech Stack:** SwiftUI, iOS 16 deployment target, Swift language mode 5, XcodeGen, GitHub Actions (`macos-15`).

**Source spec:** `docs/superpowers/specs/2026-08-19-frontend-modernization-design.md` **Section 5 — iOS parity** is a complete design-time plan with a numbered 9-screen inventory. This document decomposes it into executable tasks. **Where the spec and this plan disagree on a line number or a value, this plan wins** — the spec predates Phases 2a/2b (see Trap 12).

**Predecessors:** Phases 1, 2a, 2b (web) are done. Phase 3 (web marketing) runs in parallel on `web/static/**` — **zero file overlap with this phase.**

---

## The verification situation — read this first

**There is no Xcode and no iOS SDK on this machine.** `xcodebuild` cannot run locally. There are four gates, in ascending strength:

| Gate | Command | Covers | Strength |
|---|---|---|---|
| 1. Syntax | `xcrun swiftc -parse <file>` | all 31 app files | Weak — passes a renamed token with a missed call site |
| 2. Package | `cd ios/CorePropKit && swift build && swift run CorePropKitVerify` | the Foundation-only package, 109 assertions | Strong, but **structurally immune** to this phase (see Trap 1) |
| 3. **Typecheck** | see below | **16 of 31 app files, including the 4 most important** | **The real local gate — use it constantly** |
| 4. CI | `.github/workflows/ios.yml` | the whole app target via `xcodebuild build` | Definitive, but **will not fire on a feature-branch push** (Trap 2) |

**Gate 3, verbatim — this recipe is verified working:**
```bash
cd /Users/maxfried/CoreProp/ios
(cd CorePropKit && swift build)          # populates the module dir; .gitignored
MODDIR=CorePropKit/.build/arm64-apple-macosx/debug
xcrun swiftc -typecheck -sdk "$(xcrun --show-sdk-path)" -target arm64-apple-macos14.0 \
  -I "$MODDIR/Modules" -I "$MODDIR" \
  CoreProp/Theme/Theme.swift CoreProp/Theme/Components.swift \
  CoreProp/Features/Bets/BetRow.swift CoreProp/Features/Backtest/SlipCard.swift
```
Clean output means success. `Charts` also resolves on the macOS SDK, so `AnalyticsView`'s chart *expressions* can be typechecked in isolation with a stub for its iOS-only modifiers.

**The 16 typecheckable files:** `Theme/Theme.swift`, `Theme/Components.swift`, `Features/Bets/BetRow.swift`, `Features/Backtest/SlipCard.swift`, `Features/Bets/BetsViewModel.swift`, `Features/Backtest/BacktestViewModel.swift`, `Features/Slip/SlipViewModel.swift`, `Features/Lines/LinesViewModel.swift`, `App/MainTabView.swift`, `App/AppRouter.swift`, `App/Shared.swift`, `App/AppModel.swift`, `App/KeychainSessionStore.swift`, `App/SlipStore.swift`, `App/AuthManager.swift`, `App/AppConfig.swift`.

The other 15 use iOS-only API (`navigationBarTitleDisplayMode`, `listStyle(.insetGrouped)`, `UIImage`, `textInputAutocapitalization`, `import UIKit`, `UNUserNotification*`) and can only be reached by CI.

---

## Scope

**In scope:** `ios/CoreProp/Theme/*.swift` and `ios/CoreProp/Features/**`, plus the two styling lines in `ios/CoreProp/App/CorePropApp.swift` / `RootView.swift`.

**Out of scope:** everything under `ios/CorePropKit/` (including `EV/Payouts.swift`, the 6th payout mirror), `ios/CorePropTests/`, `ios/project.yml`, `ios/CoreProp.xcodeproj`, the asset catalogues (already correct — `AccentColor` is `#1E6FB0`, `LaunchBackground` is `#0A0A0D`), and all of `web/`.

---

## Decisions this plan makes

1. **The radius aliases stay, including `radiusXs`.** Web *deleted* `--radius-xs` because it had zero consumers; iOS's `radiusXs` has **two** (`BookBadgeView`, the `BetsView` filter-menu button) and `radiusSm` has eight. Re-point them (`radius`→`rLg` 14→16, `radiusSm`→`rMd` 10→**12**, `radiusXs`→`rSm` 8 unchanged) so no call site breaks. The two decisions only *look* contradictory.
2. **`cardGradTop`/`cardGradBot` are retained as constants but stop being used.** They have no web counterpart (`#15151F`/`#0F0F17` appear nowhere in `index.html`), and the phase's rule throughout has been "only the gradient *usage* goes, no hex value changes." Their doc comments, which currently assert gradients are "allowed on non-accent surfaces," must be reworded — leaving them is a documentation contradiction.
3. **Filter chips grow to 34pt.** The spec says "chips keep 34pt" but iOS chips are ~28pt today (7+7 padding at 13pt). The only sibling control is the 34×34 filter-menu button, so growing the chips is what actually produces one baseline. Web's 34px contract exists for the same reason.
4. **The true% stays flat blue on iOS.** This is the one **explicitly accepted divergence** in the spec: SwiftUI has no OKLCH colour space (and `Color.mix(with:)` is iOS 18), so porting web's `oklch` ramp means hand-writing a colour conversion, and flattening web's ramp to blue is a colour change the non-goals forbid. **Only the size changes: 16 → 20pt.**
5. **Push gets its own colour.** iOS conflates push with `Theme.amber` (`#F59E0B`), but web's push family is `#FBBF24` — a different hue that also serves warnings. Add a distinct token so push matches web and `amber` stays the warning colour.
6. **The calibration chart keeps `primary2` and `.monotone`.** It is a different chart with a different meaning; only the P&L chart gets the directional treatment.
7. **Three `Theme.text4` sites move to `text3`.** `text4` is 2.3:1 and is for decorative glyphs; it is currently on readable labels in `BetDetailView` (the `SIZE`/`POWER`/`FLEX` table header), `LinesView` (`BEST`/`FAIR` at 8pt) and `SlipCard` (`PAYOUT`/`HITS`/`PROJ EV`). Same class of bug web fixed twice.
8. **`.tracking()` replaces `.kerning()` at `.04em`-equivalent values.** `.tracking()` is the true `letter-spacing` analogue; `.kerning()` adjusts pairs. Web's micro-label tracking is `.04em`, which at 10.5pt is ≈**0.42pt**, not the 0.6 currently used — iOS labels are proportionally wider than web's.
9. **`SectionHeader` stays dead.** Adopting it across 11 inline section labels is a refactor, not a visual pass. Restyling it while nothing calls it would be worse.
10. **`DataAgePill`'s freshness thresholds are out of scope.** Web encodes staleness in three colours plus a `live` state; iOS's dot is unconditionally green, so it shows green over stale data. That is arguably a correctness bug, but adding thresholds is *logic*. Recorded, not fixed.
11. **`FilterChip`'s league-tinted selected state is preserved.** `BetsView` feeds `Theme.leagueColor(lg)` in as the chip's fill; web always uses `--primary`. It looks deliberate and is an encoding, so it stays.
12. **Row density will drop and that is accepted.** iOS rows go from `10` vertical padding to `--s-4` (16). Web accepted a ~10.7% drop in rows-above-fold for the same reason; iOS's jump is proportionally larger. Stated up front rather than discovered in review.

---

## File Structure

| File | Change |
|---|---|
| `ios/CoreProp/Theme/Theme.swift` | Radius scale + aliases, spacing scale, elevation tokens, `red2`/`green2`/`amber2`/`blue2`/`push`, reworded `cardGrad*` comments |
| `ios/CoreProp/Theme/Components.swift` | `cpCard` flat, `StatTile`, `FilterChip` 34pt, badges, `ErrorStateView`, `SkeletonRow` |
| `ios/CoreProp/Features/Account/AnalyticsView.swift` | The three P&L fixes + panel spacing |
| `ios/CoreProp/Features/Bets/*.swift` | `BetRow`, `BetsView`, `BetDetailView` |
| `ios/CoreProp/Features/Lines/LinesView.swift` | Row padding, labels, filter spacing |
| `ios/CoreProp/Features/Backtest/*.swift` | `SlipCard`, `BacktestView` |
| `ios/CoreProp/Features/Slip/SlipView.swift` | Cards, blocks, banner |
| `ios/CoreProp/Features/Account/*.swift` | `AccountView`, `SettingsView`, `SubscriptionView`, `NotificationsView`, `DeveloperView` |
| `ios/CoreProp/Features/Auth/AuthView.swift` | Card radius, inputs, focus ring |

---

## Task 1: The token layer

**Files:** `ios/CoreProp/Theme/Theme.swift`

- [ ] **Step 1: Baseline all four gates**

```bash
cd /Users/maxfried/CoreProp/ios/CorePropKit && swift build && swift run CorePropKitVerify
```
Expected: **109 passed, 0 failed**.

```bash
cd /Users/maxfried/CoreProp/ios
for f in $(find CoreProp -name '*.swift'); do xcrun swiftc -parse "$f" >/dev/null || echo "PARSE FAIL $f"; done
```
Expected: no output (31 files).

Then run the Gate 3 typecheck recipe from the top of this document. Expected: clean.

**Also record the acceptance baseline:**
```bash
grep -rn "LinearGradient" ios/CoreProp/
```
Expected: **3 hits** — `Components.swift` (`cpCard`), `SlipCard.swift`, `AnalyticsView.swift`. After this phase there must be exactly **one** (the P&L directional fill).

- [ ] **Step 2: Add the radius scale and re-point the aliases**

```swift
    // Radii. Mirrors web's --r-* scale. The three legacy names are kept as
    // aliases so none of the 10 existing call sites breaks: `radius` has 14 (via
    // cpCard's default), `radiusSm` 8, `radiusXs` 2. Note web DELETED its
    // --radius-xs because it had zero consumers there; here it has two, so it
    // stays. Re-pointing is not value-neutral: radius goes 14->16 and radiusSm
    // 10->12, which moves every cpCard at once.
    static let rXl: CGFloat = 20
    static let rLg: CGFloat = 16
    static let rMd: CGFloat = 12
    static let rSm: CGFloat = 8
    static let radius: CGFloat = rLg      // was 14
    static let radiusSm: CGFloat = rMd    // was 10
    static let radiusXs: CGFloat = rSm    // was 8, unchanged
```

There is deliberately **no `rPill`** — `Capsule()` is already the idiom and is used throughout.

- [ ] **Step 3: Add the spacing scale**

```swift
    // Spacing. Mirrors web's --s-* scale exactly, including its gaps: there is no
    // s7, s9 or s11 on either side. ~150 literal padding values across the app
    // migrate onto these; 14 is the most common literal (12 sites) and maps to
    // s4, so rows get roomier — the same accepted trade web made.
    static let s1: CGFloat = 4
    static let s2: CGFloat = 8
    static let s3: CGFloat = 12
    static let s4: CGFloat = 16
    static let s5: CGFloat = 20
    static let s6: CGFloat = 24
    static let s8: CGFloat = 32
    static let s10: CGFloat = 40
    static let s12: CGFloat = 48
```

- [ ] **Step 4: Add the elevation tokens**

Web's `--shadow-card` is `0 12px 32px -18px rgba(0,0,0,.7)`. CSS's negative spread has no SwiftUI equivalent, so approximate it with a tighter, darker shadow than the current one and say so:

```swift
    // Elevation. Web's --shadow-card is `0 12px 32px -18px rgba(0,0,0,.7)`; the
    // -18px spread has no SwiftUI analogue, so this approximates it as a tighter,
    // darker shadow than the one it replaces (black .35 / radius 24 / y 16).
    // Worth knowing: web puts --shadow-card on only TWO surfaces (.cp-modal and
    // .pp-card) and most cards carry no shadow at all, where iOS shadows every
    // card via cpCard.
    static let shadowColor = Color.black.opacity(0.7)
    static let shadowRadius: CGFloat = 16
    static let shadowY: CGFloat = 6
    /// Focus ring width, mirroring --ring's 4px on --primary-hi.
    static let ringWidth: CGFloat = 4
```

- [ ] **Step 5: Add the missing colour tokens**

```swift
    /// Row-level accent tint. Mirrors web's --primary-lo. --primaryHi (.22) is
    /// for rings and badges where an explicit light colour sits on top; behind a
    /// row it is inherited by muted text and fails AA.
    static let primaryLo = Color(hex: 0x1E6FB0, alpha: 0.10)
    /// The lighter error/miss red, for TEXT on a red tint. Mirrors web's --red-2.
    /// `red` (#EF4444) is the semantic red for bars, borders and fills; this is
    /// what reads on top of one. Inlined at 21 sites before this.
    static let red2 = Color(hex: 0xFCA5A5)
    /// Result-chip foregrounds, mirroring web's hit/push/pending label colours.
    static let green2 = Color(hex: 0x86EFAC)
    static let amber2 = Color(hex: 0xFDE68A)
    static let blue2 = Color(hex: 0x93C5FD)
    /// Push is #FBBF24 on web, NOT --amber (#F59E0B). iOS conflated the two;
    /// `amber` stays the warning colour and this is the outcome colour.
    static let push = Color(hex: 0xFBBF24)
```

`Color(hex:)` already takes an `alpha:` parameter — check its signature before using it.

- [ ] **Step 6: Reword the two `cardGrad*` comments**

They currently assert neutral card gradients are "allowed on non-accent surfaces." That was true before Phase 1 and is now the opposite of the design. Keep the constants (no hex value changes anywhere in this phase) but say plainly that they are retained-and-unused, and that flat `Theme.card` is the card surface.

Do the same for `Theme.swift`'s header comment if it repeats the claim.

- [ ] **Step 7: Verify**

Re-run Gate 1 on `Theme.swift`, Gate 3 on all four files, and Gate 2. All must be clean/109.

- [ ] **Step 8: Commit**

```bash
git add ios/CoreProp/Theme/Theme.swift
git commit -m "feat(ios): add the radius, spacing, elevation and colour tokens"
```

---

## Task 2: The inline-hex migration

One grep-driven mechanical pass — the highest-leverage edit in the phase.

**Files:** every file with an inline hex (11 files)

- [ ] **Step 1: Inventory**

```bash
cd /Users/maxfried/CoreProp
grep -rn "Color(hex: 0x" ios/CoreProp/ | grep -v "Theme.swift"
```
Paste the full list. Counted against the tree: **22 sites** — `0xFCA5A5` ×17, `0x86EFAC` ×2, `0xFDE68A` ×2, `0x93C5FD` ×2. (The recon said 21 for `0xFCA5A5`; the real count is 17. If yours differs from 22, say so before migrating.)

- [ ] **Step 2: Migrate**

`0xFCA5A5` → `Theme.red2`, `0x86EFAC` → `Theme.green2`, `0xFDE68A` → `Theme.amber2`, `0x93C5FD` → `Theme.blue2`.

**Do not touch `Theme.swift`'s own definitions**, and do not touch the four `fg` values inside `Theme.bookColors` — those are book-brand encodings that happen to share two of these hexes, and collapsing them would couple a book's colour to a result colour.

- [ ] **Step 3: Fix the push conflation**

In `SlipCard.swift`, the push accent uses `Theme.amber` and the push badge/chip use `0xFDE68A`. Point the **accent** at `Theme.push` and the label at `Theme.amber2`. Leave every genuine *warning* use of `Theme.amber` alone (`ErrorStateView`, `SettingsView`'s live-mode label, `SubscriptionView`'s lock, `RootView`'s splash error).

- [ ] **Step 4: Verify**

```bash
grep -rn "Color(hex: 0x" ios/CoreProp/ | grep -v "Theme.swift"
```
Expected: **no output**.

Then Gates 1 and 3 (the typecheck covers `Components.swift`, `BetRow.swift`, `SlipCard.swift` — three of the eleven).

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor(ios): tokenize the 22 inline colour literals"
```

---

## Task 3: `Components.swift`

**Files:** `ios/CoreProp/Theme/Components.swift`

- [ ] **Step 1: Flatten `cpCard`**

Replace the `LinearGradient(colors: [Theme.cardGradTop, Theme.cardGradBot], …)` background with flat `Theme.card`, change the stroke from `Theme.hair2` to `Theme.hair` (every web card uses `--hair` at .06; iOS used .10), and move the shadow onto the new elevation tokens. The default `radius` follows the `Theme.radius` alias and so is already 16 after Task 1 — **the signature needs no edit**, but be aware this changes all 14 call sites at once.

- [ ] **Step 2: `StatTile`**

Three changes: drop `.uppercased()` on the label (web's `.bt-card-label` is not uppercase — it was de-capsed in Phase 1's typography pass), change `.kerning(0.6)` to `.tracking(0.42)` (`.04em` at 10.5pt), and put the wrapper's explicit `radius: 12` on `Theme.rMd`. The tone-bad colour is already `Theme.red2` after Task 2. The 22pt value size already matches web.

- [ ] **Step 3: `FilterChip` to 34pt**

Give it an explicit `.frame(height: 34)` so it matches the 34×34 filter-menu button that sits beside it in `BetsView`. Keep `Capsule()`, keep the `accent` parameter and its league-tint behaviour.

- [ ] **Step 4: The badges**

`BookBadgeView`: add `.tracking()` equivalent to web's `.06em` at 10pt (≈0.6) — web has tracking here and iOS has none. `SideBadge`: add web's `.05em` at 12pt (≈0.6). Keep both components' shapes.

- [ ] **Step 5: `ErrorStateView` and `SkeletonRow`**

`ErrorStateView`'s message colour is `Theme.amber`; web's error state is `--red-2`. Move the *icon* to `Theme.amber` (a warning glyph is legitimately amber) and the *message* to `Theme.text2`, with the retry button unchanged — or state a different choice with a reason.

`SkeletonRow`'s bars use `cornerRadius: 4`, which is off-scale. Web's `.cp-skel` is `--r-sm`. Use `Theme.rSm`.

- [ ] **Step 6: Verify + commit**

Gates 1 and 3 must be clean. Gate 3 covers this file directly.

```bash
git commit -m "refactor(ios): flat cards, tokenized elevation, and calmer stat labels"
```

---

## Task 4: The P&L chart — the accent-gradient violation

The only accent gradient in `ios/`, and it violates CLAUDE.md, this phase's constraint 3, **and `Theme.swift`'s own header comment**.

**Files:** `ios/CoreProp/Features/Account/AnalyticsView.swift`

- [ ] **Step 1: Read the current chart**

The `Chart` block draws a `LineMark` always `Theme.primary2`, an `AreaMark` filled with `LinearGradient([Theme.primary.opacity(0.25), .clear])`, and a dashed `RuleMark` at zero. Both marks use `.interpolationMethod(.monotone)`.

- [ ] **Step 2: Apply the three fixes**

There is already a `let last = points.last?.cum ?? 0` above, and the header colour keys off it — reuse it:

```swift
let tone = last >= 0 ? Theme.green : Theme.red
```

1. **Direction.** Both marks take `tone`. `Theme.green` is `0x22C55E` and `Theme.red` is `0xEF4444` — **already hex-identical to web's line colours**, so this introduces no new colour.
2. **No accent gradient.** The area becomes `LinearGradient(colors: [tone.opacity(0.24), .clear], startPoint: .top, endPoint: .bottom)`. Web's stop opacity is `0.24`; iOS's current `0.25` is off by .01. This remains a `LinearGradient` and that is **correct** — a *semantic* directional fill is allowed; only accent gradients are banned.
3. **Step shape.** Both marks move from `.interpolationMethod(.monotone)` to **`.stepEnd`**, the Swift Charts equivalent of web's step-after path. The shape is semantic: bankroll holds flat between settlements and jumps when one settles.

- [ ] **Step 3: Do NOT touch the calibration chart**

It keeps `Theme.primary2` and `.monotone`. It is a different chart with a different meaning, and web's version diverges further still (fixed 0-100% domain, sample-scaled dots, an overconfidence red) — porting those is a data-encoding change, not a visual one.

- [ ] **Step 4: Panel spacing**

The three `.cpCard()` panels use `VStack(spacing: 14).padding(16)`. Move to `Theme.s4`/`Theme.s5` per the scale. Also move the `"CUMULATIVE P&L"` / `"CALIBRATION RELIABILITY"` / `"CLOSING LINE VALUE"` labels from `.kerning(0.6)` to `.tracking(0.42)`, keeping them uppercase — these are true section headers, which web's rule preserves.

Note the header total is `Theme.mono(18, .bold)` where web's `.pnl-total` is 34px — the largest type-scale gap in the app. Raising it is a judgement call; if you do, say so and check it does not crowd the 180pt chart.

- [ ] **Step 5: Verify the acceptance criterion**

```bash
grep -rn "LinearGradient" ios/CoreProp/
```
Expected: **exactly one hit** — this file's directional P&L fill. The `cpCard` and `SlipCard` gradients are gone by now (Tasks 3 and 5).

- [ ] **Step 6: Typecheck the chart expressions**

This file is not in the typecheckable 16 (it uses `navigationBarTitleDisplayMode`). Typecheck the chart in isolation: copy the `Chart { … }` body into a scratch file under `/tmp` with a minimal wrapper, and run Gate 3's recipe against it (`Charts` resolves on the macOS SDK). **`.stepEnd` is the item most likely to be wrong** — confirm it exists as an `InterpolationMethod` case before relying on CI to tell you.

- [ ] **Step 7: Commit**

```bash
git commit -m "fix(ios): the P&L chart shows direction, and loses the accent gradient"
```

---

## Task 5: The Bets and Lines surfaces

**Files:** `BetRow.swift`, `BetsView.swift`, `BetDetailView.swift`, `LinesView.swift`

- [ ] **Step 1: `BetRow` — the airy-row alignment**

iOS already has the right *composition* (identity line, meta line with `LEAGUE · Prop · SIDE line · game time`, wrapping book chips, right zone with hero true% + 30pt add button). What differs:
- Row padding `10`/`12` → `Theme.s4` vertical, `Theme.s5` horizontal (web's `--row-px` is `clamp(20,2vw,24)`; 20 is the fixed-width equivalent).
- The true% goes `Theme.mono(16, .bold)` → **`Theme.mono(20, .bold)`**. **The colour stays `Theme.primary2`** — see Decision 4.
- The inner `VStack`/`HStack` spacings onto the scale.
- The add button is a `Circle()`; web's is `--r-md`. Either is defensible on iOS where circular tap targets are idiomatic — **keep the circle** and note it.
- The selected/logged row backgrounds use `Theme.primaryHi`/`Theme.redHi`. Web moved the selected row to `--primary-lo` (.10) because `.22` behind inherited muted text measures 4.45:1. **Point the selected background at `Theme.primaryLo`** (added in Task 1) for the same reason, and note that iOS's logged `redHi` (.10) is already lighter than web's `.16`.

- [ ] **Step 2: `BetsView` — the filter bar**

`padding(.horizontal, 14)` appears three times → `Theme.s4`. The bar's `padding(.vertical, 10)` → `Theme.s3`. The menu button's `Theme.radiusXs` resolves to 8 unchanged; leave it. Confirm the chips now match the button's 34pt height (Task 3 Step 3).

- [ ] **Step 3: `BetDetailView` — including a contrast fix**

- `numberTile`'s label: drop `.uppercased()` and the `.kerning(0.5)`. `TRUE PROB` / `FAIR ODDS` / `EDGE` are micro-labels, not section headers — the same de-capsing web did.
- **Contrast fix:** the `evBySlip` table header (`SIZE`/`POWER`/`FLEX`) uses `Theme.text4` at 2.3:1 on readable text. Move to `Theme.text3`.
- Spacings `16`/`10`/`8`/`12`/`6` onto the scale; the explicit `cpCard(radius: 12, …)` → `Theme.rMd`.
- Keep `"BOOK PRICES"` and `"PER-LEG EV BY SLIP"` uppercase — true section headers.

- [ ] **Step 4: `LinesView` — including a contrast fix**

- Row `padding(.vertical, 10).padding(.horizontal, 12)` → `Theme.s4`/`Theme.s5`, matching `BetRow`.
- The two `padding(.horizontal, 14)` on the pickers → `Theme.s4`.
- **Contrast fix:** `labelled()`'s label uses `Theme.text4` at **8pt** — the worst of the three. Move to `Theme.text3`, and consider 9pt.
- **Do not port web's Boards table.** Web's Boards is deliberately a `<table>` and iOS is deliberately a row list; the 34px filter contract, sticky header, sortable arrows and neutral row hover have no iOS analogue.

- [ ] **Step 5: Verify + commit**

Gate 3 covers `BetRow.swift` directly. Gates 1 and 2 for the rest.

```bash
git commit -m "refactor(ios): Bets and Lines rows on the spacing scale, plus two contrast fixes"
```

---

## Task 6: Backtest and Slip

**Files:** `SlipCard.swift`, `BacktestView.swift`, `SlipView.swift`

- [ ] **Step 1: `SlipCard` — note what is already right**

iOS already satisfies three of Phase 2b's four Backtest decisions: **ONE 3px left bar** (a single overlay, no second inset bar, no blurred glow), a **flat accent** (solid border and bar, not a gradient), and **blue pending** — iOS never had web's amber-pending bug. Do not "fix" these.

What changes:
- The card radius `12` (twice) → `Theme.rLg` (16).
- The `LinearGradient([cardGradTop, cardGradBot])` background → flat `Theme.card`.
- **Add the per-outcome card fills web has and iOS lacks:** win `green.opacity(0.16)`, loss `red.opacity(0.22)`, pending `pending.opacity(0.14)`, push `push.opacity(0.08)`. And web's borders are 2px per-outcome colours rather than `accent.opacity(0.55)` at 1.5 — match the weights.
- **Contrast fix:** the footer labels (`PAYOUT`/`HITS`/`PROJ EV`) use `Theme.text4` → `Theme.text3`.
- The leg true% uses `Theme.text3`; web's `.bt-leg-pct` is `primary2` on a `--primary-lo` fill with a `--primary-hi` border. Bringing iOS closer is optional — if you do, the fill must carry an explicit light colour (that is what keeps web's equivalent off its contrast guard).
- Spacings onto the scale; the bar's `clipShape(cornerRadius: 2)` has no web analogue — keep or drop, but say which.

- [ ] **Step 2: `BacktestView`**

The loading skeleton's `RoundedRectangle(cornerRadius: 12)` → `Theme.rMd`. `VStack(spacing:)` and `padding(16)` onto the scale.

- [ ] **Step 3: `SlipView`**

`statBlock`/`evBlock` labels: `.kerning(0.5)` → `.tracking()` at the `.04em` equivalent; keep them uppercase (they read as section labels for a numeric block, which is web's kept-caps case). The `Theme.radiusSm` uses resolve to 12 via the alias — leave them. Spacings onto the scale. The banner and `optimizeCard` likewise.

- [ ] **Step 4: Verify + commit**

Gate 3 covers `SlipCard.swift` directly.

```bash
git commit -m "refactor(ios): Backtest cards get outcome fills, and Slip goes on the scale"
```

---

## Task 7: Account cluster and Auth

**Files:** `AccountView.swift`, `SettingsView.swift`, `SubscriptionView.swift`, `NotificationsView.swift`, `DeveloperView.swift`, `AuthView.swift`, `RootView.swift`

These five account screens must move **together** or the tab visibly diverges.

- [ ] **Step 1: The account screens**

Spacings onto the scale. Section labels from `.kerning(0.6)` to `.tracking(0.42)`, staying uppercase. Keep every `.listRowBackground(Theme.card)` — that is what makes an `insetGrouped` list match the palette.

**Do not invent work here:** web's account-menu bug (a blanket destructive hover painting the push toggle red) **cannot exist on iOS** — `role: .destructive` is per-button and the Slip-alerts row is a plain `NavigationLink`. Nothing to port.

**Do not modernize `SettingsView`'s `onChange`.** It uses the iOS-16 one-parameter closure; the two-parameter form is iOS 17 and would fail CI only.

- [ ] **Step 2: `AuthView`**

- `cpCard(radius: 18, padding: 20)` → `Theme.rLg` and `Theme.s5`. 18 is off-scale.
- The six duplicated field styles: the inputs' `Theme.radiusSm` resolves to 12 (matching web's `--r-md`) automatically. **Add the focus ring** web has and iOS lacks — web's `.cp-input:focus` adds `--ring` (4px on `--primary-hi`) on top of the border colour change. Use `Theme.ringWidth` and `Theme.primaryHi`.
- Spacings `22`/`28`/`20`/`10`/`12` onto the scale.

- [ ] **Step 3: `RootView`**

The splash's `VStack(spacing: 18)`, `padding(.horizontal, 32)` and `padding(.top, 8)` onto the scale. Leave `BrandWordmark`'s `UIImage` logic alone.

- [ ] **Step 4: Verify + commit**

None of these seven files is typecheckable locally (all use iOS-only API), so Gate 1 is your only local signal — **be careful, and rely on CI.**

```bash
git commit -m "refactor(ios): account, auth and splash on the spacing scale"
```

---

## Task 8: Phase 4 verification

- [ ] **Step 1: All four gates**

```bash
# Gate 1 — syntax, all 31
cd /Users/maxfried/CoreProp/ios
for f in $(find CoreProp -name '*.swift'); do xcrun swiftc -parse "$f" >/dev/null || echo "FAIL $f"; done

# Gate 2 — the package must be untouched and still green
(cd CorePropKit && swift build && swift run CorePropKitVerify)

# Gate 3 — typecheck the 16
# (full recipe at the top of this document, all 16 files)
```
Expected: no parse failures, **109 passed**, clean typecheck.

**If Gate 2 fails, you edited the wrong tree** — the package cannot see `Theme`/`Components`, so its 109 assertions are structurally immune to this phase.

- [ ] **Step 2: The acceptance criterion**

```bash
grep -rn "LinearGradient" ios/CoreProp/
```
Expected: **exactly one hit**, the P&L directional fill. Any other hit is an unflattened card.

- [ ] **Step 3: No inline hexes, no `text4` on readable text**

```bash
grep -rn "Color(hex: 0x" ios/CoreProp/ | grep -v "Theme.swift"     # expect none
grep -rn "Theme.text4" ios/CoreProp/                                # expect only decorative glyphs
```

For the second, walk each remaining hit and confirm it is a glyph or a disabled state, not a label: the `EmptyStateView` icon, `SlipView`'s disabled EV, `AnalyticsView`'s dashed rules, `BetDetailView`'s em dash. The three label sites must be gone.

- [ ] **Step 4: No iOS 17+ API introduced**

```bash
grep -rnE "ContentUnavailableView|@Observable|\.contentMargins|\.scrollTargetBehavior|\.containerRelativeFrame|\.visualEffect|\.symbolEffect|\.chartScrollableAxes|onChange\(of:[^)]*\)\s*\{\s*[a-zA-Z_]+\s*,\s*[a-zA-Z_]+\s+in" ios/CoreProp/
```
Expected: **no output**. The deployment target is iOS 16 and this error appears only in CI.

- [ ] **Step 5: No file outside scope touched**

```bash
git diff --name-only <phase-4-base>..HEAD | grep -vE "^ios/CoreProp/|^docs/"
```
Expected: no output. In particular nothing under `ios/CorePropKit/`, `ios/CorePropTests/`, `ios/project.yml`, or `web/`.

- [ ] **Step 6: Get CI to actually run**

This is the only real compile gate, and **`ios.yml` filters on `paths: ["ios/**"]` AND `branches: [main]`** — a push to a feature branch runs nothing. Either open a PR to `main`, or fire `workflow_dispatch` against the branch:

```bash
gh workflow run ios.yml --ref <branch>
gh run watch <id> --exit-status
```

If the token lacks `workflow` scope, a PR is the fallback. **Do not declare Phase 4 done on local gates alone** — 15 of 31 files are only ever compiled here.

- [ ] **Step 7: Report what could not be verified**

Be explicit in the report: there is no simulator and no screenshot on this machine, so **nothing in this phase has been seen rendered.** List what that leaves unverified — the flattened card's actual weight, the 34pt chip against its button, the outcome fills' strength, the step-shaped P&L line, the focus ring, and every spacing change. A human with Xcode should review those.

---

## Definition of done (Phase 4)

- Gate 1 clean on all 31 files; Gate 2 at 109 passed; Gate 3 clean on all 16.
- `grep LinearGradient ios/CoreProp/` → exactly one hit (the directional P&L fill).
- No `Color(hex: 0x…)` outside `Theme.swift`.
- `Theme.text4` only on decorative glyphs; the three label sites fixed.
- No iOS 17+ API.
- The radius, spacing and elevation scales exist and the legacy radius aliases still resolve for all 10 call sites.
- `cpCard` is flat with a `--hair` stroke; `StatTile`'s label is no longer uppercase.
- The P&L line and area are directional with `.stepEnd`; the calibration chart is untouched.
- Backtest cards have per-outcome fills, `--r-lg`, and push on its own colour.
- **iOS CI green** via PR or `workflow_dispatch`.
- Nothing under `ios/CorePropKit/` modified.

## Deferred, with reasons

- **`DataAgePill`'s freshness thresholds.** Web encodes staleness in three colours plus a `live` state; iOS's dot is always green, so it reads fresh over stale data. Fixing it is logic, not styling.
- **The true% colour ramp.** SwiftUI has no OKLCH; porting means hand-writing a conversion, and flattening web's is a colour change. Explicitly accepted in the spec.
- **`SectionHeader` adoption.** 11 screens inline their own section label; unifying them is a refactor.
- **The calibration chart's encodings** (fixed 0-100% domain, sample-scaled dots, overconfidence red, break-even guide). Data encodings, not visuals.
- **`.cp-input` extraction.** Six duplicated field styles in `AuthView` want a shared component; that is refactoring.
- **The 18pt vs 34px P&L total.** The largest type gap in the app; raising it needs a rendered check nobody here can do.

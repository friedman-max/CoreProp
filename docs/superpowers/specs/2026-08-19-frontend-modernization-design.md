# Frontend modernization — design

**Date:** 2026-08-19
**Status:** Approved (design), ready for implementation planning
**Surfaces:** web (`web/static/`, incl. the standalone `extension.html`), iOS app (`ios/CoreProp/`)

## Goal

Make CoreProp look like a modern, professional consumer finance app — the *quality*
of Robinhood's design (airy layout, generous spacing, uniform radii, calm
typography, flat surfaces), applied consistently across the website and the iOS
app.

## Non-goals

- **No functional change.** No handler, API call, state shape, query, or
  business rule changes. Same data, same actions, same navigation.
- **No color-scheme change.** The user explicitly clarified that "like
  Robinhood" means the modern design quality, *not* Robinhood's green palette.
  The existing blue accent and all semantic colors stay — including value→color
  encodings such as `TruePct`'s green heat ramp.
- **No copy change.** Marketing claims and all product copy stay byte-identical.
- No new features, no new screens, no routing changes, no dependency additions.

## Decisions locked during brainstorming

| Decision | Choice | Rationale |
|---|---|---|
| Core screen structure | **Airy list** (feed-style rows, not a dense table) | Modern feel; also matches how the iOS `BetRow` already renders |
| Accent color | **Keep the current blue** (`--primary #1E6FB0`) | User: "like Robinhood" = design quality, not color. Avoids the green brand/semantic clash and keeps existing contrast math valid |
| Scope | **Everything** — all app screens + marketing, web + iOS | One cohesive system, delivered in reviewable phases. "All app screens" means the *reachable* ones: Sections 3, 4 and 4a name the complete set of web files this design edits (`ev-page.jsx`, `page-boards.jsx`, `page-backtest.jsx`, `page-analytics.jsx`, `components.jsx`, `landing.jsx`, `pricing.jsx`, `index.html`, `extension.html`). See constraint 8 for the orphaned files that are excluded |
| Implementation approach | **A — token-first, then targeted components** | Effectively all app CSS lives in `index.html`'s `<style>` — there is no `.css` file anywhere in the repo — so Phase 1 needs no JSX edit and no rebuild (constraint 7 lists the four exceptions and which phase each lands in). Note the starting point honestly: **colors** are tokenized, but **geometry, elevation and spacing are not** — only 6 of 110 `border-radius:` declarations read `var(--radius*)` (all six are `.lp-*` landing rules), only 2 of 26 `box-shadow:` declarations read `var(--shadow-card)` (`.cp-modal`, `.pp-card`), and no spacing token exists at all. Phase 1 therefore *introduces* the tokens **and rewrites the ~130 hardcoded radius/shadow declarations plus the row and panel padding it re-bases**. Editing `:root` alone changes almost nothing on screen |
| Boards screens | **Stay tables** | They exist for dense side-by-side book comparison; cards would hurt scanability |

## Constraints that must hold (from CLAUDE.md — breaking these is a regression)

1. **`--primary` (`#1E6FB0`) is fill/border only; `--primary-2` (`#6FBCEC`) is
   text/icon only.** White on `--primary` clears AA (5.3:1). Never put white on
   `--primary-2`. Hover/pressed states **darken** (`#195F97`), never lighten.
2. **`--text-4` (`#4a4a59`, 2.3:1) is decorative/disabled glyphs only** — never
   text a user reads. Readable muted text is `--text-3`.
3. **No gradients on accent surfaces, no blurred decorative orbs, no
   gradient-clipped text.** Neutral grey card gradients were permitted, but this
   design removes them in favor of flat surfaces.
4. **`TWEAK_DEFAULTS.accent` in `app-main.jsx` must equal `--primary`.** An
   effect writes it back as an *inline* style on `document.documentElement`,
   which beats the stylesheet. Blue is unchanged here, so this value stays
   `#1E6FB0` — but any accent edit must update `index.html`'s `:root`,
   `app-main.jsx` **and** the committed `dist/app-main.js`.
5. **The `.jsx` → `dist/` build contract.** Render's build env is pip-only.
   `web/static/dist/*.js` is committed and served. Any `.jsx` edit requires
   `./build.sh` + committing `dist/` + `index.html`/`sw.js` cache-bust stamps, or
   production silently gets a no-op. `build.sh`'s `FILES` order must match the
   `<script>` order in `index.html`.
6. **Payout tables are mirrored in five places** and must stay identical. This
   design does not change them; `test_payout_table_mirror.py` enforces it.
7. **Almost all CSS lives in `index.html`'s single `<style>` block — but not all
   of it, and one exception is not CSS-only.** There is no `.css` file anywhere
   in the repo, so token/CSS-only changes to `index.html` need no rebuild. Four
   other CSS sources exist and each needs a decision:

   - **`web/static/app-main.jsx:256-262`** builds a `<style>` element at runtime
     and `document.head.appendChild`s it. It ships to every user, and because it
     is appended *after* `index.html`'s block it wins specificity ties. It holds
     exactly three rules:

     ```
     .density-compact .ev-row{padding-top:9px;padding-bottom:9px;font-size:13px}
     .density-compact .ev-row-data{font-size:12.5px}
     .app:not(.tint-on) .ev-row-data:hover{background:rgba(255,255,255,.025)}
     ```

     Three consequences. (a) **The hover rule is live for every user and already
     wins.** `TWEAK_DEFAULTS.tableTint` is `"off"`, so `.app` never gets
     `.tint-on` and `.app:not(.tint-on)` always matches; at (0,4,0) it outranks
     `index.html:716` `.ev-row-data:hover{background:rgba(30,111,176,.05)}`
     (0,2,0). The blue +EV row hover in `index.html` is already dead code —
     editing it changes nothing on screen, and any real hover restyle is a
     `.jsx` edit and therefore **Phase 2**. (b) **The `!important` on the
     logged/selected row backgrounds is load-bearing** against this injected
     rule — see Section 2. (c) The `.density-compact` rules carry the pre-scale
     9px row padding and are also Phase 2. `app-main.jsx:185` puts the tweak
     classes on the root element of every page
     (`"app density-" + t.density + (tableTint==="on" ? " tint-on" : "") + (showHalo ? "" : " no-halo")`),
     so the `.ev-row` / `.ev-row-data` class names must survive Phase 2's row
     rework or these rules silently go dead. No action is needed for
     `showHalo` / `no-halo`: `index.html:185-192` already documents it as a
     no-op for the wordmark, and this design removes no logo glow.
   - **`web/static/tweaks-panel.jsx:49-157`** (`__TWEAKS_STYLE`, rendered at
     line 259) styles only the `.twk-*` dev-panel chrome in a deliberately
     off-palette light theme, and `TweaksPanel` returns `null` until a
     prototyping host posts `__activate_edit_mode`, so it never mounts in
     production. **Out of scope — do not touch**, and exempt it from the new CSS
     guard test (its legitimate `backdrop-filter: blur(24px)` would
     false-positive a blurred-orb check).
   - **`web/static/extension.html`** is served at `GET /extension`
     (`web/app.py::extension_page`, line 1971) and linked from the Backtest tab
     via `.bt-slip-place-install` (`page-backtest.jsx:700`). It is deliberately
     standalone — no React, no shared stylesheet, so it still renders for an
     expired session — and therefore *restates* the palette in its own `:root`
     under different names (`--bg`, `--panel`, `--line`, `--fg`, `--muted`,
     `--accent: #6FBCEC`) with its own radii (10 / 9 / 5 / 4px). Its own comment
     already says "Keep these values in step with those tokens," and no test
     enforces that today. It is a third token mirror alongside `index.html` and
     `Theme.swift`. **In scope** (Section 4a) — plain static HTML, so it needs no
     rebuild, but Phase 1's `index.html` edits do not reach it and it needs a
     parallel hand-edit.
   - **`web/static/analytics-preview.html`** is a standalone dev harness for
     `dist/page-analytics.js`, reachable only through the `/static` mount, linked
     from nowhere, and already drifted (it carries the pre-blue
     `--primary:#6366F1` / `--text-3:#7a7a8b`) plus its own copies of
     `.an-panel`, `.bt-card` and `.pnl-range-btn.is-custom.is-on`. **Explicitly
     out of scope.** Two consequences accepted knowingly: the new CSS guard test
     scopes to `index.html`, so this file's accent gradient is deliberately
     uncovered; and if Phase 2 renames Analytics classes the harness stops
     rendering correctly until its CSS is re-copied.

   So "token/CSS-only changes need no rebuild" holds for `index.html` and
   `extension.html`, and **not** for the `app-main.jsx` injected block.
8. **`web/static/auth-page.jsx` is orphaned and out of scope.** It defines
   `AuthPage` / `CheckoutRedirect` / `WelcomePage`, but it is not in `build.sh`'s
   `FILES` array, not in `index.html`'s `<script>` list, has no `.au*` CSS
   anywhere, and nothing routes to it; `dist/auth-page.js` is a stale one-off
   artifact `build.sh` never regenerates. The live auth UI is `AuthModal` in
   `components.jsx`, and that is the only auth surface this design touches. Do
   not restyle `auth-page.jsx` and do not add `.au*` CSS for it. **`build.sh`'s
   `FILES` array and `index.html`'s `<script>` list must keep exactly their
   current 10 entries in their current order** — constraint 5's ordering rule
   governs that set, not every `.jsx` on disk, and adding an entry is a
   functional change, not a visual one.

## Section 1 — Token system

The foundation; everything inherits from it. Defined in `:root`
(`web/static/index.html`), mirrored into `ios/CoreProp/Theme/Theme.swift`, and
hand-mirrored under different names in `web/static/extension.html` (constraint 7).

### Radius scale

`index.html` today declares **110 `border-radius` values across 18 distinct
standalone sizes** — 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 22, 24, 25,
26px — plus `50%`, `999px`, explicit `0` resets, the two `var(--radius*)` aliases,
and two bottom-only shorthands (`0 0 13px 13px`, `0 0 14px 14px`) in the mobile
media queries. The new scale is five tokens:

| Token | Value | Used for |
|---|---|---|
| `--r-xl` | 20px | the two marketing hero cards only (`.pp-card`, `.lp-cta-card`) |
| `--r-lg` | 16px | cards, panels, table wrappers, modals |
| `--r-md` | 12px | inputs, inner tiles, menus, row action buttons |
| `--r-sm` | 8px | small chips, badges, skeletons, icon buttons |
| `--r-pill` | 999px | buttons, filter chips |

`20px` is a new value — nothing uses it today.

Every existing value maps as follows. The **value** column is exhaustive — every
size in the file has exactly one target, so after Phase 1 no literal radius
remains except the exemptions called out below. The selector lists name the
largest groups and the cases where the mapping is non-obvious; they are not a
complete 110-line inventory. Two easy-to-miss sites: the global
`:where(button,a,summary,input,select,[tabindex]):focus-visible` fallback radius
(L137, 6px → `--r-sm`) applies to every focusable element on the page, and the
compact-slip block at L1297–1392 carries its own 4/5/6/7px badge radii that the
base `.bt-*` rules do not cover.

| Today | New | Notes |
|---|---|---|
| 5, 6, 7, 8, 9px | `--r-sm` (8px) | ~40 declarations, the bulk of the migration: `.ev-add-btn`, `.ev-check`, `.ev-leg-i`, `.ev-leg-x`, `.ev-empty-slip`, `.ev-sharp`, `.ev-stepper`, `.ev-clear`, `.bd-odds.is-best`, `.bd-pag-btn`, `.bd-clear`, `.bt-slip-badge`, `.bt-leg-i`, `.bt-leg-actual`, `.bt-leg-side`, `.bt-slip-del`, `.bt-slip-ev`, `.obs-pill`, `.obs-mult-cell`, `.cp-link`, `.cp-skel`, `.cp-menu-item`, `.cp-modal-x`, `.cp-seg-btn` (9px), `.cp-book` (4px → 8px; it is a text badge, not a bar), `.pp-book-logo`, `.pp-pay-chip`, `.pnl-range-btn`, `.lp-bk-pill`, `.lp-sk`, `.lp-game-ctap-x`, and the global `:focus-visible` fallback. The 5/6/7px ones grow by 1–3px — **that is an intended, visible change on roughly 30 elements 15–24px tall**, not drift, and it is the single most reviewable part of the radius pass: check the book tags, leg-index squares and pagination buttons in the before/after captures |
| 10, 12px | `--r-md` (12px) | inputs, inner tiles, segmented controls: `.cp-menu`, `.cp-seg`, `.cp-input`, `.cp-btn-save`, `.pnl-custom`, `.bd-filters`, `.bd-tbl-wrap`, `.bt-card`, `.pp-faq details`, `.cal-curves` (if the dead rule is kept — see Flat surfaces), `.lp-bk-row`, and `.lp-vig` via the `--radius-sm` alias |
| 14, 16, 18px | `--r-lg` (16px) | cards, panels, table wrappers, modals: `.ev-filters`, `.ev-table`, `.bt-slip`, `.an-panel`, `.cp-modal` (18px → 16px), and four of the five `var(--radius)` consumers (`.lp-books`, `.lp-cov-grid`, `.lp-step`, `.lp-method-card`) |
| 24, 25px | `--r-xl` (20px) | `.pp-card` (24px, the pricing hero) and `.pp-card-glow` (25px, derived — see below), **plus `.lp-cta-card`**, which is explicitly promoted out of the `var(--radius)` alias group (14px → 20px) because it is the landing page's hero CTA card and the largest surface on that page. `--r-xl` has exactly these three consumers. It is **not** the minigame card |
| 999px | `--r-pill` | value unchanged — 9 declarations today (`.cp-btn` L116, `.cp-tab` L200, `.pp-toggle`/`.pp-tg-btn`/`.pp-save`/`.pp-card-trial`, `.bd-badge`, plus `.lp-game-evchip`/`.lp-game-minbar`, the last two exempt as minigame rules) |
| `50%` | unchanged | all true circles/squares: `.cp-avatar`, `.pp-check`, `.lp-why-n`, `.lp-step-n`, `.lp-game-swap`, `.bt-del-icon`, the minigame dots and `.lp-game-flash`. `50%` and `999px` render identically on a square, so leave them as `50%` |
| `0` | unchanged | deliberate square corners |

**Per-element assignments override the value mapping.** Where Section 2 or 3
names a specific element's token, that wins. The one place it changes an outcome:
Section 2 puts buttons and filter chips on the pill shape, so `.ev-chip` and
`.bd-chip` (both 8px today) take `--r-pill`, not `--r-sm`.

**Out of the scale — leave these literal.** Radii of 1–4px on decorative bars,
keys and swatches whose short dimension is 3–12px, where an 8px radius would
round them into lozenges: `.lp-game-confetti` (1px), `.lp-vig-key` (2px, 9×9),
`.cal-legend-item i` (2px, 18×3), `.ev-legend-swatch` (3px, 24×12),
`.lp-books-bartrack` (4px, 12px tall), `.obs-heat-v` (4px), `.lp-foot-nav a`
(4px, focus affordance only).

**Three radii are *derived* from another element's radius and must move in
lockstep with it.** None is a free-standing value in the table above, and all
three read as a rendering bug if the parent moves alone. Express each as
`calc()`/`var()` off the parent token, not as a new hardcoded number, so the
coupling survives the next scale change:

- `.pp-card-glow` (`index.html:848`) is `inset:-1px` over `.pp-card` (`:846`) and
  hardcodes `25px` = the card's `24px` + 1px. It is rendered
  (`pricing.jsx:122`) and forms the card's visible outer outline. It becomes
  `border-radius:calc(var(--r-xl) + 1px)`, or the two rings diverge by 5px at
  each corner and read as a double-outline artifact.
- `.ev-slip-toggle` in the `max-width:820px` block (`index.html:1464`) hardcodes
  `0 0 13px 13px` = `.ev-filters`' `14px` **minus its 1px border**, because the
  toggle bleeds edge-to-edge inside the tile (`margin:4px -16px 0`,
  `width:calc(100% + 32px)`). It becomes
  `0 0 calc(var(--r-lg) - 1px) calc(var(--r-lg) - 1px)`.
- `.ev-slip.ev-slip-mobile` (`index.html:1478`) hardcodes `0 0 14px 14px` to
  match `.ev-filters` *exactly* — it is a bordered sibling, not a child. It
  becomes `0 0 var(--r-lg) var(--r-lg)`.

Two related traps: `.ev-filters` uses a raw `14px`, not `var(--radius)`, so it
needs explicit retokenizing; and because `--radius` is `14px` today, re-pointing
that alias at `--r-lg` (16px) is itself what desynchronizes the two `.ev-*`
values above. No test covers radii, and the mobile mismatch is only visible with
the slip toggle in both its closed and open states at ≤820px — both states are on
the visual-review checklist in Section 6.

**Exemption — the hero minigame.** `.lp-game-*` lives in `index.html`'s
`<style>` (~L453–606), **not** in `landing.jsx`, so this exemption applies in
**Phase 1**. The card is a deliberate near-clone of the PrizePicks large card,
and the comment at `index.html:453-461` lists the measured PP token set as
"#121320 card / #050614 stage / #2C2C39 hairlines / **22px radius** / #6EFF00
selected / #FF4A4A loss / #93939C + #FBF9FF text" — the mimicry includes the
geometry. So the minigame is excluded from the radius scale, the flat-surface
pass and the glow removal, and keeps its values verbatim: **22px** on
`.lp-game-card` (L493), `.lp-game-wash` (L559) and `.lp-game-ctap` (L601);
**26px** on `.lp-game-stagebg` (L479); 7px on `.lp-game-mute` (L466); 14px on
`.lp-game-actbtn` (L575); plus the green idle-nudge inset glow
(`.lp-game-btns.is-idle .lp-game-btn::after`, L539-540) and the
`.lp-game-flash` radial-gradient disc (L557). Two reasons the geometry is
load-bearing: the 26px stage sits deliberately 4px outside the 22px card so the
stage padding reads as a concentric frame, and `.lp-game-wash`'s `inset:0` only
aligns because it repeats the card's 22px exactly — so those four declarations
change together or not at all, and *not at all* is the choice here. Also left
literal: the coupled 16px photo pair `.lp-game-photo-fb` (L506) and
`.lp-sk-photo` (L592), which must always match each other (the skeleton stands in
for the photo). Their 16px happens to equal `--r-lg`, so this is a
value-neutral exemption kept for rule simplicity: nothing `lp-game-*` is
retokenized. The minigame's generic skeleton and close button (`.lp-sk` L583,
`.lp-game-ctap-x` L602, both 8px) are the only exceptions and do move to
`--r-sm`.

The existing `--radius` (14px) / `--radius-sm` (10px) / `--radius-xs` (8px,
currently unused) names are kept as aliases pointing at `--r-lg` / `--r-md` /
`--r-sm` so no rule breaks mid-migration — but only **6 of the 110** radius
declarations reference them today, and all six are landing rules: `.lp-vig`
(L348, `--radius-sm`), `.lp-books` (L359), `.lp-cov-grid` (L403), `.lp-step`
(L418), `.lp-method-card` (L427), `.lp-cta-card` (L433). The other 104 are
literals and must each be rewritten to a token; the aliases prevent breakage in
those six rules only. Note this is source compatibility, not visual identity: four
of the `var(--radius)` sites move 14px → 16px, `.lp-cta-card` is rewritten
directly to `--r-xl` (14px → 20px) as a hero card, and the one `var(--radius-sm)`
site moves 10px → 12px. Mirror the same three-way alias change in
`ios/CoreProp/Theme/Theme.swift:60-62`, which carries the identical 14/10/8
triple.

### Flat surfaces

Every neutral-grey surface gradient becomes a flat background. This is the single
largest "modern" lever and it is CSS-only. There are **twelve** such rules in
`index.html` across six distinct stop pairs, not one — the list is exhaustive
rather than illustrative, because flattening only the two most common values
leaves ten rules gradient-filled next to flattened neighbours.

| Line | Selector | Current | Becomes |
|---|---|---|---|
| 635 | `.ev-filters` | `180deg,#13131c,#101019` | `var(--card)` |
| 685 | `.ev-table` | `180deg,#13131c,#101019` | `var(--card)` |
| 929 | `.bd-filters` | `180deg,#13131c,#101019` | `var(--card)` |
| 846 | `.pp-card` | `180deg,#15151f,#0f0f17` | `var(--card)` |
| 918 | `.pp-faq details` | `180deg,#13131c,#0f0f17` | `var(--card)` |
| 1000 | `.bt-card` | `180deg,#13131c,#0f0f17` | `var(--card)` |
| 1071 | `.an-panel` | `180deg,#13131c,#0f0f17` | `var(--card)` |
| 968 | `.bd-tbl-wrap` | `180deg,#13131c,#0e0e16` | `var(--card)` — **also** update the `.bd-tbl thead th` sticky-header fill (L970, a hardcoded `#13131c`) to the same value, or the sticky header stops matching its wrapper |
| 1019 | `.bt-slip` | `180deg,#15151f,#0e0e16` | `var(--card)` |
| 775 | `.ev-slip` | `180deg,#13131c,#0e0e16` | `var(--bg-2)` — a recessed sidebar rail beside `.ev-table`; `--card` would merge it into the table |
| 1476 | `.ev-slip.ev-slip-mobile` | `180deg,#13131c,#0e0e16` | `var(--card)` — a **second copy** of the rule inside the mobile `@media` block, whose comment requires the "same surface as the tile" so the drawer reads as falling out of `.ev-filters`. It must match L635, **not** L775. Editing 775 alone breaks the drawer's visual attachment and puts a visible seam across a borderless join on the phone +EV screen |
| 1125 | `.cal-curves` | `180deg,#0c0c14,#0a0a12` | `var(--bg)` — a chart *well* sunk below `.an-panel`, darker than `--bg-2`; `--card` would make it lighter than its own container. No JSX, bundle or HTML references this class today, so treat it as cleanup with no screen to verify (deleting the dead rule is equally acceptable) |

**Elevation caveat.** All six stop pairs currently average *below* `--card`
(`#14141e`), so flattening lightens each card and panel by one shade and removes
its top-to-bottom falloff. That is the intended effect, not a regression.
(`#15151f`, the top stop of `.pp-card` and `.bt-slip`, is a hair *lighter* than
`--card`, so those two lose a little top-edge brightness while still ending up
lighter overall.) It is **not** intended for the two recessed surfaces, which
read as wells and take `--bg-2` / `--bg` instead. The elevation ladder
(`--card` / `--card-2` / `--card-3`) is not redefined and no token hex changes.

**Do not flatten by blanket-replacing every `linear-gradient(180deg,…)`.** These
are state or decorative fills, not neutral card surfaces, and they stay:
`.lp-sk` (583) and `.cp-skel` (1014) shimmer animations; the red logged-row tints
(680, 741, 746, 752 — flattened separately in Phase 2, see Section 2); the
`.bt-slip.is-win/.is-loss/.is-push/.is-pending::before` outcome bars (1021-1024,
which Section 3 explicitly preserves); `.cal-legend-dashed` (1130); the
result-tone tints (1266, 1271, 1276, 1281); and the save/place button state fills
(1207, 1208, 1229, 1230, 1233, 1237, 1240). The minigame's gradients are exempt
per the Radius-scale carve-out. The two accent gradients are removed separately —
see "Removed".

### Spacing scale (new)

There is no spacing scale today, which is why padding drifts: `.ev-leg` is
`9px 10px` (`index.html:812`), `.lp-bk-row` `9px 12px` (386), `.bt-card`
`14px 16px` (1000), `.ev-row-hd` `14px clamp(18px,2vw,24px)` (1192).

```
--s-1:4px  --s-2:8px   --s-3:12px  --s-4:16px  --s-5:20px
--s-6:24px --s-8:32px  --s-10:40px --s-12:48px
```

Plus one derived token, which exists solely so the four sites that must share the
+EV row's horizontal padding cannot drift apart again:

```
--row-px: clamp(var(--s-5), 2vw, var(--s-6));   /* 20px → 24px */
```

**Row padding.** `.ev-row` is **not** 9px. Its base rule is `padding:13px 18px`
(`index.html:697`), and the density-overrides block re-declares it
*unconditionally* — not inside a media query — as `13px clamp(18px,2vw,24px)`
(`index.html:1191`). The override is what renders, so editing only the base rule
ships a no-op; both must move together.

Vertical padding goes `13px → --s-4` (16px), a genuine increase. Horizontal
padding **keeps its fluid clamp**, tokenized as `--row-px` (20–24px, up from
18–24px). It must not collapse to a flat 16px: that would *reduce* horizontal
padding from 24px to 16px at wide viewports, which is the opposite of the airy
goal, and flattening a `clamp()` is a density regression on wide monitors — the
whole reason the density block exists. Final value:
`.ev-row{padding:var(--s-4) var(--row-px)}`.

**Same-edit dependency — do not skip.** Three other rules hardcode the same
horizontal expression and must be re-derived from `--row-px` in the same change,
or selected and logged rows fall out of alignment with the header row:

- `.ev-row-hd` — `14px 18px` (709) and `14px clamp(18px,2vw,24px)` (1192) →
  `var(--s-4) var(--row-px)` in both places. The horizontal value must be
  *identical* to `.ev-row`'s or header labels stop lining up with the body
  columns; the vertical goes 14px → 16px with the row.
- `.ev-row-data.is-sel` (723) — `padding-left:calc(clamp(18px,2vw,24px) - 2px)`
  compensating for its 2px left border → `calc(var(--row-px) - 3px)`, since
  Section 2 takes that bar to 3px.
- `.ev-row-data.is-logged` (743) — `padding-left:calc(clamp(18px,2vw,24px) - 3px)`
  → `calc(var(--row-px) - 3px)`.

The inline comment at `index.html:717-722` records the last time these drifted: a
stale `16px` in exactly this `calc()` shipped a 6px content misalignment at wide
viewports. Update that comment to cite `--row-px` instead of the clamp so the
next editor sees the coupling.

The 9px values elsewhere (`.ev-leg`, `.lp-bk-row`, `.cp-tab`, `.cp-menu-item`,
`.ev-clear`, `.cp-seg-btn`) move to `--s-3`/`--s-4` as their own elements dictate;
none of them is the +EV row. The **only** `.ev-row` rule with 9px padding is
`.density-compact .ev-row{padding-top:9px;padding-bottom:9px;font-size:13px}`,
which lives in the runtime-injected `<style>` in `app-main.jsx:256-262`, not in
`index.html`. It is **Phase 2** (a `.jsx` edit needs `./build.sh` + committed
`dist/`) and it becomes `--s-3` (12px) vertical, keeping a ~4px step below
regular — flattening it to 16px would make compact identical to regular and
silently delete a user-facing setting.

#### Padding is declared two or three times, and the base copy is the one that loses

`index.html` has a "density overrides" block (comment at :1154, rules at
:1165–1192) that re-declares padding at the same specificity as the base rule but
later in source order, so it wins. A "mobile / narrow-screen fixes" block
(:1403–1537) adds a third copy inside `@media` at 900/820/560px. Substituting a
spacing token into the base rule alone is a silent no-op — no error, no visual
change. Every substitution must be applied to all copies:

| Selector | base | density override | `@media` |
|---|---|---|---|
| `.cp-nav` | :171 | :1180 | :1416 (820) |
| `.cp-tab` | :198 | :1182 | :1428 (820) |
| `.ev-main` | :632 | :1170 | :1442, :1536 (900) |
| `.ev-filters` | :635 | :1185 | :1457 (900), :1507 (560) |
| `.ev-row` | :697 | :1191 | :1491 (900) |
| `.ev-row-hd` | :709 | :1192 | — |
| `.ev-slip` | :775 | :1171 | :1474 (900, `.ev-slip-mobile`) |
| `.pp` | :827 | :1176 | — |
| `.bd-page` | :928 | :1168 | :1535 (900) |
| `.bd-filters` | :929 | :1186 | :1507 (560) |
| `.bd-tbl thead th` | :970 | :1189 | — |
| `.bd-tbl tbody td` | :976 | :1190 | — |

The override copies use fluid horizontal padding (`clamp(18px,2vw,24px)` on the
+EV rows, `clamp(22px,2.5vw,40px)` on the page shells, `clamp(22px,2.5vw,28px)`
on `.ev-filters`, `clamp(16px,2vw,22px)` on `.bd-filters`,
`clamp(14px,1.4vw,20px)` on the Boards cells), which a single fixed token cannot
express. Phase 1 keeps the fluid behaviour and
tokenizes the *endpoints* — e.g. `.bd-tbl tbody td{padding:var(--s-3) clamp(var(--s-4),1.4vw,var(--s-5))}`.
Do not flatten a `clamp()` to one token.

The same "declared twice, later wins" hazard applies to the flat-surface work:
`.ev-slip`'s gradient is re-declared at :1476 for the mobile drawer, and the
`.bt-slip-compact.is-win/.is-loss/.is-pending/.is-push` backgrounds and inset
glows that Section 3 flattens live at :1264–1283, not next to the base
`.bt-slip-*` rules.

### Elevation

```
--shadow-card: 0 12px 32px -18px rgba(0,0,0,.7);
--shadow-pop:  0 16px 40px -12px rgba(0,0,0,.6);   /* menus, modals */
--ring:        0 0 0 4px var(--primary-hi);        /* focus */
```

`--ring` is the existing 4px ring from `.cp-input:focus` (`index.html:167`),
tokenized. The two 3px variants — `.ev-auto input:focus-visible ~ .ev-check`
(788) and `.pnl-custom-input:focus` (1121) — consolidate onto it; the 2px
`outline` on the global `:focus-visible` fallback (137) is a different mechanism
and stays.

Drops the `0 2px 0 rgba(255,255,255,.03) inset` white highlight — that inset is
the skeuomorphic tell. It lives in the `--shadow-card` token (`index.html:99`)
**and** hardcoded in nine other rules: `.cp-btn-save` (152),
`.cp-tab.is-active` (205), `.cp-seg-btn.is-active` (243), `.pp-tg-btn.is-on`
(839), `.pnl-range-btn.is-custom.is-on` (1111), `.ev-prefs-save` (1201) and its
`:hover` (1204), `.bt-slip-place` (1223) and its `:hover` (1226). All ten go;
editing the token alone reaches only `.cp-modal` and `.pp-card` and leaves half
the surfaces skeuomorphic. The minigame's green idle-nudge inset (540) and the
semantic 4px left-bar insets on the compact result rows (1267, 1272, 1277, 1282)
are not white highlights and stay.

### Typography

Fonts unchanged (Inter + JetBrains Mono; SF Pro + monospaced-design on iOS).

- Micro-labels: `letter-spacing` `.1em` → `.04em`, and **uppercase is kept only
  for true section headers and table headers**. The all-caps-everywhere idiom is
  the strongest "dated dashboard" signal in the current UI.
- Numeric scale: `.pnl-total` 38px → 34px (`index.html:1091`, Phase 1); stat
  values stay 22px mono; the +EV row hero true% goes to 20px mono via a **scoped
  `.ev-row .cp-tp` rule**, because the shared `.cp-tp` base (14px,
  `index.html:249`) also styles the slip-builder legs (`.ev-leg-pct` → `<TruePct>`,
  `ev-page.jsx:505`) and those stay 14px. That rule ships in **Phase 2** with the
  row restructure: the current 9-column grid's `.65fr` TRUE % track is too narrow
  for 20px mono, so enlarging it before the row is restructured would overflow.
  The true% **color is unchanged** — see Section 2 — so this bullet is size-only.
- All numbers stay monospaced (tabular alignment is load-bearing for odds/%).

### Removed

- **Both accent gradients.** An accent gradient is any gradient whose stops are
  the accent color in *either* notation — `var(--primary*)` or the
  `rgba(30,111,176,…)` / `rgba(111,188,236,…)` literal form. There are exactly
  two in `index.html`, and both already violate the no-accent-gradient rule:
  - `.pnl-range-btn.is-custom.is-on` (1111,
    `linear-gradient(180deg,var(--primary-2),var(--primary))`) → flat
    `var(--primary)` fill, white label, inset white highlight dropped.
  - `.ev-row-data.is-sel` (723,
    `linear-gradient(90deg,rgba(30,111,176,.10),rgba(30,111,176,.02))`) → flat
    `var(--primary-hi)`. That is exactly the selected-state treatment Section 2
    specifies for the airy row, so take the left bar from 2px to 3px and move the
    `padding-left` compensation to `calc(var(--row-px) - 3px)` in the same edit,
    and update the comment above the rule (which still says 2px) rather than
    leaving it stale.

  Both are CSS-only, so **both land in Phase 1** and the guard test passes at the
  Phase 1 boundary; Phase 2's airy row inherits the already-flat selected state
  rather than introducing it. After Phase 1 the accent-gradient count in
  `index.html` is zero. `.ev-row-data.is-logged`'s red gradients (740/745/751)
  are semantic, not accent, and flatten to `--red-hi` in Phase 2 with the rest of
  the row.
- The blue drop-glows on Save/CTA buttons
  (`box-shadow: 0 6px 14px -6px rgba(30,111,176,.55)` and the `-8px`/`.7`
  hover variants) on `.ev-prefs-save` (1201, 1204) and `.bt-slip-place`
  (1223, 1226), plus the green drop-glow on `.cp-btn-save` (152).
- The pricing card-dot glow: `.pp-card-dot` (851,
  `box-shadow:0 0 10px var(--primary)`) keeps its flat `--primary` fill and
  loses the halo.
- The white inset highlight — in `--shadow-card` and in all nine hardcoded
  copies listed under Elevation.
- The accent gradient on iOS's P&L area fill
  (`ios/CoreProp/Features/Account/AnalyticsView.swift:94`,
  `Theme.primary.opacity(0.25) → .clear`) — the same violation on the other
  surface, and the only accent gradient in `ios/`. It is *replaced* by web's
  directional green/red fill, not deleted (Section 5).

### Unchanged

All colors: `--bg`, `--bg-2`, `--card*`, `--hair*`, `--text*`, `--primary*`,
`--green`, `--red`, `--amber`, book tints, league hues, and every result
semantic (hit/miss/push/pending). `--hair` (`rgba(255,255,255,.06)`) and
`--hair-2` (`.10`) keep their values — "lighter hairlines" in Section 3 means
*use the lighter of the two existing tokens*, never re-tint `--hair` itself,
which borders every card on web and is mirrored on iOS. `--text-4`'s value is
unchanged too; only one *usage* of it moves (Section 2, Inputs). The focus ring
is kept and tokenized as `--ring`. `TruePct`'s inline green `oklch` heat ramp is
unchanged.

## Section 2 — Shared components

- **Buttons** — flat fills, pill shape, consistent heights (32 / 38 / 44), no
  glow, hover darkens. **Exception: a button that lives in a filter bar is on the
  34px contract below, not this scale.**
- **Filter-bar controls** — **the 34px height contract is preserved, and it
  applies to every control in a filter bar, not just the chips** (filter bars
  align on it across screens). On the +EV bar that is `.ev-chip` (itself a
  `<button>`), `.cp-input-sm`, and `.ev-stepper` (32px inner buttons + 1px
  borders = 34px); on the Boards bar it is `.bd-f input`, `.bd-f select`,
  `.bd-clear` (a `<button>` at `height:34px; align-self:center`), `.bd-badge`,
  and `.bd-pag`. Chips keep pill shape; active chip = `--primary` fill + white.
  The comments at `index.html:640-644` and `:932-940` record two shipped
  misalignment bugs caused by 3px and 2px drift in exactly these rows — do not
  re-derive any filter-bar control's height from the button scale.
- **Cards** — flat `var(--card)` + hairline + `--shadow-card` at `--r-lg`.
- **Inputs** — `--r-md`, tokenized focus ring (`--ring`), and
  `.cp-input::placeholder` (`index.html:166`) moves `--text-4` → `--text-3`.
  Placeholder text is text the user reads — it is the only hint of what to type
  in the +EV prop search (`ev-page.jsx:559`) and it labels the auth modal's
  Email / Password / Confirm-password fields (`components.jsx:287-289`) — and at
  2.3:1 it is the last remaining violation of constraint 2. The same correction
  was already applied to `.lp-foot-disc` and `.pp-respo`. The `--text-4` token
  *value* is unchanged.
- **Airy row** (new shared primitive — the approved core pattern). All nine
  fields of today's `.ev-row` grid have exactly one destination; none is dropped:
  - `--s-4` vertical / `--row-px` horizontal padding.
  - **Left zone** — *identity line*: player name 15px/600, with the existing `GD`
    green-devil badge inline after it (same text, same green `#16a34a`; its
    hardcoded inline `borderRadius: 6` at `ev-page.jsx:698` becomes `--r-sm`.
    Because that badge is an inline JSX style rather than CSS, it is a Phase 2
    edit). *Meta line*: 12.5px `--text-3`,
    `LEAGUE · Prop · SIDE line · game time`, where game time is
    `fmtGameTime(b.startTime)` (today's `.ev-time`, `ev-page.jsx:710`) kept at
    12px mono `--text-3` with `white-space:nowrap` so it never truncates.
    *Book chips*: 11px mono, existing book tint colors, wrapping.
  - **Right zone** — hero true% 20px mono, **color unchanged**; 30px action
    button at `--r-md`.
  - **The true% keeps its green heat ramp.** `TruePct`
    (`components.jsx:322-331`, used at `ev-page.jsx:705`) sets `color` as an
    *inline* `oklch(${0.72 + t*0.05} ${0.14 + t*0.04} 145)` value ramping across
    the 54–75% range. That is a value→color encoding and is covered by the "all
    semantic colors stay" non-goal, so it is **not** in the Removed list: do not
    put `--primary-2` on the true%, and do not delete `TruePct`'s inline style.
    The hero row changes **size only**. Two mechanics follow: because the color
    is inline, no CSS token can reach it (so Phase 1 cannot affect it either
    way), and the 20px must be scoped `.ev-row .cp-tp` because `.cp-tp` is
    shared with the slip-builder legs (`.ev-leg-pct`, 12.5px) which stay as they
    are.
  - This is the composition iOS `BetRow` already uses — it renders
    `· Fmt.gameTime(start)` at 11px mono `Theme.text3` next to side + line — so
    Section 5's "just needs exact padding/size alignment" holds for both surfaces.
  - **State treatments, flat, with the padding compensation moved in the same
    edit.** Today `.ev-row-data.is-sel` is `border-left:2px solid var(--primary)`
    / `padding-left:calc(clamp(18px,2vw,24px) - 2px)` (723) and `.is-logged` is
    `3px` / `- 3px` (740-744). With both bars at 3px and row padding on
    `--row-px`, all three state rules become:

    ```
    .ev-row-data.is-sel           { border-left:3px solid var(--primary);
                                    background:var(--primary-lo);
                                    padding-left:calc(var(--row-px) - 3px) }
    .ev-row-data.is-logged        { border-left:3px solid var(--red);
                                    background:var(--red-hi) !important;
                                    padding-left:calc(var(--row-px) - 3px) }
    .ev-row-data.is-logged.is-sel { border-left:3px solid var(--red);
                                    background:rgba(239,68,68,.20) !important }
    ```

    `--primary-lo` (`rgba(30,111,176,.10)`) is a token added in Phase 1
    specifically for row-level accent tints. **Do not substitute
    `--primary-hi`** here: at `.22` it is inherited by `.ev-time`'s `--text-3`
    and measures 4.45:1, under the AA floor — `--primary-hi` is for focus rings
    and badges, where an explicit `--primary-2` sits on top. `.10` is exactly
    the most-tinted stop of the gradient being replaced, so the flat fill holds
    the identical 5.03:1 worst case. Enforced by
    `test_primary_hi_never_backs_inherited_text`.

    The hover variants (745-747) flatten the same way — one step deeper alpha, no
    gradient. **Keep the `!important`** on the logged backgrounds: at (0,3,0)
    they would otherwise lose to `app-main.jsx`'s injected
    `.app:not(.tint-on) .ev-row-data:hover` at (0,4,0) (constraint 7), and logged
    rows would fall back to the neutral hover — a hover-only regression no test
    covers. Drop it only after raising specificity to at least four class-level
    selectors. All three states keep today's semantics. The mobile flat-tint
    overrides at 1499-1501 exist only because the horizontally-scrolling
    9-column row hid the gradient's faded end and the left bar; once the airy row
    removes that scroll they are redundant and are deleted rather than re-tinted.
  - `.ev-legend-swatch` (677-681) is a deliberate miniature of the logged row —
    same 3px `--red` border, same fill — so flattening the row flattens the
    swatch: `background:var(--red-hi)` replaces the hardcoded
    `linear-gradient(90deg,rgba(239,68,68,.30),rgba(239,68,68,.10))`, and the
    comment at 674-675 changes "same gradient" to "same flat tint". Section 3's
    "the logged-row legend is preserved" means the legend keeps mirroring the row
    it explains.
- **Stat tiles** — flat, `--r-md`, calmer label, mono 22px value, tone colors
  unchanged.
- **P&L chart** — keeps the step-after curve and directional green/red (already
  the strongest element). More padding, lighter grid, 34px total. The grid lines
  are hardcoded `rgba(255,255,255,.06)` inline in `page-analytics.jsx:442, 524,
  525` rather than reading `--hair`, so "lighter grid" is a Phase 2 JSX edit
  there, not a token change. Chart stays static/non-interactive as implemented in
  `7ad72b6`.

## Section 3 — Per-screen (web)

### +EV Bets (`ev-page.jsx`)

The 9-column CSS grid becomes the airy row. Column mapping, so nothing is lost:
1 PLAYER (+ the `GD` badge) → identity line; 2 LEAGUE, 3 PROP, 4 LINE, 5 SIDE,
8 GAME → the meta line (`LEAGUE · Prop · SIDE line · game time`); 7 BOOK ODDS →
wrapping book chips below it; 6 TRUE % and 9 the add button → the right zone.
The slip sidebar (`.ev-slip`) goes flat on `--bg-2` — a recessed rail, not a card
(see Section 1 → Flat surfaces) — with `--r-md` numbered leg chips and a flat
"Log slip" button. Its mobile drawer copy (`.ev-slip.ev-slip-mobile`) goes flat on
`--card` instead, to keep matching the `.ev-filters` tile it hangs beneath.

**Side benefit:** today's `.ev-row` forces `min-width:620px`, so mobile scrolls
horizontally. Stacked rows remove that entirely, with no data loss (book chips
wrap).

Preserved exactly: filters, sort, min-true-%, the auto-log checkboxes, the
"unlogged first" toggle, the logged-row legend, pagination, slip building, and
the place/log flows.

### Boards (`page-boards.jsx` — Combined / PrizePicks / Sportsbooks)

**Deliberately stay `<table>`s** — they exist to compare many books
side-by-side. Modernized in place: row height ~44px
(`.bd-tbl tbody td` padding `11px 14px` → `var(--s-3) clamp(var(--s-4),1.4vw,var(--s-5))`,
applied to both the base rule at :976 and the density override at :1190); row and
header rules **keep `var(--hair)`** — the table already uses the lighter of the
two hairline tokens, so the grid reads lighter from the taller rows alone, not
from a new border color; sticky header restyled without heavy all-caps tracking
(and its hardcoded `#13131c` fill re-pointed at `var(--card)` alongside
`.bd-tbl-wrap`); flat best-odds pill; neutral hover replacing the blue tint;
`--r-lg` wrapper. Empty cells keep `--text-4` (decorative, allowed). Sorting and
filtering unchanged.

### Backtest (`page-backtest.jsx`)

Slip cards go flat at `--r-lg`, keeping the outcome-colored 3px left bar and
border tint (win green / loss red / push amber / pending blue), dropping the
inset colored glow — including the `.bt-slip-compact.is-*` copies at
`index.html:1264-1283`, which are a separate set of rules from the base
`.bt-slip-*` ones. Roomier leg rows, aligned mono numbers. Stat summary gets flat
tiles with larger gaps. Delete modal and place-slip flow unchanged.

### Analytics (`page-analytics.jsx`)

Flat panels at `--r-lg`, more chart padding, restyled section headers, cleaned
range segmented control (including the accent-gradient "Custom" fix), larger
stat-grid gaps. The chart remains static.

### Account menu (`components.jsx`)

Dropdown restyled with `--shadow-pop` and `--r-md` items. Auth modal inherits
the new card/input tokens (`.cp-modal` 18px → `--r-lg`).

## Section 4 — Marketing (`landing.jsx`, `pricing.jsx`)

Purely visual: tokens applied (flat cards; `--r-xl` on the two hero cards
`.pp-card` and `.lp-cta-card`, `--r-lg` on the rest of the landing cards; spacing
scale; calmer labels), pricing card-dot glow removed (`.pp-card-dot`,
`index.html:851`). The CSS for all of this is
in `index.html`, so it lands in Phase 1; Phase 3 is the `.jsx` markup pass.

**Every claim and all copy stays byte-identical.**
`tests/api_tests/test_landing_claims.py` bans invented statistics, requires
coverage-sourced figures and a computed break-even, and enforces
`coverage.books_noun`. No copy is touched.

The hero minigame is exempt from the entire token pass — **colors *and*
geometry** — because it deliberately mimics the PrizePicks board. Its colors
(`#121320` card, `#050614` stage, `#2C2C39` hairlines, `#6EFF00` selected,
`#FF4A4A` loss, `#93939C` / `#FBF9FF` text) and its clone geometry (22px card /
CTA panel / loss-wash radius, 26px stage radius) both stay. Its CSS lives in
`index.html`'s `<style>`, **not** in `landing.jsx`, so the exemption applies in
**Phase 1** — see Section 1 → Radius scale for the full rule list and the two
alignment reasons the geometry is load-bearing. Phase 3 touches only the
`landing.jsx` / `pricing.jsx` markup; the one PP hex that does appear in
`landing.jsx` (`#FF4A4A`, the loss-X SVG helper) stays too.

## Section 4a — Extension install page (`extension.html`)

The one served surface outside `index.html` (constraint 7). It stays standalone —
that is the point of the page — and stays a hand-maintained mirror; it does
**not** gain a shared stylesheet. Visual pass only:

- Radii adopt the new scale **by value**, since the page cannot read `:root`: the
  `.status` panel (10px) and the `.dl` button (9px) go to 12px (`--r-md`); `code`
  (5px) and the `:focus-visible` fallback (4px) go to 8px (`--r-sm`).
- Its five mirrored roles stay hex-identical to the tokens they shadow, and all
  five already are, so no color edit is needed here: `--bg` = `--bg` (`#0a0a0d`),
  `--panel` = `--card` (`#14141e`), `--fg` = `--text` (`#f4f4f8`),
  `--muted` = `--text-3` (`#8a8a9b`), `--accent` = `--primary-2` (`#6FBCEC`).
  `.dl` keeps dark text (`#04121f`) on `--accent` — it must never become white
  (constraint 1).
- Copy, the status-beacon script, and the download link are untouched.

Phase: **Phase 1** (plain HTML/CSS, no JSX, no rebuild).

## Section 5 — iOS parity

- **`Theme.swift`** — add spacing constants, the radius scale (`rXl` 20 /
  `rLg` 16 / `rMd` 12 / `rSm` 8), shadow tokens, and the numeric type scale.
  **Every color stays hex-identical to web.** As on web, the existing names stay
  as aliases pointing at the new scale so no call site breaks mid-migration:
  `radius` 14 → `rLg` (16), `radiusSm` 10 → `rMd` (12 — 10 is not a step in the
  new scale, and every current `radiusSm` site is an input, inner tile, row
  background or banner, which is the `--r-md` role), `radiusXs` 8 → `rSm` (8,
  unchanged). There is **no** `rPill` token: SwiftUI's `Capsule()` already is the
  `--r-pill` equivalent and stays in use (`FilterChip`, `PrimaryButtonStyle`,
  `GhostButtonStyle`, `LeaguePill`, `DataAgePill`, `BetRow`'s circular action
  button). `cardGradTop` / `cardGradBot` (`Theme.swift:25-26`) are **retained** —
  per the web rule, only the gradient *usage* goes and no hex value changes — but
  their "Neutral card gradient stops (allowed on non-accent surfaces)" comment
  and the matching `CardModifier` doc comment (`Components.swift:6-8`) are
  reworded to say the stops are retained-but-unused, so the docs stop
  contradicting the flat-surface decision.
- **`Components.swift`** — `cpCard` becomes flat `Theme.card` + hairline +
  softer shadow. Its default radius follows the `Theme.radius` alias (now 16), so
  the signature needs no edit. Three call sites pass an explicit radius and move
  onto the scale: `StatTile` (`Components.swift:170`) `radius: 12` → `Theme.rMd`;
  `BetDetailView.swift:77` `radius: 12` → `Theme.rMd`; `AuthView.swift:101`
  `radius: 18` → `Theme.rLg` (18 is off-scale). `FilterChip` keeps its shape and
  its 34pt-equivalent metrics; `PrimaryButtonStyle` is already flat (no change).
- **Screens (complete inventory — every view with visual styling is in scope).**
  In order:
  1. `Features/Bets/BetRow.swift` — already closest to the airy row; exact
     padding/size alignment (`10` vertical / `12` horizontal → the spacing scale;
     true% `Theme.mono(16, .bold)` → 20pt mono).
  2. `Features/Lines/LinesView.swift` (which defines `LineRow`) — row
     padding/labels; the filter bar's `padding(.horizontal, 14)` moves to the
     spacing scale.
  3. `Features/Backtest/SlipCard.swift`, `Features/Slip/SlipView.swift`,
     `Features/Backtest/BacktestView.swift` — flat cards at radius 16, calmer
     `kerning(0.5/0.6)` labels.
  4. `Features/Bets/BetsView.swift` — filter bar (chips keep 34pt), hardcoded
     `14`/`10` paddings → spacing scale, `listRowInsets` aligned to the row
     padding.
  5. `Features/Bets/BetDetailView.swift` — its four `cpCard` blocks inherit
     flat/radius-16; `numberTile`'s `cpCard(radius: 12, padding: 14)` → `rMd` /
     `--s-4`; `TRUE PROB` / `FAIR ODDS` / `EDGE` drop the all-caps +
     `kerning(0.5)` treatment (micro-labels, not section headers).
  6. `Features/Account/AccountView.swift`, `SettingsView.swift`,
     `DeveloperView.swift` — the same `List` + `listRowBackground(Theme.card)`
     idiom; restyle all three together or they visibly diverge inside one tab.
  7. `Features/Account/SubscriptionView.swift`, `NotificationsView.swift` —
     `cpCard` inherits; the `STATUS` / `HOW IT WORKS` labels and the hardcoded
     `9/4` and `16` paddings move to the new label and spacing scales.
  8. `Features/Auth/AuthView.swift` — the iOS counterpart of the web auth modal:
     inherits the new card/input tokens, hardcoded `28/20/12/10` paddings →
     spacing scale, inputs → `rMd`.
  9. `Features/Account/AnalyticsView.swift` — flat panels, calmer section
     headers, plus the P&L parity fix below.

  **Change only via the shared modifiers, no per-file edits expected:**
  `App/MainTabView.swift` (tab structure only, no styling) and `App/RootView.swift`
  / its `SplashView` (`Theme.bg` + wordmark). All remaining Swift files under
  `ios/` are non-UI (`App/*Manager`, `*ViewModel`, `CorePropKit`) and are
  untouched.
- **`AnalyticsView`'s P&L chart is brought to web parity (three divergences):**
  1. **Direction.** Web derives one `lineColor`
     (`last >= 0 ? #22C55E : #EF4444`, `page-analytics.jsx:398`) and uses it for
     the stroke *and* both stops of the area fill (`:435-436, 455`). iOS draws
     `LineMark(...).foregroundStyle(Theme.primary2)`
     (`AnalyticsView.swift:90-91`). Both the `LineMark` and the `AreaMark` take
     the directional color — `Theme.green` / `Theme.red`, already `0x22C55E` /
     `0xEF4444`, hex-identical to web. Reuse the existing `last >= 0` test on
     line 79, which the header total on line 86 already keys off.
  2. **Accent gradient removed.** `AnalyticsView.swift:94` is
     `LinearGradient(colors: [Theme.primary.opacity(0.25), .clear], startPoint: .top, endPoint: .bottom)`
     — an accent gradient, banned by constraint 3 and by `Theme.swift`'s own
     header comment (line 16). It becomes the directional equivalent of web's
     fill: `LinearGradient(colors: [tone.opacity(0.24), .clear], …)`, `tone`
     being the same green/red from (1). It is the only accent gradient in `ios/`;
     the two other `LinearGradient` hits (`Components.swift:16`,
     `SlipCard.swift:33`) are the neutral `cardGradTop`/`cardGradBot` card
     gradients already covered by the flat-surface work.
  3. **Curve interpolation.** Web is explicitly step-after
     (`page-analytics.jsx:386-393`); iOS is `.interpolationMethod(.monotone)` on
     both P&L marks (`AnalyticsView.swift:92, 96`). Both become
     `.interpolationMethod(.stepEnd)`, the Swift Charts equivalent — the step
     shape is semantic (bankroll holds flat between settlements), not decoration.
     The calibration chart lower in the same file (`:134-137`) is a different
     chart with a different meaning and keeps `primary2` + `.monotone`.
- **One divergence accepted, not fixed:** `BetRow.swift:73-74` renders true% as
  `Theme.mono(16, .bold)` + `Theme.primary2`, while web uses `TruePct`'s inline
  green `oklch` heat ramp. Porting the ramp to Swift, or flattening web's ramp to
  blue, would each be a color change the non-goals rule out. Both platforms keep
  what they have; "every color stays hex-identical to web" does not apply to this
  one field.
- **Structure unchanged** — same 5 tabs, same navigation, same features.

## Section 6 — Verification

This change touches no handlers, API calls, or state logic, so much of "nothing
broke" is mechanically enforceable — but the build contract is the part that is
*not*, and the spec is explicit about that below.

- **`pytest` full suite** (378 passing at design time). Two existing guards are
  relevant, and neither is a bundle-freshness guard:
  `test_landing_claims.py` bans invented statistics and inactive-league names in
  `landing.jsx` / `pricing.jsx` **and** in `dist/landing.js` / `dist/pricing.js`;
  `test_payout_table_mirror.py` re-parses the payout tables and break-even
  literals out of `ev-page.jsx` / `page-backtest.jsx` / `page-analytics.jsx`
  **and** `dist/ev-page.js` / `dist/page-backtest.js` / `dist/page-analytics.js`.
  Each checks source and bundle independently; **neither compares `.jsx` to
  `dist/`.** The payout mirror therefore catches a stale bundle only when a
  payout literal itself changed — which this design never does — and
  `test_landing_claims.py` only bans copy patterns, and this design changes no
  copy. A markup-only `.jsx` edit leaves the whole suite green on a stale bundle
  (verified: appending to `ev-page.jsx` without rebuilding still yields
  378 passed). `dist/components.js`, `dist/page-boards.js`, `dist/tweaks-panel.js`,
  `dist/api.js` and `dist/app-main.js` are referenced by no test at all.
- **Build contract** — every `.jsx` change is followed by `./build.sh`, with
  `dist/`, `index.html` and `sw.js` committed together, and `git status` confirmed
  to list a rebuilt bundle for **each** edited file before committing.
- **Bundle-freshness guard (new, two layers — the payout-table test is not one).**
  1. *Stamp consistency (pytest, node-free).* Recompute
     `cat web/static/dist/*.js | shasum | cut -c1-10` and assert it equals every
     `?v=` token on the ten `<script src="/static/dist/…">` tags in `index.html`
     and the `coreprop-shell-<id>` cache name in `sw.js` — eleven places, all
     `8b18457b39` today, so the test starts green. The glob must be all of
     `dist/*.js` (11 files today, including the orphan `auth-page.js`, which
     `build.sh`'s own `cat "$OUT_DIR"/*.js` also hashes) and **not** `build.sh`'s
     10-entry `FILES` array, or the digest will not match. This catches a rebuild
     committed without its stamps, a hand-edited bundle, and a partial `dist/`
     commit — the mixed-version-globals blank page `build.sh`'s cache-bust
     comment describes.
  2. *Skipped rebuild.* No hash of `dist/` can catch a `.jsx` edited with
     `./build.sh` never run, because `dist/` **and** the stamps are both
     unchanged and layer 1 still passes. Catching that requires compiling: add a
     node job to `.github/workflows/tests.yml` that runs `./build.sh` then
     `git diff --exit-code web/static/dist web/static/index.html web/static/sw.js`.
     GitHub runners have node and `build.sh` already pins `esbuild@0.28.1` via
     `npx --yes`. The pytest layer deliberately does not try to compile: CI's
     pytest job is pip-only and `test_payout_table_mirror.py` is dependency-free
     on purpose. Until the node job exists, "`./build.sh` after every `.jsx`
     edit" is discipline, not an enforced invariant.
- **New CSS guard test** (`tests/api_tests/test_css_guards.py`) — reads
  `web/static/index.html`, `web/static/app-main.jsx` (including its
  runtime-injected `<style>` string, since that ships to every user),
  `web/static/dist/app-main.js` and `web/static/extension.html`, parses CSS into
  `selector { declarations }` rules, and asserts three bans plus two mirror
  equalities. Each matcher is defined narrowly on purpose: the naive versions flag
  code this design deliberately keeps, and "make the test pass" would then mean
  deleting it.

  1. **No accent gradients.** Fail any `linear-gradient(` / `radial-gradient(` /
     `conic-gradient(` value whose stop list contains an accent color in any
     notation: `var(--primary)`, `var(--primary-2)`, `var(--primary-hi)`,
     `#1E6FB0`, `#6FBCEC`, `#195F97` (case-insensitive), or an `rgb(`/`rgba(`
     whose leading triple is `30,111,176`, `111,188,236` or `25,95,151`
     (whitespace-insensitive). Both notations are required: `.ev-row-data.is-sel`
     used the literal form, so a `var()`-only pattern would have passed over it.
     Expected count after Phase 1: **0**; it is 2 today, and both are fixed in
     Phase 1 (Section 1 → Removed). Explicitly **not** accent, and therefore
     passing: the neutral-white shimmer gradients (`.lp-sk`, `.cp-skel`), the
     semantic gradients built on `--green` / `--red` / `--amber` and their `rgba`
     forms (`34,197,94` / `239,68,68` / `251,191,36`), and the minigame greens
     (`#6EFF00`, `110,255,0`). Do not widen the regex to catch the semantic ones —
     CLAUDE.md's ban is on accent surfaces only, Phases 2 and 3 flatten the ones
     the visual design reaches, and widening it now makes the test fight the plan.
  2. **No blurred decorative orbs.** Fail a rule that sets `border-radius:50%`
     *and* either `filter:blur(` or a `radial-gradient(` background — that pair is
     how a glow orb is built, with or without a real blur. `backdrop-filter` is
     **out of scope by definition**: the sticky nav (`blur(18px)`, L177) and the
     modal scrim (`blur(8px)`, L226) are legitimate material blurs on full-bleed
     surfaces, and a bare `filter:blur(` substring match would flag both. There is
     no `filter:blur` in the stylesheet today, so this ban starts green as a
     regression guard against re-adding the removed `.lp-orb` discs.
  3. **No gradient-clipped text.** Fail any rule containing
     `background-clip:text` or `-webkit-background-clip:text`. Zero instances
     today; regression guard.
  4. **All three copies of the accent agree.** Parse `--primary` out of
     `index.html`'s `:root`, `TWEAK_DEFAULTS.accent` out of
     `web/static/app-main.jsx`, and the `TWEAK_DEFAULTS` accent literal out of the
     committed `web/static/dist/app-main.js`; fail if any pair disagrees.
     `dist/app-main.js` is what the browser runs and carries its own copy of the
     literal — a `.jsx`-only assertion passes while a stale bundle keeps writing
     the old accent as an inline style on `document.documentElement`, which is
     exactly the failure constraint 4 exists to prevent. The bundle is minified,
     so the key is a bare `accent:` where the `.jsx` has `"accent":`; the matcher
     must accept both. This rule has broken silently before; a test makes it
     permanent.
  5. **`extension.html`'s palette mirror matches.** Parse its `:root` and assert
     its five mirrored roles equal the `index.html` tokens they shadow
     (`--bg`/`--bg`, `--panel`/`--card`, `--fg`/`--text`, `--muted`/`--text-3`,
     `--accent`/`--primary-2`). All five already agree today, so this starts green
     and locks a mirror that is currently maintained by a comment alone. Radii are
     deliberately *not* asserted — the page cannot read `:root`, so its radius
     values are hand-copied numbers (Section 4a), and pinning them would make any
     future scale change fail in two places instead of one.

  **Exemptions, both literal so they cannot silently widen.** Selectors whose
  class list contains the prefix `lp-game-` are exempt from bans 1 and 2 — the
  hero minigame is a deliberate PrizePicks-board mimic (Section 4) and
  `.lp-game-flash` (L557) is exactly a `border-radius:50%` `radial-gradient` glow
  disc that stays. Two files are outside the set: `tweaks-panel.jsx`'s
  `__TWEAKS_STYLE`, excluded as dev-only chrome (its legitimate
  `backdrop-filter: blur(24px)` would false-positive ban 2), and
  `analytics-preview.html`, excluded per constraint 7 — noting that the latter
  leaves that file's duplicate accent gradient deliberately uncovered.

  Bans 1–3 must be green against every file in the set at the end of Phase 1.
- **The CSS guard test is web-only.** `CorePropKitVerify` cannot see
  `ios/CoreProp/`'s view source, so the iOS no-accent-gradient rule is enforced by
  code review against `grep -rn "LinearGradient" ios/CoreProp/` — after Phase 4
  the only hits should be the directional P&L fill and any surviving neutral card
  gradients.
- **Contrast checks** — white on `--primary` ≥ 4.5:1; `--text-3` ≥ 4.5:1 on both
  `--bg` and `--card`; `--text-4` appears only on decorative single-glyph rules —
  `.ev-meta-dot` (·, L672), `.bd-odds-empty` and `.bd-edge-cell` (em dash,
  L993-994), `.bt-slip-del` (✕ with `aria-label`, L1034), `.pnl-custom-arrow`
  (→, L1122), `.obs-heat-cell.is-empty` (L1145) — and on no readable text.
  `.cp-input::placeholder` (L166) is the one current exception and moves to
  `--text-3` in Phase 1, so this check is expected to pass with no waivers.
- **iOS** — `swiftc -parse` on every Swift file, `swift run CorePropKitVerify`
  (109 checks), and the `ios` CI job (`xcodebuild`, green since `ab43658`).
- **Visual review** — before/after captures of each screen at mobile (375px) and
  desktop (1440px) widths, since no test can judge "does it look right." Four
  captures are mandatory because nothing else covers them: the +EV row in plain,
  selected and logged states *with hover* on each; the +EV slip toggle in both
  closed and open states at ≤820px (the derived-radius trap); the true% column
  showing the 54–75% green ramp still varying with the value; and the Boards
  filter bar with every control on the 34px baseline.

## Phasing

Each phase is independently reviewable and revertable.

1. **Phase 1 — tokens & shared CSS (no JSX).** Define the radius/spacing/
   elevation/type tokens, then migrate the ~130 hardcoded radius and shadow
   declarations and the row/panel padding onto them — applying every substitution
   to the base rule, the density-override copy *and* the media-query copy per the
   duplicate-declaration table in Section 1. Flat surfaces (all twelve neutral
   gradients), glow removal, white-inset removal, **both** accent-gradient fixes
   (`.pnl-range-btn.is-custom.is-on` and `.ev-row-data.is-sel`), the
   `.cp-input::placeholder` `--text-4` → `--text-3` contrast fix, the
   `.pnl-total` 38px → 34px step, plus the new CSS guard test and the stamp-
   consistency test. Also apply the radius pass to `extension.html`'s own `:root`
   and rules by value (Section 4a — static HTML, no rebuild). The `.lp-game-*`
   minigame is excluded throughout. This is the largest phase by declaration
   count — a per-rule rewrite, not a token flip — but it carries zero build risk
   and modernizes every React screen plus the extension page at once.

   Three things are deliberately **not** reachable from Phase 1 and are deferred:
   the `density-compact` row padding and the +EV row hover (both in
   `app-main.jsx`'s runtime-injected `<style>`, so compact density is briefly 9px
   against a 16px regular row), and the scoped 20px true% (coupled to the row
   restructure). `tweaks-panel.jsx` and `analytics-preview.html` are out of scope
   entirely.
2. **Phase 2 — web component/markup work (JSX + `./build.sh`).** Airy +EV rows
   (including the `GD` badge's inline `borderRadius`, the logged/selected tint
   flattening, the deletion of the redundant mobile tint overrides, and the
   scoped `.ev-row .cp-tp` 20px), the `app-main.jsx` injected block
   (`density-compact` → `--s-3`, row hover), Boards table refinement, Backtest
   cards, Analytics panels (including the inline grid-line color), account menu.
3. **Phase 3 — marketing.** Landing + pricing visual pass, `landing.jsx` /
   `pricing.jsx` markup only (no copy changes). The minigame's CSS was already
   settled in Phase 1 as exempt.
4. **Phase 4 — iOS.** `Theme.swift` / `Components.swift` tokens and alias
   re-pointing, then the nine screens in Section 5's order, plus the three-part
   P&L chart parity fix.

## Risks

| Risk | Mitigation |
|---|---|
| `TWEAK_DEFAULTS.accent` inline style overrides tokens | Blue is unchanged, so the value stays `#1E6FB0`; the new CSS guard test pins it in `index.html`'s `:root`, `app-main.jsx` *and* `dist/app-main.js` |
| Token edits land on the base rule while a later "density overrides" copy still wins → zero visual change, no error | Section 1's duplicate-declaration table lists every affected selector and line; grep each selector for a second and third declaration before editing, and capture before/after at 1440px and 375px per surface |
| Runtime-injected `app-main.jsx` stylesheet outranks `index.html` for `.ev-row-data` | It is appended to `<head>` last, so it wins ties; grep `createElement("style")` before concluding a rule is reachable. Row-hover and compact-density changes go in `app-main.jsx` (Phase 2), not `index.html`; keep `!important` on the `.is-logged` / `.is-sel` backgrounds when flattening their gradients; verify hover on a plain, a selected and a logged row |
| Derived radii desynchronize from their parents (`.pp-card-glow`, the two mobile `.ev-slip*` rules) → visible 1–5px corner step that reads as a rendering bug | Section 1 expresses all three as `calc()`/`var()` off the parent token; no test covers radii, so the visual checklist requires the slip toggle captured closed *and* open at ≤820px |
| 34px filter-bar height contract broken → filter bars misalign (the 32/38/44 button scale would pull `.bd-clear` off 34px next to the 34px `.bd-badge` / `.bd-pag` / `.bd-f select`) | 34px applies to every filter-bar control including buttons; filter-bar buttons are exempt from the button height scale; visual check on every filter bar |
| Mobile row restructure hides data | Book chips wrap rather than truncate; walk the nine-column mapping in Section 3 and confirm all nine fields — including GAME time and the `GD` badge — render |
| Airy +EV row silently drops the true% green heat ramp | Field-presence checks don't cover color encodings; `TruePct`'s inline `oklch` must survive the row rewrite unchanged, and the before/after capture must show the 54–75% ramp still varying with the value |
| Stale `dist/` ships a no-op to production | Not covered by the existing suite — `test_payout_table_mirror.py` re-reads only the payout/break-even literals in three bundles, which this design leaves alone. Mitigation is (a) `./build.sh` after every `.jsx` edit with `git status` confirming a rebuilt bundle per edited file, committing `dist/` + `index.html` + `sw.js` together; (b) the new stamp-consistency test, which pins all eleven `?v=`/cache-name stamps to the `dist/*.js` digest; (c) the node CI job re-running `./build.sh` + `git diff --exit-code`, the only check that catches a rebuild skipped entirely; and (d) the before/after visual capture, the only check that sees what the browser actually ran |
| Phase 1 sized as a token flip and badly underestimated | The decision table and Phase 1 state the real counts (6 of 110 radii and 2 of 26 shadows are tokenized today); "the CSS doesn't work" after a `:root`-only edit is the expected outcome, not a bug |
| `extension.html` silently keeps the old look and drifts from the palette | Section 4a specifies its by-value radius edits; guard-test assertion 5 pins its five mirrored token values to `index.html`'s |
| Scope creep into functional changes | Any behavior change is out of scope by definition; diffs stay CSS/markup-only. `auth-page.jsx`, `analytics-preview.html` and `tweaks-panel.jsx` are excluded by constraints 7 and 8, and `build.sh`'s `FILES` array keeps its current 10 entries |
| iOS app has no local compile path (no Xcode on the dev box) | `swiftc -parse` + verifier locally; `xcodebuild` in CI is the real gate. The iOS no-accent-gradient rule has no test at all and is enforced by `grep -rn "LinearGradient" ios/CoreProp/` in review |

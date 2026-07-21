# CoreProp Frontend Redesign Audit — "Blue Wave" Direction

Branch: `frontend-redesign/blue-wave-audit`
Scope of this pass: **audit + a testable prototype**, not a full re-skin of the live app. Nothing in `web/app.py`, `index.html`, or any shipped `.jsx` was touched — see [How to test](#5-how-to-test-the-new-design) for why, and what to do next.

## TL;DR

The app doesn't look bad because of one thing — it looks like "AI slop" because it's assembled from the same half-dozen visual clichés every LLM defaults to when asked for a "modern SaaS dark UI," stacked on top of each other: an indigo/purple accent color, blurred gradient orbs, a spinning conic-gradient glow ring, rainbow gradient headline text, and glassmorphism blur. None of that is CoreProp-specific — it's generic template DNA. The good news: it's almost all centralized in CSS custom properties and two reused effect classes, so fixing the token layer + killing 2-3 effect rules fixes most of the marketing surface at once. The in-app utility pages (+EV, Analytics, Backtest) are in better shape — their charts are hand-built SVG, not templated — they mainly just inherit the indigo accent from the shared layer.

There's also a real (non-cosmetic) issue: the homepage ticker and testimonials are **fabricated data**, not illustrative placeholders — flagged in [§4](#4-fabricated-data--integrity-flag).

---

## 1. Why it reads as "AI slop" (root causes)

| # | Pattern | Why it reads as generic |
|---|---|---|
| 1 | **Indigo/purple primary** `#6366F1` | This is *the* default accent color every LLM reaches for when told "modern dark SaaS app." It's on every button, the active nav tab, focus rings, the avatar gradient — it's the dominant color of the app, and it doesn't appear anywhere in the CoreProp logo. |
| 2 | **Blurred gradient "orb" backgrounds** (`filter:blur(80px)` circles) | The single most recognizable AI-template signature. Appears behind the hero *and* the pricing page. |
| 3 | **Spinning conic-gradient glow ring** on cards (`animation:spin 6s linear infinite`) | A decorative effect with no functional purpose — pure "look how fancy this is" chrome. Also on both landing and pricing. |
| 4 | **Rainbow gradient text** on nearly every headline (`background-clip:text`) | Used on the hero H1, section H2s, the logo wordmark, and stat callouts. When *everything* is gradient text, none of it means anything — it stops being an accent and becomes wallpaper. |
| 5 | **Glassmorphism everywhere** (`backdrop-filter:blur`, translucent panels, hairline borders) | Not wrong in isolation, but combined with #1-4 it reads as "default component library," not a brand. |
| 6 | **Non-systematic spacing** (`22px`, `9px`, `13px`, `14px` used interchangeably) | A deliberate design system uses a scale (4/8/12/16/24...). Ad hoc values are what you get when spacing is picked per-component by feel rather than by a system — it reads as slightly "off" even when you can't say why. |
| 7 | **Underused brand color** | `--logo-1 #7EE7F5`, `--logo-2 #3DA9F0`, `--logo-3 #5B6BF0` already exist as tokens in `index.html` and match the actual logo — but they're only used for decorative gradient text. The indigo (#1) does all the real UI work instead. The brand blue is right there and isn't being used as the brand color. |
| 8 | **Fabricated social proof** | See [§4](#4-fabricated-data--integrity-flag) — separate from the visual issues, but it compounds the "this was generated, not built" feeling. |

---

## 2. Where — page by page

### Global (nav, buttons, modal) — `web/static/index.html`
- Primary button gradient uses indigo: `index.html:70-72` (`.cp-btn-primary`)
- Avatar gradient mixes indigo + logo blue inconsistently: `index.html:133` (`.cp-avatar{background:linear-gradient(135deg,#6366F1,#3DA9F0)}`)
- Logo wordmark rendered as gradient text instead of the actual brand mark colors: `index.html:117-120` (`.cp-word`)
- Input focus ring is indigo: `index.html:95` (`.cp-input:focus`)
- Segmented control active state is indigo: `index.html:168` (`.cp-seg-btn.is-active`)

### Homepage / Landing — `web/static/landing.jsx` + `index.html` (`LANDING` block)
This page carries most of the offending patterns:
- Blurred hero orbs: `index.html:196-198` (`.lp-orb`, `.lp-orb-1`, `.lp-orb-2`)
- Spinning conic-gradient glow ring on the hero card: `index.html:218` (`.lp-card-glow::before`)
- Rainbow gradient headline (`<em class="lp-grad">+EV bets</em>`): `landing.jsx:34`, styled at `index.html:212-213`
- Fabricated "live" ticker, fake testimonials, fake user counts — see [§4](#4-fabricated-data--integrity-flag)

### Pricing — `web/static/pricing.jsx` + `index.html`
Same two patterns, copy-pasted from the landing page:
- `pp-bg-orb-1` / `pp-bg-orb-2` — same blurred-orb background
- `pp-card-glow` — same spinning conic border on the plan card

*(Worth noting: because landing and pricing share the same effect classes and the same token layer, fixing the tokens + those two shared classes fixes both pages simultaneously.)*

### +EV tab — `web/static/ev-page.jsx`
Comparatively clean. No orbs/glow/gradient-text here — it's mostly functional cards and hand-built SVG. Main issue is just inherited chrome: the indigo accent on buttons/badges/active states, and JetBrains Mono/Inter via the global tokens. Not a rebuild target, just a re-skin via tokens.

### Analytics — `web/static/page-analytics.jsx`
Charts are custom SVG (`page-analytics.jsx:439-549`) — genuinely good, not templated. No page-specific slop found. Only inherits the global button/nav palette.

### Backtest — `web/static/page-backtest.jsx`
Mostly inherits the shared palette too. One small note: a 🗑 emoji used as a delete icon (`page-backtest.jsx:414`) — minor, but real icon sets (or a plain SVG trash icon) read more intentional than an emoji glyph.

**Bottom line:** ~80% of the "slop" feeling lives in the landing + pricing pages (the two most-copied marketing templates). The actual product surfaces are in decent shape and mainly need the token-level fix to fall in line.

---

## 3. New direction: "Blue Wave"

Grounded in the logo (blue-to-cyan wave/swoosh, no purple anywhere) and the three inspiration sites:

- **Tesla** → confident oversized type, enormous negative space, a near-monochrome palette, motion used sparingly and only when it means something. Nothing competes for attention.
- **OddsJam** → data-first, legible, utilitarian. Real borders instead of blur-glow. Icons instead of emoji. Reads as "a serious tool," not a marketing page pretending to be a tool.
- **Cursor** → dark theme done with restraint: *one* accent color, crisp 1px borders, no glassmorphism stacking, understated hover/focus motion.

### Token changes

| Token | Current | Proposed |
|---|---|---|
| Primary accent | `#6366F1` (indigo) | Retire from UI. Use `--logo-2 #3DA9F0` as the single system accent (buttons, links, focus rings, active states). |
| Secondary accent | — | `--logo-1 #7EE7F5` for highlights/hover only, `--logo-3 #5B6BF0` used sparingly (e.g. a single gradient stop, never solo). |
| Background | `#0a0a0d` (neutral near-black) | `#060910` / `#081019` — a near-black with a blue undertone, so the dark theme itself reads "ocean," not "generic dark mode." |
| Decorative hero bg | Blurred orbs (`filter:blur(80px)` circles) | One SVG wave motif echoing the actual logo swoosh — a signature visual instead of a generic blob. |
| Card border treatment | Spinning conic-gradient glow | Static 1px border, `--logo-2` at low opacity on hover only. No infinite animation. |
| Headline treatment | Gradient text on nearly every H1/H2 | Reserve gradient text for the logo mark only. Headlines are solid `--text`, high contrast, large. |
| Spacing | Ad hoc (9px, 13px, 14px, 22px, ...) | Strict 4px base scale: 4/8/12/16/24/32/48/64. |
| Fonts | Inter + JetBrains Mono | Keep both — they're not the problem, the effects piled around them are. JetBrains Mono for numeric/odds data is actually a good, purposeful choice (OddsJam does the same). |

---

## 4. Fabricated data / integrity flag

Not a styling issue — flagging separately because it's a trust/legal question, not a design one.

- `web/static/data.jsx:27-36` — `TICKER_PLAYS`: a hardcoded array of made-up "live" picks, rendered by the homepage hero card and ticker as if real-time (`landing.jsx` `HeroCard`/`Ticker`, rotating every 2.2s with a "just now" timestamp).
- `landing.jsx` `Testimonials` section — quotes attributed to unnamed users ("Best multi-book comparison tool I've used...") that aren't sourced from real customers.
- Hero eyebrow text: `"2,847 bets scanned today"` and `"4,200+ sharps building edge with CoreProp"` (`landing.jsx:33,55`) — hardcoded numbers, not live counts.
- The colored avatar row (`landing.jsx:50`) is 4 flat-colored circles, not real user avatars.

None of this is disclosed as illustrative. Fabricated testimonials and user metrics on a paid product's marketing page are a real (not just aesthetic) risk — worth fixing regardless of the visual redesign.

---

## 5. How to test the new design

Constraints that shaped this recommendation: no frontend build framework (plain global-scope React, precompiled via `build.sh`/esbuild), all styling centralized in CSS custom properties in `index.html`, single free-tier Render service with no PR preview environments configured in `render.yaml`.

**Built on this branch, ready now:**
1. **`web/static/design-preview.html`** — a standalone, zero-backend, zero-auth mockup of the new direction (nav, hero with a wave motif instead of orbs, a +EV pick card, a pricing card). It doesn't import any app code and isn't linked from anywhere the live app routes to, so it's zero-risk to merge. Open it directly:
   - Locally: `open web/static/design-preview.html` (no server needed), or
   - Via the running app once this branch is deployed/run locally: `/static/design-preview.html`
   - It includes a **live "Current / Blue Wave" toggle** in the top-right corner that swaps the entire page between the current token set and the proposed one, so you can flip back and forth on the exact same layout instead of eyeballing two static screenshots.

**Next steps, in order of effort, once a direction is picked:**
2. Lift the same toggle mechanic into the real `index.html` behind a `?theme=wave` URL param or `localStorage` flag, mapped to a `[data-theme="wave"]` CSS block. Because every color already routes through `:root` custom properties, this is a small diff, not a rewrite — and it lets you A/B the *real* app (+EV, Analytics, Backtest, with real data) instead of a mockup.
3. Run the full app locally (`uvicorn web.app:app --reload` or `python main.py`) to review before merging to `main`.
4. If you want to share a link with someone who won't run the repo locally, the static prototype could go on a free Vercel/Netlify/Render-static deploy — kept separate from the paid backend service rather than adding a second paid Render environment.

---

## 6. Notes on the two "Comment" items (for later, not built in this pass)

**Stripe / signup — already implemented, but the paywall isn't switched on.** `pricing.jsx` already calls `window.cpApi.startCheckout()`, which redirects to Stripe Checkout; `app-main.jsx` already polls billing status after `?checkout=success` returns from Stripe; `stripe>=9.0.0` is in `requirements.txt`. The acquisition *plumbing* exists. But `web/app.py:3377` reads `BILLING_ENFORCE = os.getenv("BILLING_ENFORCE", "false")` — **it defaults to `false`**, and the comment at `app-main.jsx:28-29` confirms "the app stays fully open until billing enforcement is switched on server-side." So today, unless that env var is explicitly set to `true` on Render, checkout works but nothing is actually gated behind payment — anyone can use the full app for free regardless of subscription status. That's worth a deliberate decision (flip the env var + verify gating end-to-end) before or alongside the redesign, since it's the difference between "formalized customer acquisition" and "Stripe button that doesn't gate anything."

**"Yesterday's data" +EV element — the scaffold already exists.** `landing.jsx`'s `YesterdaysWinners` section (`landing.jsx:158+`) is *already* the right shape for this idea — it renders a "Yesterday's Winners" module with a real date header. It's currently backed by the hardcoded `YESTERDAY_WINS` array in `data.jsx` instead of live data. To make it real: add a read-only endpoint (e.g. `GET /api/public/yesterday-results`) serving yesterday's settled +EV picks from the existing pipeline/backtest data, cached once a day, and swap the hardcoded array for a fetch. This is exactly the "give a taste, don't give away today's edge" mechanic you described — safe to expose publicly because it's stale/settled by definition. The `HeroCard`/`Ticker` fabricated-live data from §4 should either be replaced with the same delayed feed or removed, since a rotating "just now" card sitting next to a real "yesterday" module would undercut the real one.

This is backend + routing work, separate from the visual redesign — happy to scope it as its own task once you've picked a direction here.

# CoreProp

A +EV betting workbench. CoreProp pulls PrizePicks projections and matches
them against FanDuel, DraftKings, Pinnacle, and Novig prop markets in real
time, strips the vig out of the book prices to recover fair probabilities,
takes a conservative cross-book consensus, and surfaces the plays where
PrizePicks is paying more than the fair line implies. Every slip you log
resolves itself against ESPN box scores and rolls into a live analytics view.

> Built for personal use. Not a service, not a product, no warranty:
> just a tool for finding edges and proving they're real.

---

## What it does

**Find edges.** Scrapes PrizePicks plus four sportsbooks, fuzzy-matches
players and stat types across them, and de-vigs each book line into a fair
probability. A VWAP-style consensus is taken across books, floored to the
conservative worst-case (min-across-books) devig so a single soft book can't
manufacture a phantom edge. Anything where PrizePicks offers a worse implied
probability than that consensus gets surfaced, sorted by expected value.

**Build slips.** A 2-to-6-leg slip builder computes Power and Flex EV in real
time from the per-leg probabilities, with correlation-aware slip scoring for
same-game stacks and a per-leg break-even readout derived directly from the
PrizePicks payout tables. Green devils (PrizePicks goblins) get their own view,
ranked by hit probability.

**Prove it works.** Every logged slip resolves against ESPN's box scores and
folds into a Brier / log-loss summary, raw and expected hit rates, per-league
and per-prop breakdowns, a cumulative P&L timeline, and a closing-line-value
tracker that tells you whether your entry lines beat the market close.

**Auto-backtest.** Opt in and CoreProp logs its best slips for you every
refresh cycle using your saved slip type, leg count, and minimum-leg
threshold, so the backtest fills itself without manual clicking.

**One-click placement.** A companion Chrome extension picks up a slip you
built in CoreProp and constructs it on PrizePicks for you.

---

## Architecture

```text
.
├─ main.py                   App entrypoint (uvicorn)
├─ config.py                 Env-driven runtime config
├─ render.yaml               Render.com deploy spec + malloc tuning
├─ requirements.txt
│
├─ scrapers/                 One module per source
│  ├─ prizepicks.py
│  ├─ fanduel.py
│  ├─ draftkings.py
│  ├─ pinnacle.py
│  └─ novig.py
│
├─ engine/                   The math
│  ├─ matcher.py             Cross-book fuzzy player/prop matching
│  ├─ devig.py               Strip vig from book prices (power / worst-case)
│  ├─ consensus.py           Cross-book consensus + worst-case floor
│  ├─ ev_calculator.py       Per-leg and slip EV (Power / Flex)
│  ├─ correlation.py         Pairwise leg correlation (hourly refit)
│  ├─ calibration.py         Brier, log-loss, hit rate, CLV, P&L
│  ├─ calibration_map.py     Optional isotonic recalibration (off by default)
│  ├─ clv_checker.py         Closing-line capture
│  ├─ results_checker.py     ESPN result resolution
│  ├─ backtest.py            Slip logging, layered dedup, per-user lock
│  ├─ observatory.py         Global market feed for CLV / diagnostics
│  ├─ database.py            Supabase client (shared connection pool)
│  ├─ persistence.py         State cache + gzip envelope
│  └─ constants.py           Payout tables, break-even, prop normalization
│
├─ web/
│  ├─ app.py                 FastAPI app, routes, APScheduler pipeline
│  ├─ auth.py                Supabase JWT verification (JWKS / HS256)
│  ├─ state.py               Shared in-process state + payload cache
│  ├─ routers/admin.py       Admin / diagnostics endpoints
│  └─ static/                Frontend (React via CDN, in-browser Babel)
│     ├─ index.html          Shell, styles, App bootstrap
│     ├─ api.jsx             Supabase auth + SWR fetch layer
│     ├─ components.jsx      Nav, auth modal, shared UI
│     ├─ ev-page.jsx         +EV Bets tab + slip builder
│     ├─ page-boards.jsx     Combined / PrizePicks / Sportsbooks tabs
│     ├─ page-backtest.jsx   Backtest tab
│     ├─ page-analytics.jsx  Analytics tab
│     ├─ landing.jsx         Marketing landing
│     └─ pricing.jsx         Stripe checkout page
│
├─ coreprop-extension/       Chrome MV3 extension (PrizePicks auto-build)
├─ migration_001.sql … 017.sql   Supabase schema migrations
├─ tests/                    pytest suite (engine + API)
└─ data/                     Local snapshots / artefacts
```

---

## Quick start

```bash
git clone <repo>
cd CoreProp
python -m venv .venv
source .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Apply the SQL migrations against your Supabase project in numeric order
(`migration_001.sql` through `migration_017.sql`), then create a `.env`:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_JWT_SECRET=your-jwt-secret
```

The Supabase keys are the only values without working defaults. Run:

```bash
python main.py
```

Then open <http://127.0.0.1:8000>.

To run against a local instance without touching your production seed or
logging slips, set `DISABLE_PERSISTENCE=true` and `DISABLE_AUTO_BACKTEST=true`.

---

## Configuration

Everything except Supabase has a sensible default in `config.py`. Override in
`.env` or via the dashboard's preferences.

| Variable | Default | Purpose |
|---|---|---|
| `SUPABASE_URL` | — | Project URL (required) |
| `SUPABASE_SERVICE_KEY` | — | Server-side writes (required) |
| `SUPABASE_ANON_KEY` | falls back to service key | Browser auth flow |
| `SUPABASE_JWT_SECRET` | — | Verifies user JWTs server-side (HS256 path) |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | Bind address |
| `HEADLESS` | `false` | Headed browser locally to bypass Cloudflare; headless on servers |
| `REFRESH_INTERVAL_MINUTES` | `15` | Auto-refresh cadence |
| `MIN_INDIVIDUAL_EV_PCT` | `0.01` | Per-leg edge threshold for the +EV view |
| `SCRAPE_ALL_LEAGUES` | `false` | Override per-league flags |
| `LEAGUE_NBA` / `_WNBA` / `_MLB` / `_NHL` / `_NCAAB` | `true` | Per-league enable |
| `DISABLE_PERSISTENCE` | `false` | Don't write scrape state to Supabase (comparison mode) |
| `DISABLE_AUTO_BACKTEST` | `false` | Don't auto-log slips (display-only mode) |
| `BILLING_ENFORCE` | `false` | Gate the app behind an active Stripe subscription |

Stripe billing is optional and stays off until `BILLING_ENFORCE=true` and the
`STRIPE_*` keys are set. Most user-facing settings (slip preferences,
auto-backtest opt-in, minimum leg %, active leagues) live in the UI and
persist per-account via Supabase.

---

## Dashboard

Six tabs across the top:

- **+EV Bets** — matched plays that clear the EV threshold, with a side-by-side
  slip builder, live Power/Flex EV and break-even, Auto-Backtest opt-in, and a
  green-devils view. LOGGED tags mark bets already in your backtest.
- **Combined Lines** — every PrizePicks line annotated with the cross-book
  true odds and each book's price.
- **PrizePicks Lines** — raw projections.
- **Sportsbooks** — raw FanDuel / DraftKings / Pinnacle / Novig markets.
- **Backtest** — your logged slips, slip and leg hit rates, ROI, and CLV.
  Filter by league or result; delete your own slips.
- **Analytics** — Brier, log-loss, calibration buckets, per-league and per-prop
  performance, a touch-scrubbable cumulative P&L chart, and the slip-outcome mix.

---

## API

### Bets and slips
- `GET  /api/bets` — matched +EV bets
- `GET  /api/matched` — all matched props (pre-EV filter)
- `GET  /api/bootstrap/core` — lean first-paint payload (bets + meta)
- `GET  /api/status` — scraper status and timing
- `POST /api/slip` — calculate EV for a set of leg IDs
- `POST /api/slip/auto` — best subset from a candidate set

### Markets
- `GET  /api/prizepicks` · `POST /api/prizepicks/refresh`
- `GET  /api/fanduel`    · `POST /api/fanduel/refresh`
- `GET  /api/draftkings` · `POST /api/draftkings/refresh`
- `GET  /api/pinnacle`   · `POST /api/pinnacle/refresh`

### Backtest and analytics
- `GET    /api/backtest/slips` — paginated logged slips
- `GET    /api/backtest/keys` — minimal logged-bet key index for the UI
- `POST   /api/backtest/add-slip`
- `DELETE /api/backtest/slip/{slip_id}`
- `GET    /api/calibration` — Brier, log-loss, buckets, CLV
- `GET    /api/analytics` — full analytics payload (adds P&L timeline)

### PrizePicks extension
- `POST   /api/pending-slip` — queue a slip for the extension (returns a token)
- `GET    /api/pending-slip` — extension picks up the queued slip by token
- `DELETE /api/pending-slip` — extension clears it after building
- `POST   /api/check-pp-availability` — verify legs are live on PrizePicks

### User and config
- `GET  /api/auth/me` · `GET /api/auth/check-username`
- `GET  /api/config` · `POST /api/config`
- `GET  /api/ui-config`
- `POST /api/user/auto-backtest` · `POST /api/user/slip-prefs`

### Billing (Stripe)
- `GET  /api/billing/config` · `GET /api/billing/status`
- `POST /api/billing/checkout` · `POST /api/billing/portal`
- `POST /api/billing/webhook`

### Admin and system
- `GET  /api/admin/memory` — memory diagnostics
- `POST /api/admin/refit-calibration`
- `GET  /health` · `GET /` — dashboard

---

## How the numbers work

CoreProp's decision probability is a conservative cross-book consensus, not a
learned model. Each book's prices are de-vigged, a VWAP-style consensus is
computed across the books pricing a given side, and the result is floored to
the worst-case (min-across-books) devig so one soft book can't invent an edge.
That number drives both the +EV surface and the slip EV math.

Slip EV uses the exact PrizePicks payout tables (Power all-hit multipliers and
the Flex partial-payout grid) with a Poisson-binomial expansion over the
per-leg probabilities. Same-game stacks are re-scored with a correlation matrix
refit hourly from resolved leg pairs, so correlated legs aren't treated as
independent.

Analytics are diagnostic only. Resolved legs feed Brier / log-loss /
calibration buckets and a closing-line-value tracker, and nothing there feeds
back into the decision number. An optional isotonic recalibration map exists
(`engine/calibration_map.py`) but is disabled by default; the refit runs so the
artefact stays fresh for review, and only applies when
`CALIBRATION_MAP_ENABLED=true`.

---

## Performance notes

The app targets Render's 512 MB free tier, so it is deliberately lean:

- **Pre-serialized payload cache.** The scrape pipeline serializes each dataset
  to JSON bytes once per cycle; GET endpoints return those bytes directly with
  a weak ETag, so requests avoid a per-call `json.dumps` and 304 unchanged polls.
- **Shared Supabase connection pool.** All PostgREST clients (service role and
  every per-request user-scoped client) reuse one keep-alive HTTP/1.1 pool, so
  authenticated requests skip a fresh TLS handshake each call.
- **Lazy tabs + SWR cache.** The frontend loads a lean core payload first, then
  fetches each tab's data on first visit and revalidates in the background.
  Production React builds and origin preconnects speed up first paint.
- **Aggressive GC + malloc tuning.** Per-cycle locals are dropped and a full GC
  is forced after each scrape; `render.yaml` sets the malloc arena/pool tuning
  needed to keep RSS under the cap.

---

## Stack

Python 3.11 · FastAPI · Uvicorn / Gunicorn · APScheduler · curl_cffi · httpx ·
rapidfuzz · PyJWT · pandas · numpy · Stripe · Supabase (Postgres + PostgREST) ·
React 18 (via CDN, in-browser Babel) · Chrome MV3 extension.

Deployed on Render's free tier; `render.yaml` ships the deploy config and the
malloc tuning needed to keep RSS under the 512 MB cap.

---

## Troubleshooting

**No bets showing up.** Check league toggles, drop `MIN_INDIVIDUAL_EV_PCT`, hit
Refresh. The Status card surfaces scraper errors.

**Empty FanDuel or DraftKings.** Try `HEADLESS=false`. The Cloudflare challenge
is easier to clear from a headed browser.

**Backtest results not updating.** Result resolution runs on a schedule; only
completed games resolve. Some prop types (season-long, futures) have no ESPN
coverage and stay pending.

**Analytics empty.** You need resolved slips before anything renders. New
accounts stay empty until logged slips resolve against completed games.

**Signed in but landed on the marketing page.** Fixed: the app now reconciles
the restored Supabase session on load and routes you to the +EV Bets tab.

---

## Disclaimer

For educational and informational purposes. Lines, availability, and legality
vary by jurisdiction. Use at your own risk.

## License

MIT.

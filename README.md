# CoreProp

A +EV betting workbench. CoreProp pulls PrizePicks projections and matches
them against FanDuel, DraftKings, and Pinnacle prop markets in real time,
de-vigs the book prices into fair probabilities, calibrates them against
historical outcomes, and surfaces the slips with a positive expected value.
Logged slips backtest themselves against ESPN results, and a strategy
sandbox replays the full history under any rule set you can describe.

> Built for personal use. Not a service, not a product, no warranty —
> just a tool for finding edges and proving they're real.

---

## What it does

**Find edges.** Scrapes PrizePicks plus three sportsbooks, fuzzy-matches
players and stat types across them, and de-vigs the book lines into a fair
probability. Anything where PrizePicks is offering a worse implied
probability than the consensus fair price gets surfaced.

**Build slips.** A 2-to-6-leg slip builder computes Power and Flex EV in
real time using a hierarchical isotonic calibration of the model's
historical hit rate, with quarter-Kelly sizing recommendations.

**Prove it works.** Every logged slip is resolved against ESPN's box
scores and folded into a calibration plot, a Brier/log-loss summary,
per-league and per-prop breakdowns, and a closing-line-value tracker
that tells you whether your opening lines are beating the market close.

**Try alternative strategies.** The Sandbox tab replays the entire
Market Observatory under whatever filters you set — leagues, stat types,
minimum probability, slip size, Kelly on/off — and shows the cumulative
P&L, drawdown, and rolling ROI of that strategy across resolved history.

---

## Architecture

```text
.
├─ main.py                   App entrypoint
├─ config.py                 Env-driven runtime config
├─ render.yaml               Render.com deploy spec
├─ requirements.txt
│
├─ scrapers/                 One module per source
│  ├─ prizepicks.py
│  ├─ fanduel.py
│  ├─ draftkings.py
│  └─ pinnacle.py
│
├─ engine/                   The math
│  ├─ matcher.py             Cross-book fuzzy player/prop matching
│  ├─ devig.py               Strip vig from book prices
│  ├─ consensus.py           Cross-book consensus probability
│  ├─ ev_calculator.py       Per-leg and slip EV (Power/Flex)
│  ├─ correlation.py         Pairwise correlation from observatory
│  ├─ isotonic_calibration.py    Hierarchical isotonic calibrator
│  ├─ sharpness_calibration.py   Per-book sharpness weights
│  ├─ calibration.py         Brier, log-loss, CLV
│  ├─ clv_checker.py         Closing-line capture
│  ├─ results_checker.py     ESPN result resolution
│  ├─ backtest.py            Slip logging, dedup, per-user lock
│  ├─ strategy_tester.py     Sandbox simulator + threshold sweep
│  ├─ database.py            Supabase client
│  ├─ persistence.py         State caching layer
│  └─ constants.py           EV thresholds, payout tables, prop normalization
│
├─ web/
│  ├─ app.py                 FastAPI app, routes, scheduler
│  ├─ auth.py                Supabase JWT verification
│  └─ static/
│     ├─ index.html
│     ├─ app.js
│     └─ style.css
│
├─ migration_001.sql … 005.sql   Supabase schema migrations
└─ data/                     Local snapshots, calibration state
```

---

## Quick start

```powershell
git clone <repo>
cd CoreProp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Apply the SQL migrations against your Supabase project in numeric order
(`migration_001.sql` → `migration_005.sql`), then create a `.env`:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-service-role-key
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_JWT_SECRET=your-jwt-secret
```

The Supabase keys are the only values without working defaults. Run:

```powershell
python main.py
```

Then open <http://127.0.0.1:8000>.

---

## Configuration

Everything except Supabase has a sensible default in `config.py`. Override
in `.env` or via the dashboard's preferences.

| Variable | Default | Purpose |
|---|---|---|
| `SUPABASE_URL` | — | Project URL (required) |
| `SUPABASE_SERVICE_KEY` | — | Server-side writes (required) |
| `SUPABASE_ANON_KEY` | falls back to service key | Browser auth flow |
| `SUPABASE_JWT_SECRET` | — | Verifies user JWTs server-side |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | Bind address |
| `HEADLESS` | `false` | Headed browser locally to bypass Cloudflare; headless on servers |
| `REFRESH_INTERVAL_MINUTES` | `15` | Auto-refresh cadence |
| `MIN_INDIVIDUAL_EV_PCT` | `0.01` | Per-leg edge threshold for the +EV view |
| `SCRAPE_ALL_LEAGUES` | `false` | Override per-league flags |
| `LEAGUE_NBA` / `_MLB` / `_NHL` / `_NCAAB` / `_SOCCER` | `true` | Per-league enable |

Most user-facing settings (slip preferences, auto-backtest opt-in, EV
threshold) live in the UI and persist per-account via Supabase.

---

## Dashboard

Eight tabs across the top:

- **+EV Bets** — matched plays that clear the EV threshold, with a
  side-by-side slip builder. Auto-Backtest opt-in here.
- **Combined Lines** — every PrizePicks line annotated with the
  cross-book true odds and each book's price.
- **PrizePicks Lines** — raw projections.
- **Sportsbooks** — raw FD/DK/PIN markets, switchable.
- **Backtest** — your logged slips, hit rates, ROI, CLV%. Filter by
  league or result.
- **Analytics** — Brier, log-loss, calibration plot, per-league and
  per-prop performance, cumulative P&L, slip-outcome mix.
- **Observatory** — global market data feeding the calibrator:
  per-league shrinkage, fitted calibration curves, and a per-prop
  hit-rate heatmap by expected-probability band.
- **Sandbox** — strategy simulator. Equity, drawdown, rolling ROI,
  per-stat / per-league / per-hit-count breakdowns, slip log, CSV export.

---

## API

### Bets and slips
- `GET /api/bets` — matched +EV bets
- `GET /api/matched` — all matched props (pre-EV filter)
- `GET /api/status` — scraper status and timing
- `POST /api/slip` — calculate EV for a set of leg IDs
- `POST /api/slip/auto` — best subset from a candidate set

### Markets
- `GET  /api/prizepicks` · `POST /api/prizepicks/refresh`
- `GET  /api/fanduel`    · `POST /api/fanduel/refresh`
- `GET  /api/draftkings` · `POST /api/draftkings/refresh`
- `GET  /api/pinnacle`   · `POST /api/pinnacle/refresh`

### Backtest and analytics
- `GET    /api/backtest/slips` — paginated logged slips
- `GET    /api/backtest/keys` — minimal slip-id index for the UI
- `POST   /api/backtest/add-slip`
- `DELETE /api/backtest/slip/{slip_id}`
- `GET    /api/calibration` — Brier, log-loss, buckets
- `GET    /api/analytics` — full analytics payload
- `GET    /api/observatory` — global resolved-line feed
- `GET    /api/observatory/multipliers` — per-league shrinkage at p=0.60
- `GET    /api/calibration/curves` — fitted hierarchical curves
- `GET    /api/calibration/heatmap` — per-(league, prop, side) heatmap

### Sandbox
- `GET  /api/sandbox/stat-types` — distinct (league, stat) pairs
- `POST /api/sandbox/run` — replay a strategy
- `POST /api/sandbox/optimize` — sweep min-prob thresholds

### User and config
- `GET  /api/auth/me`
- `GET  /api/auth/check-username`
- `GET  /api/config` · `POST /api/config`
- `GET  /api/ui-config`
- `POST /api/user/auto-backtest`
- `POST /api/user/slip-prefs`
- `GET  /api/bootstrap` · `GET /api/bootstrap/core`

### Admin
- `POST /api/admin/refit-calibration`
- `GET  /api/admin/memory`

### System
- `GET /health`
- `GET /` — dashboard

---

## How calibration works

CoreProp doesn't trust raw de-vigged probabilities. It runs a hierarchical
isotonic calibration with three levels — global, per-league, and
per-(league, prop, side) — fit incrementally from outcome data and
closing-line value. Each (league, prop, side) bucket is shrunk toward the
global curve via Bayesian shrinkage so thin buckets pool to the parent
and well-attested ones are allowed to override.

The state is bucket-level sufficient statistics with exponential recency
decay (60-day half-life), refit hourly. State size scales with the number
of distinct buckets, not history depth, so the calibrator stays bounded.

Outcome-only accumulators are tracked separately from CLV signal so the
diagnostic heatmap in the Observatory tab shows true hit rates rather
than CLV-blended values.

---

## Stack

Python 3.11 · FastAPI · Uvicorn · APScheduler · curl_cffi · httpx ·
rapidfuzz · PyJWT · pandas · numpy · Supabase (Postgres + PostgREST) ·
vanilla JS · Chart.js.

Deployed on Render's free tier; `render.yaml` ships the deploy config
and the malloc tuning needed to keep RSS under the 512 MB cap.

---

## Troubleshooting

**No bets showing up.** Check league toggles, drop `MIN_INDIVIDUAL_EV_PCT`,
hit Refresh. The Status card surfaces scraper errors.

**Empty FanDuel or DraftKings.** Try `HEADLESS=false`. The Cloudflare
challenge is easier to clear from a headed browser.

**Backtest results not updating.** Result resolution runs on a schedule;
only completed games are resolved. Some prop types (season-long, futures)
have no ESPN coverage and stay pending.

**Analytics empty.** Calibration needs ~50 resolved legs before the
buckets stop being mostly noise. New deploys take a few hundred legs to
populate everything.

**Observatory heatmap empty.** The per-side bucket format is recent —
on a fresh deploy the in-memory state is empty until the next hourly
refit. The heatmap falls back to a direct database query in that case
so something useful renders immediately.

---

## Disclaimer

For educational and informational purposes. Lines, availability, and
legality vary by jurisdiction. Use at your own risk.

## License

MIT.

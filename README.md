# CoreProp

CoreProp is a local +EV betting dashboard that compares PrizePicks projections against sportsbook prices, matches similar props across books, calculates single-leg and slip-level expected value, logs and backtests your bets, and tracks calibration metrics to validate your predictive accuracy over time.

## Features

**Scraping & Matching:**
- Scrapes PrizePicks projections, FanDuel, DraftKings, and Pinnacle player prop markets via headless browser or API.
- Fuzzy player and stat matching across books to find identical props.
- De-vigs sportsbook prices to estimate fair odds (implied probability).

**EV & Slip Building:**
- Filters bets by individual +EV threshold (configurable).
- Builds multi-leg slips (2–6 legs) with Power/Flex payout calculations.
- Auto-selects best slip subsets using greedy optimization.
- Real-time EV aggregation per slip.

**Backtest & Analytics:**
- Manual slip logging with timestamp and selected legs.
- **Auto-Backtest**: opt-in per-user, builds the best 6-leg slip each refresh with cross-slip exact-leg dedup, per-user serialization (no race-condition duplicates), and a max-3-legs-per-game cap to limit correlation.
- Automatic result checking via ESPN API to resolve pending slips.
- CSV export for historical analysis.
- **Calibration metrics** (Brier score, log-loss, hit-rate) plus hierarchical / isotonic / sharpness calibration backends.
- **Per-league and per-prop performance breakdown** showing actual vs. expected hit rate.
- **Cumulative P&L chart** tracking profit/loss across resolved slips (1-unit stake baseline).
- **Closing line value (CLV) tracking** to measure edge vs. market consensus.

**Strategy Sandbox:**
- Replays historical Market Observatory data under a configurable strategy (leagues, stat types, min true-prob, slip size/type, Kelly on/off).
- Stat-type filter is sourced live from the database and grouped by sport, so chips always match what actually exists.
- **Auto-Optimize** sweeps probability thresholds to find the ROI-maximizing setting.
- Charts: cumulative P&L, drawdown %, rolling ROI + win-rate (windowed).
- Breakdowns: per stat type (per-leg attribution), per league, and per hit-count distribution.
- Slip log with per-leg detail and CSV export.
- Win rate counts only perfect slips (no missed legs) and excludes Kelly $0 entries from the denominator.

**Runtime & Config:**
- Scheduled auto-refresh (default 15 min, configurable).
- Manual refresh trigger via UI or API.
- Runtime config (interval, EV threshold, league toggles) without code edits.
- Fresh data on startup—stale cache (>30 min old) is auto-purged.

## Dashboard

The web UI includes:

- **Bets View**: Matched +EV plays sorted by ROI and volume.
- **Market Feed**: Separate raw tables for PrizePicks, FanDuel, DraftKings, Pinnacle.
- **Slip Builder**: Select legs, auto-calculate EV, build best subsets, log slips to backtest.
- **Backtest Tab**: View logged slips, filter by league/result, check results, download CSV. Toggle Auto-Backtest on/off per user.
- **Analytics Tab**: Calibration summary cards, P&L chart, calibration plot (predicted vs. actual), per-league/prop performance, CLV tracking, slip outcome distribution.
- **Sandbox Tab**: Strategy simulator (see Strategy Sandbox under Features) with equity, drawdown, rolling, and breakdown views.
- **Config Panel**: Adjust refresh interval, EV threshold, and active leagues live.

## Tech Stack

- **Backend**: Python 3.10+, FastAPI, Uvicorn, APScheduler
- **Scraping**: Playwright, httpx, curl_cffi
- **Matching**: rapidfuzz
- **Database**: Supabase (PostgreSQL + PostgREST API)
- **Frontend**: Vanilla JS, Chart.js, responsive CSS

## Project Layout

```text
.
├─ main.py                      # App entrypoint
├─ config.py                    # Runtime config via env vars
├─ requirements.txt
├─ README.md
├─ scrapers/
│  ├─ prizepicks.py            # PrizePicks API scraper
│  ├─ fanduel.py               # FanDuel headless scraper
│  ├─ draftkings.py            # DraftKings headless scraper
│  └─ pinnacle.py              # Pinnacle API scraper
├─ engine/
│  ├─ constants.py             # EV thresholds, payout tables, prop-type normalization
│  ├─ matcher.py               # Cross-book fuzzy matching
│  ├─ ev_calculator.py         # EV per leg and slip (independent + correlated)
│  ├─ correlation.py           # Pairwise correlation matrix from observatory
│  ├─ backtest.py              # Slip logging, dedup, per-user lock
│  ├─ calibration.py           # Brier score, log-loss, CLV
│  ├─ isotonic_calibration.py  # Isotonic-regression calibrator
│  ├─ sharpness_calibration.py # Per-book sharpness fitting
│  ├─ strategy_tester.py       # Sandbox simulator + threshold optimizer
│  ├─ clv_checker.py           # Closing-line capture & finalization
│  ├─ devig.py                 # Remove vig from sportsbook prices
│  ├─ consensus.py             # Cross-book consensus probability
│  ├─ results_checker.py       # ESPN result lookup
│  ├─ database.py              # Supabase client
│  └─ persistence.py           # State caching layer
├─ migration_001.sql           # slips/legs schema
├─ migration_002.sql           # market_observatory schema
├─ migration_003.sql           # per-book sharpness columns
├─ web/
│  ├─ app.py                   # FastAPI app, routes, scheduler
│  └─ static/
│     ├─ index.html            # Single-page app shell
│     ├─ app.js                # Tab logic, fetch handlers, charts
│     └─ style.css             # Responsive dark theme
└─ data/                        # Local scraper snapshots for debug
```

## Quick Start

1. **Clone and set up environment:**

```powershell
git clone <repo>
cd CoreProp
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. **Configure credentials** (create `.env` or edit `config.py`):

```env
# Supabase
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key

# Scraping
HEADLESS=false
REFRESH_INTERVAL_MINUTES=15

# EV filtering
MIN_INDIVIDUAL_EV_PCT=0.01

# Leagues (enable/disable)
SCRAPE_ALL_LEAGUES=false
LEAGUE_NBA=true
LEAGUE_MLB=true
LEAGUE_NHL=true
LEAGUE_NCAAB=true
LEAGUE_SOCCER=true

# Server
HOST=127.0.0.1
PORT=8000
```

3. **Run the app:**

```powershell
python main.py
```

4. **Open the dashboard:**

Open http://127.0.0.1:8000 in your browser.

## Configuration

### Environment Variables

| Variable | Default | Notes |
|----------|---------|-------|
| `SUPABASE_URL` | — | Supabase project URL (required) |
| `SUPABASE_KEY` | — | Supabase anon key (required) |
| `HEADLESS` | `false` | Run Playwright in headless mode (set `true` on servers) |
| `REFRESH_INTERVAL_MINUTES` | `15` | Auto-refresh interval |
| `MIN_INDIVIDUAL_EV_PCT` | `0.01` | Minimum 1% EV to display a bet |
| `SCRAPE_ALL_LEAGUES` | `false` | Override league toggles and scrape all |
| `LEAGUE_NBA` / `LEAGUE_MLB` / etc. | `true` | Enable/disable specific leagues |
| `HOST` | `127.0.0.1` | Server bind address |
| `PORT` | `8000` | Server port |

### Runtime Config (Via UI)

The **Config** tab in the dashboard lets you change:
- **Refresh Interval**: How often the pipeline runs (minutes).
- **EV Threshold**: Minimum +EV% to display a bet.
- **Active Leagues**: Toggle NBA, MLB, NHL, NCAAB on/off without restart.

Changes are applied immediately; no restart needed.

## API Endpoints

### Core Bets

- `GET /api/bets` - Current matched +EV bets (sorted by ROI).
- `GET /api/matched` - All matched props (before EV filtering).
- `GET /api/status` - Scrape status, timing, error messages.
- `POST /api/refresh` - Trigger full pipeline refresh now.
- `POST /api/slip` - Calculate slip EV for selected leg IDs.
- `POST /api/slip/auto` - Find best slip subset from a selection.

### Configuration

- `GET /api/config` - Read current runtime config (interval, EV, leagues).
- `POST /api/config` - Update interval, min EV, league toggles.

### Book Lines

- `GET /api/prizepicks` - Current PrizePicks projections.
- `POST /api/prizepicks/refresh` - Scrape PrizePicks now.
- `GET /api/fanduel` - Current FanDuel props.
- `POST /api/fanduel/refresh` - Scrape FanDuel now.
- `GET /api/draftkings` - Current DraftKings props.
- `POST /api/draftkings/refresh` - Scrape DraftKings now.
- `GET /api/pinnacle` - Current Pinnacle props.
- `POST /api/pinnacle/refresh` - Scrape Pinnacle now.

### Backtest & Analytics

- `GET /api/backtest/latest-slip` - Most recent logged slip.
- `GET /api/backtest/slips` - List logged slips (paginated).
- `POST /api/backtest/add-slip` - Log current bets as a slip.
- `DELETE /api/backtest/slip/{slip_id}` - Delete a logged slip.
- `GET /api/backtest/download-csv` - Download backtest CSV export.
- `POST /api/backtest/check-results` - Resolve pending slips via ESPN.
- `POST /api/user/auto-backtest` - Toggle auto-logging of best 6-leg slip per refresh.
- `GET /api/calibration` - Brier score, log-loss, calibration buckets.
- `GET /api/analytics` - Full analytics: calibration + per-league/prop + P&L chart data + CLV.

### Sandbox

- `GET /api/sandbox/stat-types` - Distinct `(league, stat_type)` pairs from the observatory; powers the dynamic filter chips.
- `POST /api/sandbox/run` - Replay a strategy and return summary, equity curve, drawdown curve, rolling ROI/win-rate, breakdowns, and the full slip log.
- `POST /api/sandbox/optimize` - Sweep min-prob thresholds and return the ROI-maximizing value plus the full sweep results.

### System

- `GET /health` - Health check (always 200 OK).
- `GET /` - Serve the dashboard HTML.

## Backtest & Calibration

### Logging Slips

1. Select legs in the slip builder (or any bet row).
2. Click **Log Slip** in the Backtest tab.
3. The slip is stored in Supabase with timestamp, leg details, and projected EV.

### Checking Results

1. Click **Check Results** in the Backtest tab.
2. The app queries ESPN for completed games and updates `result` (hit/miss/push) for each leg.
3. Slip payout is calculated based on leg results and slip type (Power/Flex).

### Analytics

The **Analytics** tab displays:

- **Calibration Cards**: Brier Score (lower is better, <0.25 beats coin flip), Log-Loss (penalizes confident mispredictions), Hit Rate, Avg Predicted Prob, Delta (actual − expected).
- **P&L Chart**: Cumulative profit/loss per resolved slip (1-unit stake, payout-based).
- **Calibration Plot**: Bubble chart of predicted vs. actual hit rate per probability bucket (50–79%).
- **Per-League Breakdown**: Legs, hits, actual vs. expected, delta.
- **Top Prop Types**: Performance on your most-played prop types.
- **CLV Tracking**: % of legs where your opening line beat the closing line, average %.
- **Slip Outcome Mix**: Donut chart of won / partial / lost / pending slips.

## Troubleshooting

### No bets displayed

- Verify league toggles in UI or `.env`.
- Check that `MIN_INDIVIDUAL_EV_PCT` is not too high (default 0.01 = 1%).
- Click **Refresh** in the UI or call `POST /api/refresh`.
- Check the **Status** card for scraper errors.

### Empty FanDuel or DraftKings

- Set `HEADLESS=false` and retry (headless mode can trigger anti-bot).
- Ensure Playwright browsers are installed: `playwright install`.
- Check browser console (`F12`) for JavaScript errors.

### Stale data on startup

- Data older than 30 minutes in Supabase cache is auto-purged.
- The pipeline runs automatically on startup; wait ~30 sec for fresh data.
- If stuck, click **Refresh** or restart the app.

### Backtest results not updating

- Click **Check Results** to query ESPN for latest outcomes.
- Only completed games are resolved; pending games show as "pending."
- Some props (e.g., season-long) may not have ESPN coverage.

### Analytics tab shows no data

- Check the browser console (`F12`) for errors.
- Ensure at least one slip is resolved (check Backtest tab for status).
- Calibration requires ≥50 resolved legs; new backtests will be sparse at first.

## Performance Notes

- **Startup warmup**: First scrape takes ~30–60 sec (depends on Playwright + sportsbook load).
- **Auto-refresh**: Runs every 15 min by default; adjust `REFRESH_INTERVAL_MINUTES`.
- **Supabase caching**: Snapshots are cached to accelerate cold-start if <30 min old.
- **Sandbox queries**: `/api/sandbox/run` reads all resolved observatory rows for the selected leagues; expect ~1–2 s on a populated database. Stat-types endpoint pages through the table 1k rows at a time.
- **Frontend charts**: Charts.js renders multiple charts + tables; smooth on modern browsers.

## Disclaimer

This tool is for **educational and informational purposes**. Odds, line availability, and props can change quickly and may be restricted by jurisdiction. Use at your own risk. The author is not responsible for betting losses.

## License

MIT

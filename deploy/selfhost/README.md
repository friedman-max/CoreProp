# Self-hosting CoreProp on the Mac behind a Cloudflare Tunnel

Replaces the Render deploy. The app runs as a launchd service on loopback;
`cloudflared` dials out to Cloudflare and serves it on your domain over HTTPS.
No port forwarding, no static IP, and your home IP never appears in DNS.

A bonus that matters for this app specifically: sportsbooks block datacenter IP
ranges far harder than residential ones. PrizePicks already 429s this scraper
occasionally from a home connection — it fares worse from a cloud host.

---

## Before you start: you need a domain on Cloudflare

A **named tunnel** (stable hostname) requires a domain whose nameservers point
at Cloudflare. The old URL, `coreprop.onrender.com`, was Render's subdomain and
does not transfer.

If you don't own one yet, buy any cheap domain and add it to Cloudflare (free
plan is fine) before step 4.

> `cloudflared tunnel --url http://127.0.0.1:8010` works with no domain, but it
> mints a **new random `*.trycloudflare.com` URL on every restart**. That breaks
> the Stripe webhook and the browser extension every time the service bounces,
> so it's fine for a smoke test and unusable for production.

---

## 1. Copy the Stripe keys off Render — do this first

**This is the step that silently costs money if you skip it.**

Your `.env` contains Supabase credentials and scraper settings only. It has
**no Stripe keys** — they exist solely in Render's dashboard. And the access
check fails *open*:

```python
if not BILLING_ENFORCE or not _billing_configured():
    return True          # everybody gets in
```

`_billing_configured()` requires `STRIPE_SECRET_KEY`, `STRIPE_PRICE_MONTHLY`
and `STRIPE_PRICE_YEARLY` to all be present. So a self-hosted instance missing
them serves the full +EV board to **every visitor for free**, even with
`BILLING_ENFORCE=true`, and nothing in the UI will look wrong.

From the Render dashboard (Environment tab), copy into `.env`:

```
STRIPE_SECRET_KEY=
STRIPE_PUBLISHABLE_KEY=
STRIPE_WEBHOOK_SECRET=
STRIPE_PRICE_MONTHLY=
STRIPE_PRICE_YEARLY=
COMP_ACCOUNTS=
```

Copy/paste them — don't retype. The monthly price ID has a known
lowercase-`l` / uppercase-`I` ambiguity that is invisible in most fonts and
produces a checkout that fails only at the final step.

Do this **while the Render service still exists**. Once the project is deleted
those values are unrecoverable.

## 2. Stop the Mac from sleeping

Currently `pmset` reports `sleep 1` — this Mac suspends after one minute on AC.
A sleeping host is an offline site.

```bash
sudo pmset -a sleep 0 && sudo pmset -a autorestart 1
```

`sleep 0` never suspends the system; `autorestart 1` reboots automatically after
a power failure. Display sleep is unaffected — the screen can still turn off.

If FileVault is on, the disk stays locked after an unattended reboot and nothing
starts until someone logs in. Either turn FileVault off or accept that power
cuts need a manual login.

## 3. Install the app service

```bash
mkdir -p ~/Library/Logs/coreprop
cp deploy/selfhost/com.coreprop.server.plist ~/Library/LaunchAgents/
```

Edit `~/Library/LaunchAgents/com.coreprop.server.plist` and set
`PUBLIC_BASE_URL` to `https://your-domain`. Then:

```bash
launchctl load ~/Library/LaunchAgents/com.coreprop.server.plist
```

Verify it came up on loopback:

```bash
curl -s localhost:8010/health && curl -s localhost:8010/api/status
```

`total_bets` should be non-zero within about a minute of the first scrape.

## 4. Create the tunnel

```bash
cloudflared tunnel login
cloudflared tunnel create coreprop
cloudflared tunnel route dns coreprop your-domain.com
```

`create` prints a UUID and a credentials path. Copy the template and fill both
placeholders in:

```bash
cp deploy/selfhost/cloudflared-config.yml ~/.cloudflared/config.yml
```

Set `credentials-file` to the printed UUID path and `hostname` to your domain —
it must match `PUBLIC_BASE_URL` exactly. Then run it as a service:

```bash
mkdir -p ~/Library/Logs/coreprop
cp deploy/selfhost/com.coreprop.tunnel.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.coreprop.tunnel.plist
```

> **Do NOT use `brew services start cloudflared`.** Homebrew's bundled plist
> runs `cloudflared tunnel --url http://127.0.0.1:8010` — a **quick tunnel**.
> It ignores `~/.cloudflared/config.yml` and the named tunnel entirely, mints a
> fresh random `*.trycloudflare.com` hostname on every restart, and shows up as
> `error` in `brew services list`. Your domain is never served. If you already
> started it: `brew services stop cloudflared`.

Or just run `bash deploy/selfhost/finish-tunnel-setup.sh`, which does steps 4
and 5 and is safe to re-run.

## 5. Re-point everything that hardcoded the Render URL

**Stripe webhook.** In the Stripe dashboard, change the endpoint to
`https://your-domain/api/billing/webhook` — note `billing`, not `stripe`. The
route is defined at `web/app.py:4256` and there is no `/api/stripe/*` route in
the app at all; pointing Stripe at one yields a 404, the charge still succeeds,
and the subscription silently never activates. Stripe issues a **new signing secret**
— put it in `.env` as `STRIPE_WEBHOOK_SECRET` and restart the service.
Subscriptions will not activate after checkout until this is right.

**Supabase Auth.** Add `https://your-domain` to the redirect allowlist
(Authentication → URL Configuration), or logins bounce back to the old host.

**Browser extension.** `coreprop-extension/manifest.json` hardcodes the Render
URL in two places. Both need your domain added:

- `host_permissions` — currently `https://coreprop.onrender.com/*`
- the `cp-beacon.js` entry in `content_scripts.matches`

Then re-run `./package-extension.sh` and reinstall the unpacked extension.
Until you do, one-click PrizePicks placement stays broken.

## 6. Verify end to end

```bash
curl -s https://your-domain/health
curl -s https://your-domain/api/status
```

Then in a browser: log in, confirm the +EV board fills, open the Backtest tab
and check the P&L chart renders, and run one real checkout in Stripe test mode
to confirm the webhook fires.

---

## Operating it

```bash
launchctl unload ~/Library/LaunchAgents/com.coreprop.server.plist   # stop
launchctl load   ~/Library/LaunchAgents/com.coreprop.server.plist   # start
tail -f ~/Library/Logs/coreprop/server.log                          # logs
launchctl unload ~/Library/LaunchAgents/com.coreprop.tunnel.plist   # stop tunnel
launchctl load   ~/Library/LaunchAgents/com.coreprop.tunnel.plist   # start tunnel
tail -f ~/Library/Logs/coreprop/tunnel.err.log                      # tunnel logs
cloudflared tunnel info coreprop                                    # edge connections
```

**Only one instance may run with `DISABLE_PERSISTENCE=false`.** That flag makes
an instance write the shared scraped-state seed back to Supabase. Two writers
overwrite each other. If Render ever comes back online, set it to `true` on one
side.

**Logs.** `httpx` used to log one line per book request — measured at 3208 of
3276 lines, about 200 MB/day. `web/app.py` now pins that logger to WARNING, so a
full scrape cycle writes ~2 KB instead of ~590 KB. Export
`COREPROP_HTTP_LOG=info` to get per-request lines back while debugging a scraper.

That is small enough that rotation is a safety net rather than a necessity, but
uvicorn's access log still accumulates. To cap it, add
`/etc/newsyslog.d/coreprop.conf` (needs sudo):

```
# logfilename                                   [owner:group]     mode count size when  flags
/Users/seniortech/Library/Logs/coreprop/*.log   seniortech:staff  644  7     10240 *    GJ
```

Seven compressed generations, rotated at 10 MB each. Verify with
`sudo newsyslog -nv`.

**Refresh interval.** Defaults to 5 minutes (`web/state.py`), tunable in the UI.
Five minutes from a single residential IP is what triggers the intermittent
PrizePicks 429s; if you see `"prizepicks": "Empty response"` in `/api/status`,
raise it to 10.

## What this setup does not give you

No redundancy and no alerting. If the Mac reboots, the internet drops, or the
service crashes in a way launchd can't recover, the site is down and nothing
tells you — you find out from a customer. With live Stripe billing that is a
real risk. If it starts costing you, Render Starter at $7/mo is a drop-in
return: no code changes, and the old URL keeps every integration above working.

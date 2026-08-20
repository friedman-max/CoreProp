# Deploying CoreProp

CoreProp runs in **two independent places**:

- **Production — `https://coreprop.me`**: a self-hosted server on a personal
  laptop, exposed through a **Cloudflare Tunnel**. This is the live site.
- **`https://coreprop.onrender.com`**: a Render deploy that auto-builds from
  GitHub `main` (`render.yaml`). Currently **down (503)** and unused.

> **Key fact:** `git push` to GitHub `main` updates the (dead) Render copy but
> does **NOT** update `coreprop.me`. The laptop has to pull `main` and restart.
> Until it does, the live site keeps running whatever it last started with.
> This is the gap to close — see the script below.

## Deploy to coreprop.me (run on the laptop that serves it)

1. Find the app service and its checkout (macOS):
   ```bash
   launchctl list | grep -i coreprop
   ls -la ~/Library/LaunchAgents/ | grep -iE 'coreprop|cloudflare'
   ps aux | grep -iE 'uvicorn|gunicorn|main\.py|cloudflared' | grep -v grep
   ```
   `cat` the plist it names (likely `~/Library/LaunchAgents/com.coreprop.server.plist`)
   to see the **checkout directory** and where **env vars** are set.

2. From that checkout directory:
   ```bash
   ./deploy.sh
   ```
   It runs `git pull origin main`, rebuilds the JS bundles **iff** `node` is
   present (the committed `dist/` already matches `main`, so this is optional),
   and restarts the launchd service. Verify the plist name in the script matches
   yours.

## Environment (set on the laptop — NOT Render)

Set these in the launchd plist's `EnvironmentVariables` (or the `.env` the
service loads):

| Var | Value | Why |
|---|---|---|
| `PUBLIC_BASE_URL` | `https://coreprop.me` | Stripe success/cancel/portal URLs — default is the dead onrender host |
| `VAPID_PUBLIC_KEY` | *(generated)* | Web Push — browsers subscribe with it |
| `VAPID_PRIVATE_KEY` | *(generated, secret)* | Web Push — signs notifications |
| `VAPID_SUBJECT` | `mailto:you@…` | Web Push contact |
| `BILLING_ENFORCE` | `true` *(only if you want the paywall enforced)* | defaults off |

Generate VAPID keys with the one-liner documented in `config.py`.

## Verify after deploy

```bash
curl -s  https://coreprop.me/api/ui-config   # should include "vapid_public_key"
curl -sI https://coreprop.me/sw.js            # should be 200
```

Then, on iPhone: Safari → Share → **Add to Home Screen** → open the icon → sign
in → account menu → **Turn on slip alerts**.

## Known fragility

Production depends on a **personal laptop staying awake** — if it sleeps, the
Cloudflare Tunnel drops and `coreprop.me` goes offline. Keep it awake
(`caffeinate -dimsu`, or `sudo pmset -a sleep 0 && sudo pmset -a autorestart 1`),
or move production to an always-on host (a paid Render instance, a small VPS).

## Tunnel watchdog

`cloudflared` retries a dropped edge connection with exponential backoff capped
at ~64s. The laptop's WiFi drops briefly around **115 times a day**
(`sendmsg: network is unreachable` in `~/Library/Logs/coreprop/tunnel.err.log`),
and 90 of those escalated to the full cap — so a five-second blip becomes a
**one-to-two minute outage for every user**. cloudflared is behaving correctly;
that backoff is tuned for a server with a stable uplink, which a laptop is not.

launchd's `KeepAlive` already restarts cloudflared if the process *dies*
(measured: under a second). It does nothing when the process is alive but wedged
in backoff — which is the failure that actually happens. The watchdog covers
exactly that gap.

```bash
mkdir -p ~/Library/Application\ Support/CoreProp
cp deploy/selfhost/tunnel-watchdog.sh ~/Library/Application\ Support/CoreProp/
chmod +x ~/Library/Application\ Support/CoreProp/tunnel-watchdog.sh
cp deploy/selfhost/com.coreprop.watchdog.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.coreprop.watchdog.plist
```

> **It must not run from the repo checkout.** This repo lives under `~/Desktop`,
> and macOS TCC refuses to let a LaunchAgent execute anything there. It fails
> with `Operation not permitted` **silently, every interval**, while `launchctl
> list` cheerfully reports the job as loaded — so the watchdog looks installed
> and does nothing. Run it from Application Support and re-copy after editing.

It will not restart while the app itself is down (a different fault, and
bouncing the tunnel would mask it), nor while the internet is unreachable
(reconnecting is impossible), and it honours a 180s cooldown. Activity lands in
`~/Library/Logs/coreprop/watchdog.log`.

Verified by fault injection in both modes: `kill -9` (launchd recovers, <1s) and
`kill -STOP` — process alive but not serving, the real one — recovered in 73s.

**This does not replace external monitoring.** A watchdog on the host cannot
report that the host is asleep, off, or offline. Point a free uptime service at
`https://coreprop.me/health` for that.

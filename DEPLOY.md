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

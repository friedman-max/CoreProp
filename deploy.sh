#!/usr/bin/env bash
# Deploy the latest GitHub `main` to THIS self-hosted coreprop.me box.
#
# `git push` updates GitHub (and the dead Render copy) but NOT coreprop.me — the
# laptop serving it has to pull and restart. Run this here to do that. See
# DEPLOY.md for the full picture and the env vars this box must have set.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> git pull origin main"
git pull origin main

# The committed dist/*.js already matches main, so a rebuild is optional. Do it
# only if node is available here (Render's build env is pip-only, but a laptop
# usually has node).
if command -v node >/dev/null 2>&1 && [ -x ./build.sh ]; then
  echo "==> node present — rebuilding bundles"
  ./build.sh
else
  echo "==> node absent — serving committed dist/ (fine, it matches main)"
fi

# Restart the app. Adjust the plist name if yours differs (see DEPLOY.md step 1).
PLIST="$HOME/Library/LaunchAgents/com.coreprop.server.plist"
if [ -f "$PLIST" ]; then
  echo "==> restarting via $PLIST"
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load "$PLIST"
else
  echo "!! $PLIST not found — restart the app however you run it, then re-check." >&2
fi

echo "==> done. Verify:"
echo "   curl -s  https://coreprop.me/api/ui-config   # expect \"vapid_public_key\""
echo "   curl -sI https://coreprop.me/sw.js            # expect 200"

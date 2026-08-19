#!/usr/bin/env python3
"""Check that STRIPE_WEBHOOK_SECRET in .env is the one the running service will
accept — without needing Stripe, the tunnel, or a real card.

Signs a synthetic event with the secret from .env and posts it at the local
service, exactly as Stripe would. Run it after ANY change to the webhook
endpoint in the Stripe dashboard.

    python3 deploy/selfhost/verify-webhook.py

A 200 on the valid case AND a 400 on the forged case is the passing result.
Both 400 means the secret in .env no longer matches the endpoint — copy the
current one from the Stripe dashboard (Signing secret -> Reveal) and reload:

    launchctl unload ~/Library/LaunchAgents/com.coreprop.server.plist
    launchctl load   ~/Library/LaunchAgents/com.coreprop.server.plist

The event used is `customer.updated`, which billing_webhook verifies and then
deliberately ignores, so this writes nothing to Supabase.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
URL = os.getenv("COREPROP_URL", "http://127.0.0.1:8010") + "/api/billing/webhook"


def _secret_from_env_file() -> str:
    env = REPO / ".env"
    if not env.exists():
        sys.exit(f"no .env at {env}")
    for line in env.read_text().splitlines():
        line = line.strip()
        if line.startswith("STRIPE_WEBHOOK_SECRET="):
            return line.split("=", 1)[1].strip()
    sys.exit("STRIPE_WEBHOOK_SECRET not found in .env")


def _post(body: bytes, secret: str, forge: bool = False) -> tuple[int, str]:
    ts = str(int(time.time()))
    sig = "0" * 64 if forge else hmac.new(
        secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    req = urllib.request.Request(URL, data=body, headers={
        "Content-Type": "application/json",
        "stripe-signature": f"t={ts},v1={sig}",
    })
    try:
        r = urllib.request.urlopen(req, timeout=20)
        return r.status, r.read().decode()[:80]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:80]
    except urllib.error.URLError as e:
        sys.exit(f"cannot reach {URL}: {e.reason}\nis the launchd service running?")


def main() -> int:
    secret = _secret_from_env_file()
    body = json.dumps({
        "id": "evt_verify", "object": "event", "type": "customer.updated",
        "data": {"object": {"id": "cus_verify", "object": "customer"}},
    }).encode()

    print(f"endpoint: {URL}")
    print(f"secret:   {secret[:11]}…{secret[-4:]}\n")

    ok_code, ok_body = _post(body, secret)
    bad_code, _ = _post(body, secret, forge=True)

    print(f"  valid signature  -> HTTP {ok_code} {ok_body}")
    print(f"  forged signature -> HTTP {bad_code}\n")

    if ok_code == 200 and bad_code == 400:
        print("PASS — the secret in .env matches and forgeries are rejected.")
        return 0
    if ok_code == 400 and bad_code == 400:
        print("FAIL — .env's STRIPE_WEBHOOK_SECRET is not the endpoint's current "
              "secret.\n       Copy it from the Stripe dashboard and reload the service.")
        return 1
    if bad_code != 400:
        print("FAIL — a forged signature was NOT rejected. Do not take payments "
              "until this is understood.")
        return 2
    print(f"FAIL — unexpected result (valid={ok_code}, forged={bad_code}).")
    return 3


if __name__ == "__main__":
    raise SystemExit(main())

# CoreProp for iOS

A native SwiftUI client for CoreProp — it reads the same backend
(`https://coreprop.me`) and authenticates against the same Supabase project as
the web app / PWA, so the server needs no changes to support it.

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for the full design, the backend
contract, and the screen map.

## Requirements

- **Xcode 15+** (iOS 16.0 deployment target).
- To run on a device: an Apple Developer account (free is fine for local
  installs) — set your Team in the target's *Signing & Capabilities*.

> The project was scaffolded on a machine with only Xcode **Command Line
> Tools**, so it ships a committed `CoreProp.xcodeproj` plus a `project.yml`.
> You do **not** need XcodeGen to open it.

## Open & run

```bash
open ios/CoreProp.xcodeproj
```

Pick the **CoreProp** scheme, choose a simulator (or your device), and Run
(⌘R). On a device, set your signing Team first (Signing & Capabilities →
Automatically manage signing).

The app points at production (`https://coreprop.me`) by default. To run against
a local backend (`python main.py` on port 8000), either:

- set a `CorePropBaseURL` string in `CoreProp/Support/Info.plist`
  (e.g. `http://127.0.0.1:8000` — an ATS exception for localhost is already in
  the plist), or
- use **Account → Developer → Backend** in the running app and relaunch.

## Regenerating the project

`project.yml` is the source of truth. If you have
[XcodeGen](https://github.com/yonyz/XcodeGen) (`brew install xcodegen`):

```bash
cd ios && xcodegen generate
```

This rewrites `CoreProp.xcodeproj` from `project.yml`, the local `CorePropKit`
Swift package, and the sources under `CoreProp/`.

## Tests & verification

Two layers, because bare Command Line Tools have no XCTest:

- **In Xcode:** ⌘U runs `CorePropTests` (the XCTest suite).
- **Anywhere with a Swift toolchain:** the platform-agnostic core has an
  executable assertion runner:

  ```bash
  cd ios/CorePropKit
  swift build
  swift run CorePropKitVerify      # exits non-zero on any failure
  ```

  It verifies model decoding against real backend JSON, the EV / payout math
  (re-deriving `BREAK_EVEN` from the payout tables — the same contract
  `tests/engine_tests/test_payout_table_mirror.py` enforces server-side),
  backtest scoring, formatting, and ISO-8601 parsing.

## Project layout

```
ios/
  CorePropKit/            Foundation-only Swift package: models, API client,
                          Supabase auth, EV/payout math, formatting.
                          (Compiles + runs on macOS for verification.)
  CoreProp/               The SwiftUI app.
    App/                  @main, root shell, AuthManager, AppModel, SlipStore,
                          notifications, app delegate.
    Theme/                Design tokens (ported from index.html :root) + shared
                          components.
    Features/             One folder per screen: Auth, Bets, Lines, Slip,
                          Backtest, Account.
    Support/              Info.plist + Assets.xcassets.
  CorePropTests/          XCTest suite (runs in Xcode).
  project.yml             XcodeGen project definition.
  CoreProp.xcodeproj      Committed, generated project.
```

## Payment / subscriptions (App Store note)

Billing is **off by default** on the backend, so the app is fully functional
without any purchase. When billing is enforced, this app behaves as a
**reader/companion**: it shows subscription *status* and (for existing paying
subscribers) a "Manage subscription" link to the Stripe customer portal, but it
does **not** present an in-app purchase or a buy button — that would violate App
Store Review Guideline 3.1.1, since payment is Stripe web checkout.

To ship a version that *sells* the subscription in-app, add StoreKit In-App
Purchase and reconcile the entitlement with the backend server-side. That work
is intentionally out of scope here.

## Notifications (follow-up work)

The app requests notification permission and captures the APNs device token, but
**does not** claim to deliver anything: the existing server push is Web Push
(VAPID / `pywebpush`), which only reaches the installed *web* PWA. Native
delivery needs APNs. To finish it:

1. Add the **Push Notifications** capability to the target (adds
   `aps-environment` to entitlements) and enable it on your App ID.
2. Add a server endpoint, e.g. `POST /api/push/apns/register`, storing
   `{user_id, device_token, environment}` in an `apns_tokens` table (RLS: owner
   policy, like `push_subscriptions` — see `migrations/migration_022.sql`).
3. Send via APNs from the auto-backtest worker alongside the existing Web Push
   send (`engine/push.py::send_to_user`), using an APNs auth key (`.p8`).
4. Upload the captured token in `NotificationManager.setDeviceToken` once the
   endpoint exists.

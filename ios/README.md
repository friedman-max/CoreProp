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

## Notifications (APNs) — implemented; how to turn it on

Native push is implemented end-to-end (client **and** server): the app uploads
its APNs token to `POST /api/push/apns/register`, and the auto-backtest worker
sends an Apple Push (via `engine/push.py::send_apns_to_user`) each time it logs
+EV slips for you, next to the existing Web Push. It is **env-gated** and off
until you provide credentials — two operator steps, no code changes:

1. **Server** — apply `migrations/migration_023.sql` (adds the owner-scoped
   `apns_tokens` table), then set the `APNS_*` env vars documented in
   `config.py`:
   - `APNS_AUTH_KEY` — the full `.p8` contents (an APNs Auth Key from the Apple
     Developer portal → Keys)
   - `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_BUNDLE_ID` (default `me.coreprop.app`)
   `h2` (in `requirements.txt`) provides the HTTP/2 APNs needs.
2. **App** — add the **Push Notifications** capability to the CoreProp target in
   Xcode (Signing & Capabilities). That adds an `aps-environment` entitlement
   and requires an APNs-enabled App ID. Debug builds register on the APNs
   *sandbox*, release builds on *production* (`NotificationManager.pushEnvironment`).

Until both are done, permission + token registration still work; delivery is a
server-side no-op. Tapping a slip alert opens the Backtest tab.

## Performance / Analytics

The **Account → Performance** screen renders your logged-slip analytics with
Swift Charts (cumulative P&L, a calibration reliability curve, CLV, and accuracy
stats) from `GET /api/analytics`. The deeper observatory/per-prop breakdowns
remain on the web app.

## Continuous integration

`.github/workflows/ios.yml` runs on macOS: it verifies `CorePropKit`
(`swift run CorePropKitVerify`), regenerates the project with XcodeGen, and
builds the app for the iOS Simulator (no signing) — so a broken build or a
failed logic check is caught in CI.

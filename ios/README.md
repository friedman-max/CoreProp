# CoreProp for iOS

A native iOS app that hosts coreprop.me in a `WKWebView`. The web app is the
product; this target exists to put it on the home screen with an icon, a dark
launch screen, and native handling for the things a browser tab does badly
(offline state, external links, pull-to-refresh).

## Prerequisites

**Xcode** — the full app from the Mac App Store, not Command Line Tools.
`xcodebuild` cannot build an iOS target without it. Roughly a 10 GB download and
~35 GB installed.

After installing, point the toolchain at it (needs your password):

```bash
sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
```

**XcodeGen** — already installed via Homebrew. `CoreProp.xcodeproj` is generated
from `project.yml` and is *not* checked in, so there is no 20k-line
`project.pbxproj` to resolve merge conflicts in.

```bash
brew install xcodegen   # if setting up a new machine
cd ios && xcodegen generate
```

## Getting it onto your iPhone

1. `cd ios && xcodegen generate && open CoreProp.xcodeproj`
2. In Xcode: **Settings → Accounts → +** and sign in with your Apple ID. You
   have to do this part yourself — it is your password.
3. Select the **CoreProp** target → **Signing & Capabilities** → set **Team** to
   your personal team. Leave *Automatically manage signing* checked.
4. If the bundle id `me.coreprop.ios` is rejected as unavailable, change it to
   something globally unique (`me.coreprop.ios.<yourname>`). Free provisioning
   allocates real App IDs, so someone else may already hold it.
5. Plug in the iPhone, pick it from the run-destination menu, press **⌘R**.
6. First run only: the phone refuses to launch an untrusted developer build.
   **Settings → General → VPN & Device Management → <your Apple ID> → Trust**.

## What a free account does and does not get you

| | Free | Paid ($99/yr) |
|---|---|---|
| Install on your own device | ✅ | ✅ |
| **Signing validity** | **7 days** | 1 year |
| TestFlight | ❌ | ✅ |
| App Store | ❌ | ✅ |
| Devices | your own | 100/yr |

**The 7-day expiry is the thing to know.** After a week the provisioning profile
dies and the app refuses to launch — tapping the icon does nothing. The fix is
to re-run it from Xcode, which takes about a minute. Nothing is lost; it is not
a reinstall. But it does mean this is not a "set it up once" arrangement until
the account is paid.

Free provisioning also caps you at 10 App IDs per 7 days, which only matters if
you keep changing the bundle identifier.

## Before this could go on the App Store

Not blockers today — a free account cannot submit at all — but they decide
whether paying $99 actually gets CoreProp listed:

- **Guideline 3.1.1 (In-App Purchase).** Subscriptions unlocking digital content
  must sell through Apple's IAP at a 15–30% cut. CoreProp sells access via
  Stripe. As-is, this is a rejection, and routing through IAP changes the unit
  economics of the $500 annual plan considerably.
- **Guideline 4.2 (Minimum Functionality).** Apple rejects apps that are only a
  website in a shell. This would need native surface — push notifications for
  line moves, a share sheet, an offline-readable slip — to clear that bar.
- **Betting-adjacent review.** CoreProp takes no wagers and settles no money,
  which keeps it out of Guideline 5.3's gambling rules, but expect the review to
  ask. Having the "no wagering, analytics only" framing ready is worth it.

## Known limitation: Google sign-in

Google refuses OAuth inside embedded web views (`disallowed_useragent`) and has
since 2021, so **Sign in with Google will not work in this app**. Email and
password sign-in works normally — that path runs through Supabase and Resend,
which have no such restriction.

The fix is not a user-agent override; that is against Google's terms and breaks
whenever they tighten detection. It needs the OAuth callback redirected to a
custom URL scheme that the app claims, then the returned session handed to the
web view. That is real work on both the app and `web/static/api.jsx`, so it is
deliberately not in v1.

## Layout notes

Two things in `WebView.swift` are load-bearing and look like they could be
deleted:

- `isOpaque = false` plus clear backgrounds on both the web view and its scroll
  view. Without all three, there is a white frame on every navigation, which is
  glaring against a `#0a0a0d` page.
- The top safe area is **not** ignored. The site's `.cp-nav` is
  `position: sticky; top: 0` with no `env(safe-area-inset-top)` padding, so
  extending under the notch puts the tab bar behind the status bar. If the web
  CSS ever adopts `viewport-fit=cover`, this can change.

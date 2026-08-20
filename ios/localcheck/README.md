# `ios/localcheck` — a local type-check gate for the iOS app

```bash
./typecheck-all.sh                  # the gate. non-zero exit on any error.
./typecheck-all.sh --selftest       # prove it still catches 13 planted defects
./typecheck-all.sh --selftest-reach # prove all 31 files are individually checked
```

This box has Command Line Tools only — no Xcode, no iOS SDK — so `ios/CoreProp`
cannot be compiled locally and `.github/workflows/ios.yml` (`xcodebuild` on
`macos-15`, push/PR to `main` only) is the only real compile gate. This script
type-checks the **real, unmodified** app sources against the **macOS** SDK, with
local shims standing in for iOS-only API.

It covers **all 31** app files. Without shims the reachable set is **10**, not the
~15 usually quoted, because the count has to be a *transitive closure* rather than
just "files with no iOS-only API": 14 files need shims directly (2 `import UIKit`,
12 use iOS-only SwiftUI), and that then disqualifies `AppModel` (references
`NotificationManager`), the four view models (reference `AppModel`),
`MainTabView` (references all five tab views) and `RootView` (references
`AuthView` and `UIImage`). What is left is `Theme`, `Components`, `BetRow`,
`SlipCard`, `Shared`, `AppConfig`, `AppRouter`, `AuthManager`,
`KeychainSessionStore`, `SlipStore` — and notice `RootView` is *not* in it,
despite being one of the files most in need of a gate.

Nothing in this directory is compiled into the app. `project.yml` globs the app
target from `sources: - path: CoreProp` and the test target from
`- path: CorePropTests`; `localcheck/` is a sibling of both and matches neither,
so there is no path by which a shim reaches a shipping binary.

---

## What a green run proves

Every identifier and every type in all 31 app files resolves, against the real
`CorePropKit` module and the real SwiftUI/Charts/Foundation declarations. In
practice that catches the failure modes a large visual refactor actually
produces:

- a misspelled `Theme` member (`Theme.s7` — the scale has no `s7`/`s9`/`s11`)
- a wrong or extra argument label (`cpCard(cornerRadius:)` for `cpCard(radius:)`)
- a modifier applied to a type that does not have it — including the classic
  misplaced-paren slip, `.font(Theme.mono(12).foregroundColor(...))`, which
  applies a `View` modifier to a `Font`
- a wrong argument type, including across the app ↔ `CorePropKit` boundary
- a member removed from `Theme`/`Components` while call sites still reference it
- **iOS-17-only API in an iOS-16 target** — but only as a proxy; see below

`--selftest` asserts each of those by planting the defect in a **copy** of the
sources and requiring a specific diagnostic. `--selftest-reach` plants a canary
in all 31 files at once and requires all 31 to be named in the output — which is
the only real evidence that a clean run means "31 files were examined" rather
than "the file list quietly shrank".

## What a green run does **not** prove

- **It is not `xcodebuild`.** Different SDK, different platform, no code
  generation, no linking, no asset catalogue, no `Info.plist`, no entitlements,
  no code signing, no Swift concurrency diagnostics under the app target's
  actual `SWIFT_STRICT_CONCURRENCY` setting. CI remains the gate that decides
  whether the app builds.
- **Nothing about runtime or rendering.** `-typecheck` never emits code. Every
  shim body is `fatalError()` or `self`. A layout that resolves to a blank
  screen, a `@State` that never updates, a `List` that scrolls wrong, a colour
  that fails contrast — all invisible here.
- **The shims are not Apple's declarations.** They are hand-written
  approximations of the iOS API, in this repo, maintained by hand. Where a shim
  is narrower than the real thing, a legal call site can fail here and pass in
  CI. Where it is wider, the reverse. Both have happened during development of
  this script.
- **Availability checking is a proxy and it is thin.** See the next section.
- **Selectors and string-keyed lookups are unchecked** — as they are in
  `xcodebuild` too. A misspelled
  `application(_:didRegisterForRemoteNotificationsWithDeviceToken:)` compiles
  everywhere and simply never fires. `UIImage(named: "BrandWordmark")` will not
  tell you the asset is missing.
- **A failing run is not a partial result.** See "The verdict is binary".

## Availability is a proxy

The script targets `<arch>-apple-macos13.0` because macOS 13 is the platform
peer of iOS 16, the app's deployment target (`project.yml`:
`deploymentTarget.iOS "16.0"`). That is what lets it reject iOS-17-only API:
`ContentUnavailableView` and the two-parameter `onChange(of:) { old, new in }`
are iOS 17 / macOS 14, so at `macos13.0` the frontend says *"only available in
macOS 14.0 or newer"*. Verified — it is self-test case 4.

Two things follow, and the second is the important one:

1. The recipe this script grew out of used `-target arm64-apple-macos14.0`.
   macOS 14 is the peer of iOS **17**, so that target accepts every iOS-17-only
   API silently. If you copy a one-off `swiftc` line out of a plan document,
   check its target.
2. **The iOS↔macOS version mapping is a convention, not a contract.** Apple
   annotates each declaration per-platform, and the versions do not always move
   together: plenty of API shipped on macOS a release later than the "equivalent"
   iOS version, or never. So this check has both false negatives (an iOS-17-only
   API that Apple happened to ship on macOS 13 will pass here) and false
   positives (a macOS-14-only API that exists on iOS 16 would fail here for no
   good reason). It is a smoke alarm, not a compliance check. **An iOS
   availability claim still requires CI.**

`-Xfrontend -disable-availability-checking` would delete every
`unavailable in macOS` error in one flag, with zero shims and perfect signature
fidelity — genuinely tempting, and strictly better for that one purpose. It was
rejected because it also switches off the version check above, which is the only
availability signal this gate has. The shims in `shims/SwiftUIiOSOnly.swift` are
the price of keeping it.

## The verdict is binary

**A clean run is meaningful. A failing run is not a score.**

`swiftc` reports diagnostics from **only the first file in command-line order
that has errors** when type-checking whole-module, and says nothing at all about
the rest. Measured directly: three files each containing `let a: Int = "x"`
produce one error whole-module and three under
`-enable-batch-mode -driver-batch-count 3`. This is not specific to availability
diagnostics — it applies to all of them.

Worse, a *fatal* diagnostic — a missing module above all — aborts the whole
invocation in **both** modes. That is exactly what the pre-harness baseline
looked like: `import UIKit` in two files produced

```
CoreProp/App/CorePropApp.swift:2:8: error: no such module 'UIKit'
```

and **not one word** about the other 30 files. One line of output, zero
coverage, and nothing on screen to suggest the difference.

The script therefore always passes `-enable-batch-mode -driver-batch-count N`,
and prints a warning on failure that the count is not a progress metric. If you
are adding a shim and watching the error count fall, understand that the run is
not covering the files that still fail — do not read a drop from 18 to 4 as
"most of the tree is verified now". Get to zero, then believe it.

## Symbols we shadow

Two different reasons a SwiftUI symbol fails on the macOS SDK, needing opposite
treatment:

- *"has no member `x`"* — absent from macOS entirely. A plain extension **adds**
  it. No shadowing, no risk.
- *"`x` is unavailable in macOS"* — present in the macOS SwiftUI interface,
  marked `@available(macOS, unavailable)`. A same-name declaration in our module
  **wins overload resolution** over the unavailable one. This is deliberate
  shadowing of a real SDK symbol, and it is the one honest risk in this
  directory.

| Symbol | Shadowed? | Note |
|---|---|---|
| `View.navigationBarTitleDisplayMode(_:)` | **yes** | macOS-unavailable. Parameter is our own 3-case enum, not iOS's `NavigationBarItem.TitleDisplayMode` (that nested type is itself macOS-unavailable, so it cannot be reused). Typo'd cases still fail — self-test 8. |
| `ToolbarItemPlacement.navigationBarTrailing` / `.navigationBarLeading` | **yes** | macOS-unavailable. Real `ToolbarItemPlacement` values, so `ToolbarItem(placement:)` still type-checks its argument. Self-test 9. |
| `ListStyle.insetGrouped` | **yes** | macOS-unavailable. See "erased type information". Self-test 13. |
| `View.textContentType(_:)` | **yes** | macOS *has* this, typed `NSTextContentType?` — a **different type** from iOS's `UITextContentType?`. We declare the iOS shape so the app's `.newPassword` resolves against the iOS member list. This is the subtlest entry in the table: a same-named, differently-typed real symbol. Self-test 11. |
| `View.keyboardType(_:)` | no | absent on macOS. Typed against shimmed `UIKeyboardType`. Self-test 10. |
| `View.textInputAutocapitalization(_:)` | no | absent on macOS. Self-test 12. |
| `UIApplication`, `UIImage`, `UIKeyboardType`, `UITextContentType`, `UIApplicationDelegate`, `UIApplicationDelegateAdaptor` | no | no macOS symbol of these names; macOS has `NSApplication` / `NSImage` / `NSTextContentType`. |

What shadowing **cannot** catch is signature drift: if a future SDK changes the
real modifier's shape, our shim keeps accepting the old shape and the difference
only surfaces in CI. There is no local defence against that. Re-run `--selftest`
after an Xcode/CLT upgrade.

One cosmetic leak: because `navigationBarTitleDisplayMode` takes our enum, a
typo produces `type 'NavigationBarTitleDisplayModeShim' has no member 'inlin'`.
The shim name appears in a diagnostic about real app code. Accepted in exchange
for the type checking.

## Where we erased type information

One place, deliberately:

- **`ListStyle.insetGrouped` returns `PlainListStyle`.** `ListStyle` is a sealed
  protocol — a local `struct CPInsetGroupedListStyle: ListStyle {}` fails with
  *"does not conform to protocol 'ListStyle'"* because the witness is
  SDK-internal — so the shim routes through an existing available conformer.
  Consequence: `.insetGrouped` and `.plain` are the **same type** to this gate,
  so swapping one for the other is invisible. List style is a pure appearance
  choice with no API surface downstream, so the loss is acceptable. A misspelled
  style name is still caught (self-test 13); only a *valid but wrong* style is
  not.

Everything else preserves types: each shimmed enum/struct carries the **full**
real iOS member list, not just the members the app happens to use. That matters
in both directions — a shim declaring only `.emailAddress` would reject a
legitimate `.numberPad` and teach people to ignore the gate, while a shim taking
`Any` would accept anything and make a PASS actively misleading.

## Don't build a "minimal support graph"

It is tempting to type-check a handful of files instead of all 31. Measured, the
transitive closure fans out to essentially the whole module:

- `AnalyticsView.swift` + `Theme` + `Components` → 7 errors
  (`cannot find type 'LoadState'`, `cannot find type 'AppModel'`,
  `APIError has no member 'display'`). `LoadState` and `APIError.display` live
  in `App/Shared.swift`; adding it leaves `AppModel` unresolved.
- `AppModel.swift` → needs `AuthManager`, `KeychainSessionStore`,
  `NotificationManager` (which imports UIKit).
- `BetsView.swift` → needs `AppModel`, `SlipStore`, `BetsViewModel`, `BetRow`,
  `BetDetailView`.

So any curated subset is a support-file list that silently rots as the tree
changes, and its failures look like your bug when they are the recipe's. The
script globs `CoreProp/**/*.swift` and compiles the lot — a new file is covered
the moment it lands, and the question does not arise. (For the record: the
often-cited 4-file recipe — `Theme`, `Components`, `BetRow`, `SlipCard` — *is*
clean, at both `bf8489d` and `c6b63d3`. The 16-error report attributed to it came
from the `AnalyticsView` recipe.)

## Files

```
typecheck-all.sh              the gate + both self-test modes
shims/UIKitModule/UIKit.swift a deliberately EMPTY module named UIKit, so
                              `import UIKit` resolves. A missing module is fatal
                              and blanks all 31 files' diagnostics.
shims/UIKitTypes.swift        UIKit declarations, compiled into the app module
                              (not the UIKit module) because RootView and
                              AuthView use UIKit types with no import — on iOS
                              SwiftUI re-exports UIKit, and same-module
                              declarations reproduce that ambient visibility.
shims/SwiftUIiOSOnly.swift    iOS-only SwiftUI modifiers.
```

Adding a `.swift` file to `shims/` is enough for it to be picked up; the script
globs that directory too.

## If you change the shims

Run `--selftest`. It is the only thing standing between this directory and a
gate that passes vacuously — which would be worse than having no gate, because
someone will trust it instead of CI.

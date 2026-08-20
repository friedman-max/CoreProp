// A deliberately EMPTY module named `UIKit`.
//
// Two app files say `import UIKit`. On the macOS SDK that module does not
// exist, and a missing module is a *fatal* frontend error: swiftc stops before
// type-checking anything, so one `import UIKit` blanks the whole 31-file run.
// This file exists only so the import resolves.
//
// Why empty rather than "put the UIKit declarations here": Swift imports are
// file-scoped, but `RootView.swift` (UIImage) and `AuthView.swift`
// (UIKeyboardType, UITextContentType) use UIKit types *without* importing
// UIKit — on iOS they arrive because SwiftUI re-exports UIKit. Reproducing that
// through a real module would need `@_exported import UIKit` to leak
// module-wide, which is an implementation detail we would rather not stake the
// gate on. Instead every UIKit declaration lives in `shims/UIKitTypes.swift`,
// which is compiled *into the same module* as the app sources and is therefore
// visible in all 31 files with no import at all — exactly the iOS behaviour we
// are emulating. This module stays empty so it can never shadow those.
//
// `internal` so nothing here is visible to importers even by accident.
internal enum __CorePropLocalCheckUIKitPlaceholder {}

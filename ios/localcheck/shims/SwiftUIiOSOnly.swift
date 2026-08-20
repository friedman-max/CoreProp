// SwiftUI API that the app uses and the macOS SDK does not offer.
//
// Compiled into the same module as the 31 app sources; never into the app.
//
// There are two different reasons a SwiftUI symbol fails on the macOS SDK, and
// they need opposite treatment:
//
//   1. "has no member 'x'"          — the symbol does not exist on macOS at all.
//                                     A plain extension adds it. No shadowing.
//   2. "'x' is unavailable in macOS" — the symbol EXISTS in the macOS SwiftUI
//                                     interface, marked @available(macOS,
//                                     unavailable). A same-name declaration in
//                                     our module wins overload resolution over
//                                     the unavailable one, which is how the call
//                                     site starts checking again.
//
// Case 2 is deliberate shadowing of a real SDK symbol, and it is the one honest
// risk in this file. Every case-2 shim below is listed in README.md under
// "Symbols we shadow". The mitigation is that each shim's parameter type is a
// real enum/struct with the real full case list, so a typo'd case
// (`.inlin`, `.navigationBarTrailingg`) is still an error — verified by the
// can-it-fail suite. What shadowing *cannot* catch is a signature drift in a
// future SDK: if Apple changes the real modifier's shape, our shim keeps
// accepting the old shape and the difference only shows up in CI.
//
// Rejected alternative: `-Xfrontend -disable-availability-checking`. It clears
// every case-2 error in one flag with zero shims and perfect signature fidelity,
// which is strictly better *for case 2* — but it also switches off version-based
// availability checking, which is the only thing standing between this gate and
// an iOS-17-only API landing in an iOS-16 target. Measured: at
// `-target arm64-apple-macos13.0` (the macOS peer of iOS 16) the frontend
// rejects `ContentUnavailableView` and the two-parameter `onChange(of:)` with
// "only available in macOS 14.0 or newer"; adding the flag silences both. The
// shims cost more words and buy that check back, so the shims win.

import SwiftUI

// MARK: - Case 2: exists on macOS, marked unavailable

/// Stands in for iOS's `NavigationBarItem.TitleDisplayMode`. The real nested
/// type is itself unavailable on macOS, so we cannot reuse it; this carries the
/// same three cases so `.inline` / `.large` / `.automatic` check and anything
/// else does not.
enum NavigationBarTitleDisplayModeShim {
    case automatic
    case inline
    case large
}

extension View {
    /// Shadows the macOS-unavailable `View.navigationBarTitleDisplayMode(_:)`.
    /// Used by 9 of the 31 files, which is why it is the single highest-value
    /// declaration in this directory: without it those 9 files — including 5 of
    /// the 7 that have no other local gate — are unreachable.
    func navigationBarTitleDisplayMode(_ mode: NavigationBarTitleDisplayModeShim) -> some View {
        self
    }
}

extension ToolbarItemPlacement {
    /// Shadows the macOS-unavailable placements. Kept as real
    /// `ToolbarItemPlacement` values so `ToolbarItem(placement:)` still checks
    /// its argument type, and a misspelled placement is still an error.
    /// `.automatic` is the stand-in value; placement is a layout concept and
    /// `-typecheck` never lays anything out.
    static var navigationBarTrailing: ToolbarItemPlacement { .automatic }
    static var navigationBarLeading: ToolbarItemPlacement { .automatic }
}

extension ListStyle where Self == PlainListStyle {
    /// Shadows `ListStyle.insetGrouped`. `ListStyle` is a sealed protocol — a
    /// local `struct CPInsetGroupedListStyle: ListStyle {}` fails with "does not
    /// conform" because the witness is underscored SDK-internal — so this
    /// returns an existing available conformer instead. That erases *which*
    /// list style was named: `.insetGrouped` and `.plain` are the same type
    /// here, so swapping one for the other is invisible to this gate. It is a
    /// pure appearance choice with no API surface, so the loss is acceptable;
    /// noted in README under "Where we erased type information".
    static var insetGrouped: PlainListStyle { .plain }
}

// MARK: - Case 1: absent from macOS entirely

/// iOS's `TextInputAutocapitalization`. Real type is a struct with four static
/// members; all four are declared so a legal value never false-fails.
struct TextInputAutocapitalization {
    static let never = TextInputAutocapitalization()
    static let words = TextInputAutocapitalization()
    static let sentences = TextInputAutocapitalization()
    static let characters = TextInputAutocapitalization()
}

extension View {
    /// Genuinely absent on macOS ("has no member"), so this adds rather than
    /// shadows.
    func textInputAutocapitalization(_ style: TextInputAutocapitalization?) -> some View {
        self
    }

    /// `View.keyboardType(_:)` is iOS-only. Typed against the shimmed
    /// `UIKeyboardType` in UIKitTypes.swift, so `.URL` checks and `.url` does
    /// not.
    func keyboardType(_ type: UIKeyboardType) -> some View { self }

    /// `View.textContentType(_:)` does exist on macOS, but typed as
    /// `NSTextContentType?` — a *different* type from iOS's
    /// `UITextContentType?`. Declaring the iOS shape means the app's
    /// `.textContentType(.newPassword)` resolves against the iOS member list,
    /// which is what we want checked. This is a case-2-style shadow of a
    /// same-named, differently-typed macOS symbol; listed in the README.
    func textContentType(_ type: UITextContentType?) -> some View { self }
}

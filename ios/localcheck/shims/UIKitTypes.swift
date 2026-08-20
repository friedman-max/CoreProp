// UIKit declarations, for the local macOS type-check gate only.
//
// This file is compiled INTO THE SAME MODULE as the 31 app sources, never into
// the app. That placement is deliberate: on iOS, `import SwiftUI` re-exports
// UIKit, so `RootView.swift` uses `UIImage` and `AuthView.swift` uses
// `UIKeyboardType` / `UITextContentType` with no `import UIKit` line at all.
// Same-module declarations reproduce that ambient visibility exactly; a shim
// *module* would not, because Swift imports are file-scoped.
//
// Fidelity rule for everything below: copy the real iOS signature, including
// the full case/member list, even where the app only touches one case. A shim
// that declares just `.emailAddress` would pass `.emailAddress` and reject a
// legitimate `.numberPad` — the gate would then be a source of false failures,
// which is how people learn to ignore a gate. A shim that declares the full
// real list passes every legal spelling and rejects every typo, which is the
// whole point.
//
// None of these bodies run. `-typecheck` never emits code, so `fatalError()`
// is the honest body: it documents that this is a signature, not behaviour.

import Foundation
import SwiftUI

// MARK: - UIApplication

/// iOS `UIApplication`. macOS has `NSApplication`; the names do not collide.
final class UIApplication {
    /// Real iOS type is a `RawRepresentable` struct wrapper over `String`.
    struct LaunchOptionsKey: Hashable, RawRepresentable {
        let rawValue: String
        init(rawValue: String) { self.rawValue = rawValue }
    }

    static let shared = UIApplication()

    /// Type information preserved: passing anything to this would be an error,
    /// as on iOS.
    func registerForRemoteNotifications() { fatalError("localcheck shim") }

    /// iOS declares `open(_:options:completionHandler:)` with defaults; the
    /// one-argument call site is the only one in the app. Defaults are spelled
    /// out rather than dropped so a call that passes `options:` still checks.
    @discardableResult
    func open(_ url: URL,
              options: [String: Any] = [:],
              completionHandler: ((Bool) -> Void)? = nil) -> Bool {
        fatalError("localcheck shim")
    }

    static let openSettingsURLString: String = "app-settings:"
}

/// iOS `UIApplicationDelegate` refines `NSObjectProtocol` and declares every
/// callback as optional-with-default. Only the three the app implements are
/// declared here, with their real labels and types, because those labels are
/// exactly what we want checked: `didRegisterForRemoteNotificationsWithDeviceToken`
/// misspelled is a silent no-op on iOS (the selector simply never fires) and is
/// one of the few classes of bug that neither this gate nor `xcodebuild` catches
/// — see README, "What this does not prove".
protocol UIApplicationDelegate: NSObjectProtocol {}

extension UIApplicationDelegate {
    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool { true }
    func application(_ application: UIApplication,
                     didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {}
    func application(_ application: UIApplication,
                     didFailToRegisterForRemoteNotificationsWithError error: Error) {}
}

/// iOS's `@UIApplicationDelegateAdaptor`. The `NSObject & UIApplicationDelegate`
/// constraint is copied from the real declaration, so pointing the adaptor at a
/// type that is not an app delegate still fails here.
@propertyWrapper
struct UIApplicationDelegateAdaptor<DelegateType>: DynamicProperty
where DelegateType: NSObject, DelegateType: UIApplicationDelegate {
    var wrappedValue: DelegateType
    init(_ delegateType: DelegateType.Type = DelegateType.self) {
        fatalError("localcheck shim")
    }
}

// MARK: - UIImage

/// `RootView.swift` only asks whether a named asset exists
/// (`UIImage(named:) != nil`). The failable initialiser is the load-bearing
/// part: making it non-failable would turn that `!= nil` into a warning-free
/// tautology and hide a real mistake.
final class UIImage {
    init?(named name: String) { fatalError("localcheck shim") }
}

// MARK: - Text input

/// Full iOS 16 case list. `URL` is capitalised in the real enum — `DeveloperView`
/// writes `.keyboardType(.URL)`, and `.url` would be a genuine error.
enum UIKeyboardType {
    case `default`
    case asciiCapable
    case numbersAndPunctuation
    case URL
    case numberPad
    case phonePad
    case namePhonePad
    case emailAddress
    case decimalPad
    case twitter
    case webSearch
    case asciiCapableNumberPad
}

/// Full iOS 16 member list. A struct with static members, matching iOS, rather
/// than an enum: the app passes `UITextContentType?` around and compares
/// `mode == .signUp ? .newPassword : .password`, which needs the members to be
/// values of the type.
struct UITextContentType: Hashable, RawRepresentable {
    let rawValue: String
    init(rawValue: String) { self.rawValue = rawValue }

    static let name = UITextContentType(rawValue: "name")
    static let namePrefix = UITextContentType(rawValue: "namePrefix")
    static let givenName = UITextContentType(rawValue: "givenName")
    static let middleName = UITextContentType(rawValue: "middleName")
    static let familyName = UITextContentType(rawValue: "familyName")
    static let nameSuffix = UITextContentType(rawValue: "nameSuffix")
    static let nickname = UITextContentType(rawValue: "nickname")
    static let jobTitle = UITextContentType(rawValue: "jobTitle")
    static let organizationName = UITextContentType(rawValue: "organizationName")
    static let location = UITextContentType(rawValue: "location")
    static let fullStreetAddress = UITextContentType(rawValue: "fullStreetAddress")
    static let streetAddressLine1 = UITextContentType(rawValue: "streetAddressLine1")
    static let streetAddressLine2 = UITextContentType(rawValue: "streetAddressLine2")
    static let addressCity = UITextContentType(rawValue: "addressCity")
    static let addressState = UITextContentType(rawValue: "addressState")
    static let addressCityAndState = UITextContentType(rawValue: "addressCityAndState")
    static let sublocality = UITextContentType(rawValue: "sublocality")
    static let countryName = UITextContentType(rawValue: "countryName")
    static let postalCode = UITextContentType(rawValue: "postalCode")
    static let telephoneNumber = UITextContentType(rawValue: "telephoneNumber")
    static let emailAddress = UITextContentType(rawValue: "emailAddress")
    static let URL = UITextContentType(rawValue: "URL")
    static let creditCardNumber = UITextContentType(rawValue: "creditCardNumber")
    static let username = UITextContentType(rawValue: "username")
    static let password = UITextContentType(rawValue: "password")
    static let newPassword = UITextContentType(rawValue: "newPassword")
    static let oneTimeCode = UITextContentType(rawValue: "oneTimeCode")
}

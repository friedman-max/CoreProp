import Foundation
import CorePropKit

/// Resolves the backend environment at launch. Defaults to production
/// (`https://coreprop.me`); overridable with an `CorePropBaseURL` string in
/// Info.plist (handy for pointing a debug build at a localhost server) or the
/// `CorePropBaseURL` UserDefaults key (settable from the Account → Developer row).
enum AppConfig {
    static var environment: CoreEnvironment {
        if let override = resolvedBaseURL() {
            return CoreEnvironment(baseURL: override)
        }
        return .production
    }

    private static func resolvedBaseURL() -> URL? {
        if let s = UserDefaults.standard.string(forKey: "CorePropBaseURL"),
           let url = URL(string: s), url.scheme != nil {
            return url
        }
        if let s = Bundle.main.object(forInfoDictionaryKey: "CorePropBaseURL") as? String,
           !s.isEmpty, let url = URL(string: s), url.scheme != nil {
            return url
        }
        return nil
    }

    static func setBaseURLOverride(_ string: String?) {
        if let string, !string.isEmpty {
            UserDefaults.standard.set(string, forKey: "CorePropBaseURL")
        } else {
            UserDefaults.standard.removeObject(forKey: "CorePropBaseURL")
        }
    }
}

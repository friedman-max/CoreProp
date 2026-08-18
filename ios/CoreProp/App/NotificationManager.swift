import Foundation
import Combine
import UIKit
import UserNotifications

/// Client-side notification support (Apple Push / APNs).
///
/// Requests notification authorization, registers for remote notifications, and
/// uploads the captured APNs device token to the backend
/// (`POST /api/push/apns/register`, via `AppModel`'s upload handler) so the
/// auto-backtest worker can push slip alerts. This is the native counterpart of
/// the web app's Web Push (VAPID), which only reaches the installed PWA.
///
/// End-to-end delivery is real but has two operator prerequisites (both env/
/// account level, not code): the app target must have the **Push Notifications**
/// capability (an `aps-environment` entitlement + an APNs-enabled App ID), and
/// the server must have the `APNS_*` keys configured (see config.py /
/// engine.push.apns_is_configured). Until both are in place, permission +
/// registration still work; delivery is simply a server-side no-op.
@MainActor
final class NotificationManager: ObservableObject {
    static let shared = NotificationManager()

    @Published private(set) var authorizationStatus: UNAuthorizationStatus = .notDetermined
    @Published private(set) var deviceTokenHex: String?
    @Published private(set) var lastError: String?

    /// Set by `AppModel`: uploads a captured token to the backend (only when
    /// signed in). Called whenever a fresh APNs token arrives.
    var uploadHandler: (@Sendable (_ token: String, _ environment: String) async -> Void)?

    /// APNs environment for this build. Debug builds provision the sandbox
    /// gateway; release builds the production gateway.
    static var pushEnvironment: String {
        #if DEBUG
        return "sandbox"
        #else
        return "production"
        #endif
    }

    private init() {}

    func refreshStatus() async {
        let settings = await UNUserNotificationCenter.current().notificationSettings()
        authorizationStatus = settings.authorizationStatus
    }

    /// Request authorization from a user gesture, then register for remote
    /// notifications so the APNs token flows into `setDeviceToken`.
    func requestAuthorization() async {
        do {
            let granted = try await UNUserNotificationCenter.current()
                .requestAuthorization(options: [.alert, .sound, .badge])
            await refreshStatus()
            if granted {
                UIApplication.shared.registerForRemoteNotifications()
            }
        } catch {
            lastError = error.localizedDescription
            await refreshStatus()
        }
    }

    func openSystemSettings() {
        if let url = URL(string: UIApplication.openSettingsURLString) {
            UIApplication.shared.open(url)
        }
    }

    // Called from the app delegate when APNs returns a device token.
    func setDeviceToken(_ data: Data) {
        let hex = data.map { String(format: "%02x", $0) }.joined()
        deviceTokenHex = hex
        lastError = nil
        // Upload to the backend (no-op when signed out / the endpoint is
        // unconfigured — the handler guards on auth and swallows errors).
        if let uploadHandler {
            let env = Self.pushEnvironment
            Task { await uploadHandler(hex, env) }
        }
    }

    func setRegistrationFailure(_ error: Error) {
        lastError = error.localizedDescription
    }
}

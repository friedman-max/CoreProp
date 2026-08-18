import Foundation
import Combine
import UIKit
import UserNotifications

/// Client-side notification support.
///
/// **Honest scope:** the existing CoreProp push feature is Web Push (VAPID),
/// which only reaches the installed *web* PWA — it cannot deliver to a native
/// app. Native delivery needs APNs: an Apple Developer Program membership, the
/// Push Notifications capability, a server endpoint to register device tokens,
/// and a server-side APNs sender. None of that is wired yet.
///
/// So this requests notification authorization and (best-effort) registers for
/// remote notifications to capture the APNs device token, and surfaces the
/// resulting state in Account. It does **not** claim to deliver anything: the
/// token is held locally until a `POST /api/push/apns/register` endpoint exists
/// (see README → Notifications). No fake/local "slip alert" is scheduled.
@MainActor
final class NotificationManager: ObservableObject {
    static let shared = NotificationManager()

    @Published private(set) var authorizationStatus: UNAuthorizationStatus = .notDetermined
    @Published private(set) var deviceTokenHex: String?
    @Published private(set) var lastError: String?

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

    // Called from the app delegate.
    func setDeviceToken(_ data: Data) {
        deviceTokenHex = data.map { String(format: "%02x", $0) }.joined()
        lastError = nil
        // Intentionally NOT uploaded: server-side APNs registration/delivery is
        // not implemented (see class doc + README).
    }

    func setRegistrationFailure(_ error: Error) {
        lastError = error.localizedDescription
    }
}

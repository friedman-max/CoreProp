import SwiftUI
import UIKit

/// App delegate solely for APNs device-token callbacks (SwiftUI has no hook for
/// these). See `NotificationManager` for the honest scope of push support.
final class AppDelegate: NSObject, UIApplicationDelegate {
    func application(_ application: UIApplication,
                     didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
        Task { @MainActor in NotificationManager.shared.setDeviceToken(deviceToken) }
    }

    func application(_ application: UIApplication,
                     didFailToRegisterForRemoteNotificationsWithError error: Error) {
        Task { @MainActor in NotificationManager.shared.setRegistrationFailure(error) }
    }
}

@main
struct CorePropApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var model = AppModel(environment: AppConfig.environment)
    @StateObject private var slip = SlipStore()
    @StateObject private var notifications = NotificationManager.shared

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(model)
                .environmentObject(model.auth)
                .environmentObject(slip)
                .environmentObject(notifications)
                .tint(Theme.primary)
                .preferredColorScheme(.dark)
        }
    }
}

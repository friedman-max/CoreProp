import SwiftUI
import UIKit
import UserNotifications

/// App delegate for APNs device-token callbacks (SwiftUI has no hook for these)
/// and notification presentation/tap handling. See `NotificationManager` for
/// the honest scope of push support.
final class AppDelegate: NSObject, UIApplicationDelegate, UNUserNotificationCenterDelegate {
    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil) -> Bool {
        UNUserNotificationCenter.current().delegate = self
        return true
    }

    func application(_ application: UIApplication,
                     didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data) {
        Task { @MainActor in NotificationManager.shared.setDeviceToken(deviceToken) }
    }

    func application(_ application: UIApplication,
                     didFailToRegisterForRemoteNotificationsWithError error: Error) {
        Task { @MainActor in NotificationManager.shared.setRegistrationFailure(error) }
    }

    // Show slip alerts even while the app is foregrounded.
    func userNotificationCenter(_ center: UNUserNotificationCenter,
                                willPresent notification: UNNotification,
                                withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void) {
        completionHandler([.banner, .list, .sound])
    }

    // A tapped slip alert opens the Backtest tab.
    func userNotificationCenter(_ center: UNUserNotificationCenter,
                                didReceive response: UNNotificationResponse,
                                withCompletionHandler completionHandler: @escaping () -> Void) {
        Task { @MainActor in AppRouter.shared.openBacktest() }
        completionHandler()
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

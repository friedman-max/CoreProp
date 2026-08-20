import SwiftUI
import CorePropKit

/// Notification permission + honest status. See `NotificationManager` for why
/// native delivery is not yet wired (needs server-side APNs).
struct NotificationsView: View {
    @EnvironmentObject private var notifications: NotificationManager

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                VStack(alignment: .leading, spacing: 10) {
                    Label("Slip alerts", systemImage: "bell.badge")
                        .font(Theme.ui(16, .bold)).foregroundColor(Theme.text)
                    Text("Get notified when CoreProp logs a slip for you (Auto-Backtest). Grant permission below.")
                        .font(Theme.ui(13)).foregroundColor(Theme.text2)
                    statusRow
                    actionButton
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .cpCard()

                VStack(alignment: .leading, spacing: 8) {
                    Text("HOW IT WORKS").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
                    Text("When you enable alerts, this device registers with Apple Push and the server sends a notification each time it auto-logs +EV slips for you. Tapping an alert opens your Backtest. (Delivery requires the server's APNs keys to be configured.)")
                        .font(Theme.ui(13)).foregroundColor(Theme.text3)
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .cpCard()
            }
            .padding(16)
        }
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Slip alerts")
        .navigationBarTitleDisplayMode(.inline)
        .task { await notifications.refreshStatus() }
    }

    private var statusRow: some View {
        HStack {
            Text("Permission").font(Theme.ui(14)).foregroundColor(Theme.text2)
            Spacer()
            Text(statusText).font(Theme.ui(14, .semibold)).foregroundColor(statusColor)
        }
    }

    private var statusText: String {
        switch notifications.authorizationStatus {
        case .authorized: return "Authorized"
        case .provisional: return "Provisional"
        case .ephemeral: return "Ephemeral"
        case .denied: return "Denied"
        case .notDetermined: return "Not requested"
        @unknown default: return "Unknown"
        }
    }
    private var statusColor: Color {
        switch notifications.authorizationStatus {
        case .authorized, .provisional, .ephemeral: return Theme.green
        case .denied: return Theme.red2
        default: return Theme.text3
        }
    }

    @ViewBuilder
    private var actionButton: some View {
        switch notifications.authorizationStatus {
        case .notDetermined:
            Button("Enable slip alerts") { Task { await notifications.requestAuthorization() } }
                .buttonStyle(PrimaryButtonStyle())
        case .denied:
            Button("Open Settings") { notifications.openSystemSettings() }
                .buttonStyle(GhostButtonStyle(fullWidth: true))
        default:
            EmptyView()
        }
    }
}

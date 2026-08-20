import SwiftUI
import CorePropKit

struct AccountView: View {
    @EnvironmentObject private var model: AppModel
    @EnvironmentObject private var auth: AuthManager
    @EnvironmentObject private var notifications: NotificationManager
    @Environment(\.openURL) private var openURL
    @State private var signingOut = false

    private var displayName: String {
        auth.user?.username ?? auth.user?.email ?? auth.email ?? "Account"
    }
    private var initial: String {
        String(displayName.first ?? "C").uppercased()
    }

    var body: some View {
        NavigationStack {
            List {
                profileSection
                subscriptionSection
                notificationsSection
                settingsSection
                coverageSection
                aboutSection
                signOutSection
            }
            .listStyle(.insetGrouped)
            .scrollContentBackground(.hidden)
            .background(Theme.bg.ignoresSafeArea())
            .navigationTitle("Account")
        }
        .task {
            await notifications.refreshStatus()
            await model.refreshBilling()
        }
    }

    private var profileSection: some View {
        Section {
            HStack(spacing: 14) {
                Text(initial)
                    .font(Theme.ui(22, .bold)).foregroundColor(.white)
                    .frame(width: 52, height: 52)
                    .background(Theme.primary).clipShape(Circle())
                VStack(alignment: .leading, spacing: 3) {
                    Text(displayName).font(Theme.ui(17, .bold)).foregroundColor(Theme.text)
                    if let email = auth.user?.email, email != displayName {
                        Text(email).font(Theme.ui(13)).foregroundColor(Theme.text3)
                    }
                }
                Spacer()
            }
            .listRowBackground(Theme.card)
        }
    }

    private var subscriptionSection: some View {
        Section {
            NavigationLink { SubscriptionView() } label: {
                HStack {
                    Label("Subscription", systemImage: "creditcard")
                    Spacer()
                    Text(subscriptionBadge).font(Theme.ui(13, .semibold)).foregroundColor(subscriptionColor)
                }
            }
            .listRowBackground(Theme.card)
        }
    }

    private var subscriptionBadge: String {
        guard let b = model.billing else { return "—" }
        if b.isComped { return "Comped" }
        if b.isTrialing { return "Trial" }
        if b.isUnlocked { return "Active" }
        return "Inactive"
    }
    private var subscriptionColor: Color {
        guard let b = model.billing else { return Theme.text3 }
        return b.isUnlocked ? Theme.green : Theme.red2
    }

    private var notificationsSection: some View {
        Section {
            NavigationLink { NotificationsView() } label: {
                HStack {
                    Label("Slip alerts", systemImage: "bell")
                    Spacer()
                    Text(notifStatusText).font(Theme.ui(13)).foregroundColor(Theme.text3)
                }
            }
            .listRowBackground(Theme.card)
        } footer: {
            Text("Get an Apple Push each time CoreProp auto-logs +EV slips for you.")
        }
    }

    private var notifStatusText: String {
        switch notifications.authorizationStatus {
        case .authorized, .provisional, .ephemeral: return "On"
        case .denied: return "Off"
        default: return "Not set"
        }
    }

    private var settingsSection: some View {
        Section("Automation") {
            NavigationLink { SettingsView() } label: { Label("Auto-Backtest & slip prefs", systemImage: "gearshape") }
                .listRowBackground(Theme.card)
            NavigationLink { AnalyticsView() } label: { Label("Performance", systemImage: "chart.bar.xaxis") }
                .listRowBackground(Theme.card)
        }
    }

    private var coverageSection: some View {
        Section("Coverage") {
            if let c = model.coverage {
                infoRow("Prop source", c.propSource ?? "PrizePicks")
                infoRow("Price sources", c.booksCountPhrase)
                infoRow("Leagues", (c.leagues ?? []).joined(separator: ", "))
                if let r = c.refreshMinutes { infoRow("Refreshes", "every \(r) min") }
                if let t = c.trialDays { infoRow("Free trial", "\(t) days") }
            } else {
                Text("Loading coverage…").font(Theme.ui(13)).foregroundColor(Theme.text3)
                    .listRowBackground(Theme.card)
            }
        }
    }

    private var aboutSection: some View {
        Section {
            linkRow("Privacy Policy", "doc.text") { open("/privacy") }
            linkRow("Terms of Service", "doc.plaintext") { open("/terms") }
            NavigationLink { DeveloperView() } label: { Label("Developer", systemImage: "hammer") }
                .listRowBackground(Theme.card)
            infoRow("Version", appVersion)
        } footer: {
            Text("CoreProp is an analytics tool, not betting advice. 21+. If gambling stops being fun, call 1-800-GAMBLER.")
        }
    }

    private func linkRow(_ title: String, _ icon: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack {
                Label(title, systemImage: icon).foregroundColor(Theme.text)
                Spacer()
                Image(systemName: "arrow.up.forward").font(.system(size: 12)).foregroundColor(Theme.text3)
            }
        }
        .listRowBackground(Theme.card)
    }

    private func open(_ path: String) {
        let base = model.environment.baseURL
        if let url = URL(string: base.absoluteString + path) { openURL(url) }
    }

    private var signOutSection: some View {
        Section {
            Button(role: .destructive) {
                signingOut = true
                Task { await auth.signOut(); signingOut = false }
            } label: {
                HStack {
                    if signingOut { ProgressView().controlSize(.small) }
                    Text("Sign out")
                }
            }
            .listRowBackground(Theme.card)
        }
    }

    private func infoRow(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).foregroundColor(Theme.text2)
            Spacer()
            Text(value).font(Theme.ui(14)).foregroundColor(Theme.text).multilineTextAlignment(.trailing)
        }
        .listRowBackground(Theme.card)
    }

    private var appVersion: String {
        let v = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "1.0"
        let b = Bundle.main.object(forInfoDictionaryKey: "CFBundleVersion") as? String ?? "1"
        return "\(v) (\(b))"
    }
}

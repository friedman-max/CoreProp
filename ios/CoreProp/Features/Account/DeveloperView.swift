import SwiftUI
import CorePropKit

/// Developer utilities: point the app at a different backend and inspect the
/// captured APNs token. The override takes effect on next launch (the client is
/// built once at startup).
struct DeveloperView: View {
    @EnvironmentObject private var model: AppModel
    @EnvironmentObject private var notifications: NotificationManager
    @State private var baseURLText = ""
    @State private var saved = false

    var body: some View {
        Form {
            Section("Backend") {
                LabeledContent("Active") {
                    Text(model.environment.baseURL.absoluteString)
                        .font(Theme.mono(12)).foregroundColor(Theme.text2)
                }
                .listRowBackground(Theme.card)
                TextField("https://coreprop.me", text: $baseURLText)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .keyboardType(.URL)
                    .listRowBackground(Theme.card)
                Button("Save override (restart to apply)") {
                    AppConfig.setBaseURLOverride(baseURLText.isEmpty ? nil : baseURLText)
                    saved = true
                }
                .listRowBackground(Theme.card)
                Button("Clear override → use production", role: .destructive) {
                    AppConfig.setBaseURLOverride(nil)
                    baseURLText = ""
                    saved = true
                }
                .listRowBackground(Theme.card)
                if saved {
                    Text("Saved. Fully quit and relaunch to apply.")
                        .font(Theme.ui(12)).foregroundColor(Theme.green)
                        .listRowBackground(Theme.card)
                }
            }

            Section("Push (APNs)") {
                if let token = notifications.deviceTokenHex {
                    LabeledContent("Device token") {
                        Text(token.prefix(16) + "…").font(Theme.mono(11)).foregroundColor(Theme.text2)
                    }
                    .listRowBackground(Theme.card)
                } else {
                    Text("No APNs token captured. Enable slip alerts and ensure the Push Notifications capability is provisioned.")
                        .font(Theme.ui(12)).foregroundColor(Theme.text3)
                        .listRowBackground(Theme.card)
                }
                if let err = notifications.lastError {
                    Text(err).font(Theme.ui(12)).foregroundColor(Color(hex: 0xFCA5A5))
                        .listRowBackground(Theme.card)
                }
            } footer: {
                Text("The token is uploaded to /api/push/apns/register when you're signed in. Delivery also needs the server's APNs keys + the Push Notifications capability.")
            }
        }
        .scrollContentBackground(.hidden)
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle("Developer")
        .navigationBarTitleDisplayMode(.inline)
        .onAppear {
            if baseURLText.isEmpty { baseURLText = UserDefaults.standard.string(forKey: "CorePropBaseURL") ?? "" }
        }
    }
}

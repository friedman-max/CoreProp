import Foundation
import Combine
import CorePropKit

/// Top-level app state: owns the `CoreClient`, the `AuthManager`, and the
/// coverage / billing facts that gate the UI. Injected as an `@EnvironmentObject`.
@MainActor
final class AppModel: ObservableObject {
    let environment: CoreEnvironment
    let auth: AuthManager
    let client: CoreClient

    @Published var coverage: Coverage?
    @Published var billing: BillingStatus?
    /// True when billing is enforced and the user has no access (402 territory).
    @Published var locked: Bool = false
    @Published var bootstrapError: String?

    init(environment: CoreEnvironment) {
        self.environment = environment
        let auth = AuthManager(store: KeychainSessionStore())
        self.auth = auth
        self.client = CoreClient(
            environment: environment,
            tokenProvider: { [weak auth] in await auth?.validAccessToken() }
        )
    }

    /// Launch sequence: load Supabase config, restore the session, then fetch
    /// public coverage (and billing if signed in).
    func bootstrap() async {
        do {
            let ui = try await client.uiConfig()
            if let urlStr = ui.supabaseUrl, let url = URL(string: urlStr),
               let key = ui.supabaseAnonKey, !key.isEmpty {
                auth.configure(supabaseURL: url, anonKey: key, siteURL: environment.baseURL)
            } else {
                bootstrapError = "The server did not return Supabase configuration."
            }
        } catch {
            bootstrapError = (error as? APIError)?.errorDescription ?? error.localizedDescription
        }

        await auth.restore()
        await refreshCoverage()
        if auth.phase == .signedIn { await refreshBilling() }
    }

    func refreshCoverage() async {
        coverage = try? await client.coverage()
    }

    func refreshBilling() async {
        guard auth.phase == .signedIn else { return }
        if let b = try? await client.billingStatus() {
            billing = b
            locked = !b.isUnlocked
        }
    }

    /// Call when any data endpoint returns 402: re-check billing and lock.
    func handlePaymentRequired() async {
        locked = true
        await refreshBilling()
    }
}

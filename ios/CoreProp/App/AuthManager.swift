import Foundation
import Combine
import CorePropKit

/// Owns the Supabase session lifecycle: restore-on-launch, sign in/up/out, and
/// proactive token refresh. Everything is main-actor isolated so views can bind
/// to `@Published` state directly; `validAccessToken()` is what `CoreClient`
/// calls before every request.
@MainActor
final class AuthManager: ObservableObject {
    enum Phase: Equatable { case loading, signedOut, signedIn }

    @Published private(set) var phase: Phase = .loading
    @Published private(set) var user: AuthUser?
    @Published private(set) var email: String?
    /// True after a signup that needs email confirmation (no session yet).
    @Published var pendingEmailConfirmation: Bool = false

    private let store: SessionStore
    private var authClient: SupabaseAuthClient?
    private var siteURL: URL?
    private var refreshTask: Task<String?, Never>?
    /// Bumped whenever the logical session identity changes (sign in/up/out).
    /// An in-flight refresh captures the generation at start and refuses to
    /// write its result if the generation has since changed — so a refresh that
    /// completes after sign-out can never resurrect the session (or re-persist
    /// tokens to the Keychain).
    private var sessionGeneration = 0

    private var session: AuthSession? {
        didSet {
            store.save(session)
            email = session?.user?.email ?? email
        }
    }

    init(store: SessionStore) {
        self.store = store
        self.session = store.load()
        self.user = session?.user
        self.email = session?.user?.email
    }

    /// Called once `/api/ui-config` yields the Supabase URL + anon key.
    /// `siteURL` (the backend origin, e.g. `https://coreprop.me`) is used as the
    /// email-confirmation `redirect_to` on sign-up.
    func configure(supabaseURL: URL, anonKey: String, siteURL: URL?) {
        authClient = SupabaseAuthClient(supabaseURL: supabaseURL, anonKey: anonKey)
        self.siteURL = siteURL
    }

    var isConfigured: Bool { authClient != nil }

    /// Restore a persisted session at launch: refresh if near expiry, refetch
    /// the user, and settle `phase`.
    func restore() async {
        guard session != nil else { phase = .signedOut; return }
        if session?.isExpired() == true {
            _ = await refreshIfNeeded()
        }
        guard session != nil else { phase = .signedOut; return }
        await loadUser()
        phase = session != nil ? .signedIn : .signedOut
    }

    /// A valid access token for `CoreClient`, refreshing first if it is expired
    /// or within a minute of expiring. Returns nil (and signs out) if refresh
    /// fails.
    func validAccessToken() async -> String? {
        guard let session else { return nil }
        if session.isExpired(leeway: 60) {
            return await refreshIfNeeded()
        }
        return session.accessToken
    }

    // MARK: Sign in / up / out

    func signIn(email: String, password: String) async throws {
        guard let authClient else { throw APIError.notConfigured("Sign-in isn't ready yet. Try again in a moment.") }
        let newSession = try await authClient.signIn(email: email, password: password)
        sessionGeneration &+= 1
        self.session = newSession
        self.user = newSession.user
        self.pendingEmailConfirmation = false
        await loadUser()
        phase = .signedIn
    }

    /// Returns `true` when the account was created but needs email confirmation
    /// (no session yet); `false` when a session is active and the user is in.
    @discardableResult
    func signUp(email: String, password: String, username: String?) async throws -> Bool {
        guard let authClient else { throw APIError.notConfigured("Sign-up isn't ready yet. Try again in a moment.") }
        let result = try await authClient.signUp(email: email, password: password,
                                                 username: username, emailRedirectTo: siteURL)
        if let s = result.effectiveSession {
            sessionGeneration &+= 1
            self.session = s
            self.user = s.user
            self.pendingEmailConfirmation = false
            await loadUser()
            phase = .signedIn
            return false
        } else {
            self.pendingEmailConfirmation = true
            return true
        }
    }

    func signOut() async {
        // Clear local state and bump the generation FIRST, so an in-flight
        // refresh that resumes during the remote logout can't resurrect the
        // session (its generation-guarded write becomes a no-op). Then do the
        // best-effort global logout with the captured token.
        let token = session?.accessToken
        sessionGeneration &+= 1
        refreshTask?.cancel()
        refreshTask = nil
        session = nil
        user = nil
        pendingEmailConfirmation = false
        phase = .signedOut
        if let authClient, let token {
            try? await authClient.signOut(accessToken: token)
        }
    }

    func requestPasswordReset(email: String) async throws {
        guard let authClient else { throw APIError.notConfigured("Not ready yet.") }
        try await authClient.requestPasswordReset(email: email)
    }

    // MARK: Internals

    private func loadUser() async {
        guard let authClient, let token = session?.accessToken else { return }
        if let u = try? await authClient.getUser(accessToken: token) {
            self.user = u
            self.email = u.email ?? email
        }
    }

    /// Single-flight refresh: concurrent callers share one in-flight task. The
    /// captured `gen` guards the write so a refresh that finishes after a
    /// sign-out (or a switch to a different session) cannot overwrite state.
    private func refreshIfNeeded() async -> String? {
        if let task = refreshTask { return await task.value }
        guard let authClient, let refreshToken = session?.refreshToken else { return nil }
        let gen = sessionGeneration

        let task = Task<String?, Never> { [weak self] in
            do {
                let refreshed = try await authClient.refresh(refreshToken: refreshToken)
                return await MainActor.run { [weak self] () -> String? in
                    guard let self, self.sessionGeneration == gen else { return nil }
                    self.session = refreshed
                    if let u = refreshed.user { self.user = u }
                    return refreshed.accessToken
                }
            } catch {
                // A failed refresh means the refresh token is dead — sign out,
                // but only if this refresh still owns the current session.
                await MainActor.run { [weak self] in
                    guard let self, self.sessionGeneration == gen else { return }
                    self.forceSignOutLocally()
                }
                return nil
            }
        }
        refreshTask = task
        let result = await task.value
        refreshTask = nil
        return result
    }

    private func forceSignOutLocally() {
        sessionGeneration &+= 1
        session = nil
        user = nil
        phase = .signedOut
    }
}

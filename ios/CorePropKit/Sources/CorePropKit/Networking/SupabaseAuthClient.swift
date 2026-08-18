import Foundation

/// Talks to Supabase Auth (GoTrue) REST directly — the native equivalent of
/// what `supabase-js` does for the web app. Stateless and `Sendable`: it holds
/// only the immutable base URL + anon key + an `HTTPClient`.
///
/// Every call carries the `apikey: <anon>` header (GoTrue requires it);
/// authenticated calls (`logout`, `getUser`) also carry the bearer token.
public struct SupabaseAuthClient: Sendable {
    public let baseURL: URL
    public let anonKey: String
    private let http: HTTPClient

    /// - Parameters:
    ///   - supabaseURL: e.g. `https://xyz.supabase.co` (from `/api/ui-config`).
    ///   - anonKey: the public anon key (from `/api/ui-config`).
    public init(supabaseURL: URL, anonKey: String, http: HTTPClient = HTTPClient()) {
        self.baseURL = supabaseURL
        self.anonKey = anonKey
        self.http = http
    }

    private var baseHeaders: [String: String] {
        ["apikey": anonKey, "Content-Type": "application/json", "Accept": "application/json"]
    }

    private static let encoder = JSONEncoder()   // plain: request bodies use explicit keys

    // MARK: Sign in

    public func signIn(email: String, password: String) async throws -> AuthSession {
        struct Body: Encodable { let email: String; let password: String }
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/token", method: "POST",
            query: [URLQueryItem(name: "grant_type", value: "password")],
            headers: baseHeaders,
            body: try Self.encoder.encode(Body(email: email, password: password))
        )
        return try await sendAuth(req, as: AuthSession.self)
    }

    // MARK: Sign up

    /// Creates an account. If the Supabase project requires email confirmation,
    /// the returned `SignUpResponse.needsEmailConfirmation` is true and no
    /// session is present until the user confirms.
    ///
    /// `emailRedirectTo` maps to GoTrue's `redirect_to` query param — the native
    /// analogue of the web client's `emailRedirectTo` (see `web/static/api.jsx`).
    /// Without it, GoTrue builds the confirmation link from the project's Site
    /// URL (whose default is a dead `localhost`), so confirmation emails from a
    /// project that requires confirmation would point nowhere. It must be on the
    /// project's Redirect URLs allow-list.
    public func signUp(email: String, password: String, username: String?,
                       emailRedirectTo: URL? = nil) async throws -> SignUpResponse {
        struct Body: Encodable {
            let email: String
            let password: String
            let data: [String: String]
        }
        let meta = (username?.isEmpty == false) ? ["username": username!] : [:]
        var query: [URLQueryItem] = []
        if let emailRedirectTo {
            query.append(URLQueryItem(name: "redirect_to", value: emailRedirectTo.absoluteString))
        }
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/signup", method: "POST",
            query: query, headers: baseHeaders,
            body: try Self.encoder.encode(Body(email: email, password: password, data: meta))
        )
        return try await sendAuth(req, as: SignUpResponse.self)
    }

    // MARK: Refresh

    public func refresh(refreshToken: String) async throws -> AuthSession {
        struct Body: Encodable {
            let refreshToken: String
            enum CodingKeys: String, CodingKey { case refreshToken = "refresh_token" }
        }
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/token", method: "POST",
            query: [URLQueryItem(name: "grant_type", value: "refresh_token")],
            headers: baseHeaders,
            body: try Self.encoder.encode(Body(refreshToken: refreshToken))
        )
        return try await sendAuth(req, as: AuthSession.self)
    }

    // MARK: Sign out

    /// Global sign-out invalidates the refresh token everywhere (matches the
    /// web app's `signOut({ scope: "global" })`). Best-effort: a network error
    /// here should not block local cleanup, so the caller may ignore throws.
    public func signOut(accessToken: String) async throws {
        var headers = baseHeaders
        headers["Authorization"] = "Bearer \(accessToken)"
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/logout", method: "POST",
            query: [URLQueryItem(name: "scope", value: "global")],
            headers: headers
        )
        try await http.sendVoid(req)
    }

    // MARK: Current user

    public func getUser(accessToken: String) async throws -> AuthUser {
        var headers = baseHeaders
        headers["Authorization"] = "Bearer \(accessToken)"
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/user", method: "GET", headers: headers
        )
        return try await sendAuth(req, as: AuthUser.self)
    }

    // MARK: Password recovery

    public func requestPasswordReset(email: String) async throws {
        struct Body: Encodable { let email: String }
        let req = try URLRequest.build(
            base: baseURL, path: "/auth/v1/recover", method: "POST",
            headers: baseHeaders,
            body: try Self.encoder.encode(Body(email: email))
        )
        try await http.sendVoid(req)
    }

    // MARK: Helpers

    /// Like `HTTPClient.send`, but rewrites the common auth HTTP failures into
    /// friendly `.auth(_)` messages (bad credentials, rate limits, unconfirmed).
    private func sendAuth<T: Decodable>(_ request: URLRequest, as type: T.Type) async throws -> T {
        do {
            return try await http.send(request, as: T.self)
        } catch let APIError.http(status, message) {
            throw APIError.auth(Self.friendlyAuthMessage(status: status, serverMessage: message))
        }
    }

    static func friendlyAuthMessage(status: Int, serverMessage: String?) -> String {
        if let m = serverMessage, !m.isEmpty {
            // GoTrue's own message is usually the clearest ("Invalid login
            // credentials", "User already registered", "Email not confirmed").
            return m
        }
        switch status {
        case 400: return "Invalid email or password."
        case 401, 403: return "Not authorized."
        case 422: return "Please check the email and password and try again."
        case 429: return "Too many attempts. Please wait a moment and try again."
        default: return "Authentication failed (HTTP \(status))."
        }
    }
}

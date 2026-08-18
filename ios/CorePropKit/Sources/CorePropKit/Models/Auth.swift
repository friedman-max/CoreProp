import Foundation

/// A Supabase GoTrue session (`/auth/v1/token` response). Decoded with
/// `.convertFromSnakeCase`. `expiresAt` is unix seconds.
public struct AuthSession: Codable, Sendable, Equatable {
    public let accessToken: String
    public let tokenType: String?
    public let expiresIn: Int?
    public let expiresAt: Int?
    public let refreshToken: String
    public let user: AuthUser?

    public init(accessToken: String, tokenType: String? = nil, expiresIn: Int? = nil,
                expiresAt: Int? = nil, refreshToken: String, user: AuthUser? = nil) {
        self.accessToken = accessToken; self.tokenType = tokenType
        self.expiresIn = expiresIn; self.expiresAt = expiresAt
        self.refreshToken = refreshToken; self.user = user
    }

    /// When the access token expires, if known.
    public var expiryDate: Date? { expiresAt.map { Date(timeIntervalSince1970: TimeInterval($0)) } }

    /// True when the token is expired or within `leeway` seconds of expiring,
    /// so callers refresh proactively rather than after a 401. Unknown expiry
    /// is treated as "not expired" (server 401 remains the backstop).
    public func isExpired(leeway: TimeInterval = 60, now: Date = Date()) -> Bool {
        guard let exp = expiryDate else { return false }
        return now.addingTimeInterval(leeway) >= exp
    }
}

/// A Supabase user. `user_metadata` is decoded into a small struct; unknown
/// keys are ignored by Codable, so extra metadata does not break the decode.
public struct AuthUser: Codable, Identifiable, Sendable, Equatable {
    public let id: String
    public let email: String?
    public let userMetadata: UserMetadata?
    public let createdAt: String?

    public var username: String? { userMetadata?.username }

    public init(id: String, email: String?, userMetadata: UserMetadata? = nil, createdAt: String? = nil) {
        self.id = id; self.email = email; self.userMetadata = userMetadata; self.createdAt = createdAt
    }
}

public struct UserMetadata: Codable, Sendable, Equatable {
    public let username: String?
    public init(username: String?) { self.username = username }
}

/// `/auth/v1/signup` response. When email confirmation is required Supabase
/// returns the user with **no** session — surface a "check your email" state.
public struct SignUpResponse: Codable, Sendable {
    public let session: AuthSession?
    public let accessToken: String?
    public let refreshToken: String?
    public let user: AuthUser?
    public let id: String?

    /// A ready-to-use session when the signup auto-confirmed, else nil.
    public var effectiveSession: AuthSession? {
        if let session { return session }
        if let accessToken, let refreshToken {
            return AuthSession(accessToken: accessToken, refreshToken: refreshToken, user: user)
        }
        return nil
    }

    /// True when Supabase created the account but is awaiting email confirmation.
    public var needsEmailConfirmation: Bool { effectiveSession == nil }
}

/// GoTrue error bodies vary by endpoint (`{error, error_description}`,
/// `{msg}`, `{message}`, `{error_code}`). Decoded with the shared
/// `.convertFromSnakeCase` decoder, so `error_description` → `errorDescription`
/// and `error_code` → `errorCode` automatically — no explicit `CodingKeys`
/// (which would fight the strategy).
public struct AuthErrorBody: Codable, Sendable {
    public let error: String?
    public let errorDescription: String?
    public let msg: String?
    public let message: String?
    public let errorCode: String?

    public var readableMessage: String? {
        errorDescription ?? msg ?? message ?? error
    }
}

import Foundation

/// Every failure the networking layer can surface, in a form the UI can branch
/// on. The two status codes the app treats specially are 401 (token expired /
/// invalid — trigger a refresh or sign-out) and 402 (subscription required —
/// the backend gates paid payloads with this when `BILLING_ENFORCE=true`).
public enum APIError: Error, LocalizedError, Equatable {
    /// A URL could not be built from the environment + path.
    case invalidURL(String)
    /// Non-2xx HTTP response. `message` is the server's body (truncated).
    case http(status: Int, message: String?)
    /// The response body could not be decoded into the expected type.
    case decoding(String)
    /// Underlying transport failure (offline, DNS, TLS, timeout).
    case transport(String)
    /// An auth-specific failure from Supabase GoTrue (bad credentials, etc.).
    case auth(String)
    /// A prerequisite is missing (e.g. Supabase config not loaded yet).
    case notConfigured(String)

    public var errorDescription: String? {
        switch self {
        case .invalidURL(let s):        return "Invalid URL: \(s)"
        case .http(let status, let msg):
            if let msg, !msg.isEmpty { return msg }
            return "Request failed (HTTP \(status))."
        case .decoding(let s):          return "Could not read the server response. \(s)"
        case .transport(let s):         return s
        case .auth(let s):              return s
        case .notConfigured(let s):     return s
        }
    }

    /// The access token is missing/expired/invalid — the app should try a
    /// refresh and, failing that, sign the user out.
    public var isUnauthorized: Bool {
        if case .http(let status, _) = self { return status == 401 }
        return false
    }

    /// The backend requires an active subscription for this payload.
    public var isPaymentRequired: Bool {
        if case .http(let status, _) = self { return status == 402 }
        return false
    }

    /// The per-IP rate limiter on the public endpoints tripped.
    public var isRateLimited: Bool {
        if case .http(let status, _) = self { return status == 429 }
        return false
    }
}

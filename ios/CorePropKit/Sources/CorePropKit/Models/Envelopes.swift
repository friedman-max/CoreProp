import Foundation

/// `GET /api/bootstrap/core` and `GET /api/bets`.
public struct CoreBundle: Codable, Sendable {
    public let bets: [Bet]
    public let total: Int?
    public let isScraping: Bool?
    public let lastRefresh: String?
    public let intervalMin: Int?

    public var lastRefreshDate: Date? { ISO8601Date.parse(lastRefresh) }
}

/// `GET /api/matched` — combined per-book lines.
public struct MatchedEnvelope: Codable, Sendable {
    public let matches: [MarketLine]
    public let total: Int?
    public let lastRefresh: String?
}

/// `GET /api/prizepicks` | `/api/fanduel` | `/api/draftkings` | `/api/pinnacle`.
public struct LinesEnvelope: Codable, Sendable {
    public let lines: [MarketLine]
    public let total: Int?
    public let lastRefresh: String?
}

/// `GET /api/status`.
public struct ScrapeStatus: Codable, Sendable {
    public let isScraping: Bool?
    public let lastRefresh: String?
    public let nextRefresh: String?
    public let intervalMin: Int?
    public let totalBets: Int?
    public let scrapeErrors: [String: String]?

    public var lastRefreshDate: Date? { ISO8601Date.parse(lastRefresh) }
    public var nextRefreshDate: Date? { ISO8601Date.parse(nextRefresh) }
}

/// `GET /api/ui-config` — the native analogue of `window.__COREPROP_CONFIG`.
public struct UIConfig: Codable, Sendable {
    public let supabaseUrl: String?
    public let supabaseAnonKey: String?
    public let vapidPublicKey: String?
}

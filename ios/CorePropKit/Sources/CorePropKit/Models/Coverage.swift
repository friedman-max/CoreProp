import Foundation

/// `GET /api/public/coverage` — the marketing/coverage facts the app is allowed
/// to state (no user data, no auth). Carries **no** break-even and no dollar
/// figures: the per-leg break-even is computed client-side from the payout
/// table, and prices come from `/api/billing/config`.
public struct Coverage: Codable, Sendable {
    public let propSource: String?
    public let books: [String]?
    /// "price sources" when the Novig exchange is in the set, else "sportsbooks".
    public let booksNoun: String?
    public let leagues: [String]?
    public let refreshMinutes: Int?
    public let trialDays: Int?

    /// e.g. "4 price sources" — count + the honest noun for the current set.
    public var booksCountPhrase: String {
        let n = books?.count ?? 0
        return "\(n) \(booksNoun ?? "sportsbooks")"
    }

    public init(propSource: String?, books: [String]?, booksNoun: String?,
                leagues: [String]?, refreshMinutes: Int?, trialDays: Int?) {
        self.propSource = propSource; self.books = books; self.booksNoun = booksNoun
        self.leagues = leagues; self.refreshMinutes = refreshMinutes; self.trialDays = trialDays
    }
}

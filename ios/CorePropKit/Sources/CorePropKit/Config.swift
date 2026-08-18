import Foundation

/// Points the app at a backend deployment.
///
/// Production is `https://coreprop.me` (a self-hosted server exposed through a
/// Cloudflare Tunnel — see the repo's `DEPLOY.md`). `coreprop.onrender.com` is a
/// dead mirror and must not be used as the default.
///
/// The Supabase URL and anon key are **not** hard-coded here: the client fetches
/// them at launch from `GET /api/ui-config`, exactly as the web frontend reads
/// `window.__COREPROP_CONFIG`. That keeps the anon key in one place (the server)
/// and lets a single `baseURL` switch point the whole app at a different
/// environment.
public struct CoreEnvironment: Equatable, Sendable {
    /// Base URL of the FastAPI backend, e.g. `https://coreprop.me`.
    public var baseURL: URL

    public init(baseURL: URL) {
        self.baseURL = baseURL
    }

    /// The live production backend.
    public static let production = CoreEnvironment(
        baseURL: URL(string: "https://coreprop.me")!
    )

    /// A localhost backend for development (`python main.py` serves on :8000).
    public static let localhost = CoreEnvironment(
        baseURL: URL(string: "http://127.0.0.1:8000")!
    )
}

/// App-wide constants that are not environment-specific.
public enum CorePropConstants {
    public static let appName = "CoreProp"
    public static let tagline = "Sharper props, less guesswork."

    /// PrizePicks is the prop source; these are the books devigged against it.
    /// Novig is a peer-to-peer exchange, so the honest noun is "price sources"
    /// once it is in the set — the server tells us the right noun via
    /// `/api/public/coverage`.`booksNoun`.
    public static let defaultPricingBooks = ["FanDuel", "DraftKings", "Pinnacle", "Novig"]

    /// Leagues the pipeline can cover. The live set is authoritative from
    /// `/api/public/coverage`.`leagues`; this is only a display fallback.
    public static let knownLeagues = ["NBA", "WNBA", "MLB", "NHL", "NCAAB"]
}

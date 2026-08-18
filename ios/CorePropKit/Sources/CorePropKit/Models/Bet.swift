import Foundation

/// A single +EV leg as emitted by `BetResult.to_dict` plus the pipeline extras
/// (`fd_odds_book`, …, `bet_key`) added in `web/app.py`. Decoded with
/// `.convertFromSnakeCase`, so `bet_id` → `betId`, `true_prob` → `trueProb`, etc.
///
/// `in_backtest` is intentionally **not** a field here: the payload never
/// carries it. The client joins `betKey` against `GET /api/backtest/keys`
/// locally (a `Set<String>`), exactly like the web frontend.
public struct Bet: Codable, Identifiable, Hashable, Sendable {
    public let betId: String
    public let playerName: String
    public let league: String
    public let propType: String
    public let ppLine: Double
    public let fdLine: Double?
    /// "over" / "under" (lowercased on the wire).
    public let side: String
    /// The decision probability (raw consensus + validated corrections), 0…1.
    public let trueProb: Double
    /// Untouched market consensus (the "one ruler"); may be absent on old rows.
    public let rawTrueProb: Double?
    public let marketWidth: Double?
    public let team: String?
    /// Fair American odds implied by `trueProb`. Server emits an integer; kept
    /// as `Double?` so a float never fails the decode.
    public let trueOdds: Double?
    public let edge: Double?
    public let individualEvPct: Double?
    public let overOdds: Int?
    public let underOdds: Int?
    public let bothSided: Bool?
    public let startTime: String?
    /// "standard" or "goblin" (a PrizePicks green-devil / discounted line).
    public let oddsType: String?
    public let fdOddsBook: Double?
    public let dkOddsBook: Double?
    public let pinOddsBook: Double?
    public let nvOddsBook: Double?
    /// "player|YYYY-MM-DD" — the backtest dedup key, joined client-side.
    public let betKey: String?

    public var id: String { betId }

    // MARK: Derived

    /// `trueProb` as a percentage (0…100).
    public var truePct: Double { trueProb * 100 }

    /// PrizePicks "goblin"/green-devil line: discounted, higher hit rate.
    public var isGreenDevil: Bool { (oddsType ?? "standard").lowercased() == "goblin" }

    public var isOver: Bool { side.lowercased() == "over" }
    public var sideLabel: String { side.uppercased() }

    /// Per-book American odds for the chosen side, rounded to whole numbers
    /// (the devig math produces floats). Nil books are omitted.
    public var bookOdds: [BookOdds] {
        var out: [BookOdds] = []
        if let v = fdOddsBook  { out.append(.init(book: .fanduel,    odds: Int(v.rounded()))) }
        if let v = dkOddsBook  { out.append(.init(book: .draftkings, odds: Int(v.rounded()))) }
        if let v = pinOddsBook { out.append(.init(book: .pinnacle,   odds: Int(v.rounded()))) }
        if let v = nvOddsBook  { out.append(.init(book: .novig,      odds: Int(v.rounded()))) }
        return out
    }

    public var startDate: Date? { ISO8601Date.parse(startTime) }

    public init(
        betId: String, playerName: String, league: String, propType: String,
        ppLine: Double, fdLine: Double?, side: String, trueProb: Double,
        rawTrueProb: Double?, marketWidth: Double?, team: String?, trueOdds: Double?,
        edge: Double?, individualEvPct: Double?, overOdds: Int?, underOdds: Int?,
        bothSided: Bool?, startTime: String?, oddsType: String?,
        fdOddsBook: Double?, dkOddsBook: Double?, pinOddsBook: Double?, nvOddsBook: Double?,
        betKey: String?
    ) {
        self.betId = betId; self.playerName = playerName; self.league = league
        self.propType = propType; self.ppLine = ppLine; self.fdLine = fdLine
        self.side = side; self.trueProb = trueProb; self.rawTrueProb = rawTrueProb
        self.marketWidth = marketWidth; self.team = team; self.trueOdds = trueOdds
        self.edge = edge; self.individualEvPct = individualEvPct; self.overOdds = overOdds
        self.underOdds = underOdds; self.bothSided = bothSided; self.startTime = startTime
        self.oddsType = oddsType; self.fdOddsBook = fdOddsBook; self.dkOddsBook = dkOddsBook
        self.pinOddsBook = pinOddsBook; self.nvOddsBook = nvOddsBook; self.betKey = betKey
    }
}

/// A sportsbook (or the Novig exchange) with its price for a bet's chosen side.
public struct BookOdds: Hashable, Sendable {
    public let book: Book
    public let odds: Int
    public init(book: Book, odds: Int) { self.book = book; self.odds = odds }
}

/// The price sources the pipeline devigs against. `label` matches the web
/// BookBadge chips (FD/DK/PIN/NV).
public enum Book: String, CaseIterable, Sendable {
    case fanduel, draftkings, pinnacle, novig

    public var label: String {
        switch self {
        case .fanduel:    return "FD"
        case .draftkings: return "DK"
        case .pinnacle:   return "PIN"
        case .novig:      return "NV"
        }
    }

    public var fullName: String {
        switch self {
        case .fanduel:    return "FanDuel"
        case .draftkings: return "DraftKings"
        case .pinnacle:   return "Pinnacle"
        case .novig:      return "Novig"
        }
    }
}

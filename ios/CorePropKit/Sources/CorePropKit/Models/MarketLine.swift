import Foundation

/// A generic market line for the board tabs (Combined / PrizePicks / Sportsbooks).
///
/// The shape varies by endpoint (see `web/static/api.jsx` `lineToUi`):
/// * PP:      `player_name, stat_type, line_score, side, start_time`
/// * matched: `player_name, stat_type, (pp_)line, side, fd_odds, dk_odds,
///             pin_odds, nv_odds, best_odds, true_odds, start_time`
/// * FD/DK/PIN: `player_name, stat_type, line_score, side, line_odds,
///             true_odds, start_time`
///
/// Rather than model each variant, this decodes the union of possible fields
/// (all optional, odds decoded leniently since devig produces floats) and
/// exposes the merged accessors `lineToUi` computes. Manual `init(from:)` is
/// used so a stray string/number line can't fail the whole decode.
public struct MarketLine: Codable, Identifiable, Hashable, Sendable {
    public let lineId: String?
    public let playerName: String?
    public let league: String?
    public let statType: String?
    public let propType: String?

    public let line: Double?
    public let ppLine: Double?
    public let lineScore: Double?
    public let fdLine: Double?
    public let dkLine: Double?
    public let pinLine: Double?

    public let side: String?
    public let trueProb: Double?
    public let trueOdds: Double?

    public let fdOdds: Double?
    public let dkOdds: Double?
    public let pinOdds: Double?
    public let nvOdds: Double?
    public let fdOddsBook: Double?
    public let dkOddsBook: Double?
    public let pinOddsBook: Double?
    public let nvOddsBook: Double?
    public let lineOdds: Double?
    public let bestOdds: Double?

    public let startTime: String?

    // MARK: Derived (mirrors `lineToUi`)

    public var prop: String { statType ?? propType ?? "" }

    /// The single line value, using whichever field the endpoint carries.
    public var lineValue: Double? {
        line ?? ppLine ?? lineScore ?? fdLine ?? dkLine ?? pinLine
    }

    public var sideLabel: String { (side ?? "").uppercased() }
    public var truePct: Double? { trueProb.map { $0 * 100 } }

    public var fd: Int?  { round(fdOdds ?? fdOddsBook) }
    public var dk: Int?  { round(dkOdds ?? dkOddsBook) }
    public var pin: Int? { round(pinOdds ?? pinOddsBook) }
    public var nv: Int?  { round(nvOdds ?? nvOddsBook) }
    /// Single-book rows carry one price in `line_odds`.
    public var bookOdds: Int? { round(lineOdds) }
    public var best: Int? { round(bestOdds) }
    public var trueOddsInt: Int? { round(trueOdds) }

    public var startDate: Date? { ISO8601Date.parse(startTime) }

    /// Stable identity for lists: the server id if present, else a composite.
    public var id: String {
        lineId ?? "\(playerName ?? "")|\(prop)|\(lineValue.map { String($0) } ?? "")|\(sideLabel)"
    }

    private func round(_ v: Double?) -> Int? { v.map { Int($0.rounded()) } }

    enum CodingKeys: String, CodingKey {
        case lineId, playerName, league, statType, propType
        case line, ppLine, lineScore, fdLine, dkLine, pinLine
        case side, trueProb, trueOdds
        case fdOdds, dkOdds, pinOdds, nvOdds
        case fdOddsBook, dkOddsBook, pinOddsBook, nvOddsBook
        case lineOdds, bestOdds, startTime
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        lineId      = c.decodeLenientString(forKey: .lineId)
        playerName  = try? c.decodeIfPresent(String.self, forKey: .playerName)
        league      = try? c.decodeIfPresent(String.self, forKey: .league)
        statType    = try? c.decodeIfPresent(String.self, forKey: .statType)
        propType    = try? c.decodeIfPresent(String.self, forKey: .propType)
        line        = c.decodeLenientDouble(forKey: .line)
        ppLine      = c.decodeLenientDouble(forKey: .ppLine)
        lineScore   = c.decodeLenientDouble(forKey: .lineScore)
        fdLine      = c.decodeLenientDouble(forKey: .fdLine)
        dkLine      = c.decodeLenientDouble(forKey: .dkLine)
        pinLine     = c.decodeLenientDouble(forKey: .pinLine)
        side        = try? c.decodeIfPresent(String.self, forKey: .side)
        trueProb    = c.decodeLenientDouble(forKey: .trueProb)
        trueOdds    = c.decodeLenientDouble(forKey: .trueOdds)
        fdOdds      = c.decodeLenientDouble(forKey: .fdOdds)
        dkOdds      = c.decodeLenientDouble(forKey: .dkOdds)
        pinOdds     = c.decodeLenientDouble(forKey: .pinOdds)
        nvOdds      = c.decodeLenientDouble(forKey: .nvOdds)
        fdOddsBook  = c.decodeLenientDouble(forKey: .fdOddsBook)
        dkOddsBook  = c.decodeLenientDouble(forKey: .dkOddsBook)
        pinOddsBook = c.decodeLenientDouble(forKey: .pinOddsBook)
        nvOddsBook  = c.decodeLenientDouble(forKey: .nvOddsBook)
        lineOdds    = c.decodeLenientDouble(forKey: .lineOdds)
        bestOdds    = c.decodeLenientDouble(forKey: .bestOdds)
        startTime   = try? c.decodeIfPresent(String.self, forKey: .startTime)
    }
}

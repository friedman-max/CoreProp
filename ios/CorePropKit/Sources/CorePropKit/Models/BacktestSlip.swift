import Foundation

/// The per-leg settlement state. `dnp` = did-not-play; `push` = tie/void — both
/// are excluded from the effective-leg denominator when scoring a slip.
public enum LegResult: String, Codable, Sendable {
    case pending, hit, miss, push, dnp

    public init(raw: String?) {
        self = LegResult(rawValue: (raw ?? "pending").lowercased()) ?? .pending
    }
}

/// One leg of a logged backtest slip (`GET /api/backtest/slips`). `line` and
/// `stat_actual` are decoded leniently — older rows stored them as strings.
public struct SlipLeg: Codable, Identifiable, Hashable, Sendable {
    public let slipId: String?
    public let legNum: Int?
    public let player: String?
    public let league: String?
    public let prop: String?
    public let line: Double?
    public let side: String?
    public let trueProb: Double?
    public let resultRaw: String?
    public let statActual: Double?
    public let gameStart: String?

    public var result: LegResult { LegResult(raw: resultRaw) }
    public var sideLabel: String { (side ?? "").uppercased() }
    public var truePct: Double? { trueProb.map { $0 * 100 } }
    public var gameStartDate: Date? { ISO8601Date.parse(gameStart) }

    public var id: String { "\(slipId ?? "")-\(legNum ?? 0)-\(player ?? "")" }

    enum CodingKeys: String, CodingKey {
        case slipId, legNum, player, league, prop, line, side
        case trueProb, resultRaw = "result", statActual, gameStart
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        slipId     = c.decodeLenientString(forKey: .slipId)
        legNum     = c.decodeLenientInt(forKey: .legNum)
        player     = try? c.decodeIfPresent(String.self, forKey: .player)
        league     = try? c.decodeIfPresent(String.self, forKey: .league)
        prop       = try? c.decodeIfPresent(String.self, forKey: .prop)
        line       = c.decodeLenientDouble(forKey: .line)
        side       = try? c.decodeIfPresent(String.self, forKey: .side)
        trueProb   = c.decodeLenientDouble(forKey: .trueProb)
        resultRaw  = try? c.decodeIfPresent(String.self, forKey: .resultRaw)
        statActual = c.decodeLenientDouble(forKey: .statActual)
        gameStart  = try? c.decodeIfPresent(String.self, forKey: .gameStart)
    }

    public init(slipId: String?, legNum: Int?, player: String?, league: String?,
                prop: String?, line: Double?, side: String?, trueProb: Double?,
                resultRaw: String?, statActual: Double?, gameStart: String?) {
        self.slipId = slipId; self.legNum = legNum; self.player = player
        self.league = league; self.prop = prop; self.line = line; self.side = side
        self.trueProb = trueProb; self.resultRaw = resultRaw
        self.statActual = statActual; self.gameStart = gameStart
    }
}

/// A logged backtest slip header (`GET /api/backtest/slips`). The server also
/// returns `payout`/`hits`/`completed`, but — matching `page-backtest.jsx` —
/// the client recomputes those from the legs (see `BacktestScoring`) so the
/// payout table is the single ruler.
public struct BacktestSlip: Codable, Identifiable, Hashable, Sendable {
    public let id: String
    public let timestamp: String?
    public let slipType: String?
    public let nLegs: Int?
    public let projSlipEvPct: Double?
    public let legs: [SlipLeg]

    public var timestampDate: Date? { ISO8601Date.parse(timestamp) }

    /// "Power" / "Flex" / "Manual" as stored. Manual slips score as Flex
    /// (partial credit) unless exactly 2 legs, where both degenerate to Power-2.
    public var slipTypeLabel: String { slipType ?? "Manual" }

    enum CodingKeys: String, CodingKey {
        case id, timestamp, slipType, nLegs, projSlipEvPct, legs
        // `slip_id` (== id) and the server's advisory payout/hits/completed are
        // intentionally not decoded.
    }

    public init(id: String, timestamp: String?, slipType: String?, nLegs: Int?,
                projSlipEvPct: Double?, legs: [SlipLeg]) {
        self.id = id; self.timestamp = timestamp; self.slipType = slipType
        self.nLegs = nLegs; self.projSlipEvPct = projSlipEvPct; self.legs = legs
    }
}

/// `{slips, total}` wrapper.
public struct BacktestSlipsEnvelope: Codable, Sendable {
    public let slips: [BacktestSlip]
    public let total: Int?
}

/// `{keys}` — `"player|YYYY-MM-DD"` join keys already logged by this user.
public struct BacktestKeysEnvelope: Codable, Sendable {
    public let keys: [String]
}

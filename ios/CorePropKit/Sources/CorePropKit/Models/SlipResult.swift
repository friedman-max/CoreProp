import Foundation

/// `POST /api/slip` and `POST /api/slip/auto` — server-computed slip EV
/// (correlation-aware Monte Carlo). `optimalK`/`optimalBetIds` are present only
/// on the `/auto` response.
public struct SlipResult: Codable, Sendable {
    public let nPicks: Int?
    public let powerEvPct: Double?
    public let flexEvPct: Double?
    public let bestPlayType: String?
    public let bestEvPct: Double?
    public let expectedProfit: Double?
    public let bankroll: Double?
    public let legs: [SlipResultLeg]?
    public let optimalK: Int?
    public let optimalBetIds: [String]?
}

public struct SlipResultLeg: Codable, Sendable {
    public let playerName: String?
    public let propType: String?
    public let ppLine: Double?
    public let side: String?
    public let trueProb: Double?
    public let indEvPct: Double?
}

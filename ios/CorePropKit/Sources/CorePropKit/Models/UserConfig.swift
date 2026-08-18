import Foundation

/// `GET /api/config` — the signed-in user's settings (merged over server
/// defaults). Note `interval_min`/`min_ev_pct`/`active_leagues` on `POST
/// /api/config` mutate **global** pipeline state, not this per-user row; the
/// per-user auto-backtest prefs persist via `/api/user/*`.
public struct UserConfig: Codable, Sendable {
    public var intervalMin: Int?
    public var minEvPct: Double?
    public var activeLeagues: [String: Bool]?
    public var autoBacktest: Bool?
    public var autoSlipType: String?
    public var autoSlipLegs: Int?
    public var autoSlipMinProb: Double?
    public var autoBacktestGreenDevils: Bool?

    public init(
        intervalMin: Int? = nil, minEvPct: Double? = nil,
        activeLeagues: [String: Bool]? = nil, autoBacktest: Bool? = nil,
        autoSlipType: String? = nil, autoSlipLegs: Int? = nil,
        autoSlipMinProb: Double? = nil, autoBacktestGreenDevils: Bool? = nil
    ) {
        self.intervalMin = intervalMin; self.minEvPct = minEvPct
        self.activeLeagues = activeLeagues; self.autoBacktest = autoBacktest
        self.autoSlipType = autoSlipType; self.autoSlipLegs = autoSlipLegs
        self.autoSlipMinProb = autoSlipMinProb
        self.autoBacktestGreenDevils = autoBacktestGreenDevils
    }
}

/// Request body for `POST /api/user/slip-prefs`. `autoSlipType` must be
/// "Power"/"Flex"; legs 2…6 (Flex ≥ 3); minProb strictly in (0,1) if present.
public struct SlipPrefsUpdate: Codable, Sendable {
    public var autoSlipType: String
    public var autoSlipLegs: Int
    public var autoSlipMinProb: Double?
    public var autoBacktestGreenDevils: Bool?

    public init(autoSlipType: String, autoSlipLegs: Int,
                autoSlipMinProb: Double? = nil, autoBacktestGreenDevils: Bool? = nil) {
        self.autoSlipType = autoSlipType; self.autoSlipLegs = autoSlipLegs
        self.autoSlipMinProb = autoSlipMinProb
        self.autoBacktestGreenDevils = autoBacktestGreenDevils
    }
}

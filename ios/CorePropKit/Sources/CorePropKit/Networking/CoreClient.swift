import Foundation

/// Which board a `MarketLine` list comes from.
public enum BoardSource: String, CaseIterable, Sendable {
    case combined, prizepicks, fanduel, draftkings, pinnacle

    var path: String {
        switch self {
        case .combined:   return "/api/matched"
        case .prizepicks: return "/api/prizepicks"
        case .fanduel:    return "/api/fanduel"
        case .draftkings: return "/api/draftkings"
        case .pinnacle:   return "/api/pinnacle"
        }
    }

    public var title: String {
        switch self {
        case .combined:   return "Combined"
        case .prizepicks: return "PrizePicks"
        case .fanduel:    return "FanDuel"
        case .draftkings: return "DraftKings"
        case .pinnacle:   return "Pinnacle"
        }
    }
}

/// The FastAPI backend client. Stateless + `Sendable`: it holds the environment
/// and a `tokenProvider` closure that returns the current (already-refreshed)
/// Supabase access token, which is attached as `Authorization: Bearer …` when
/// present. Token lifecycle lives in the app's `AuthManager`.
public struct CoreClient: Sendable {
    public let environment: CoreEnvironment
    private let http: HTTPClient
    private let tokenProvider: @Sendable () async -> String?

    public init(
        environment: CoreEnvironment,
        http: HTTPClient = HTTPClient(),
        tokenProvider: @escaping @Sendable () async -> String? = { nil }
    ) {
        self.environment = environment
        self.http = http
        self.tokenProvider = tokenProvider
    }

    private static let encoder = JSONEncoder()  // plain: bodies use explicit snake_case keys

    private func headers() async -> [String: String] {
        var h = ["Accept": "application/json"]
        if let token = await tokenProvider() {
            h["Authorization"] = "Bearer \(token)"
        }
        return h
    }

    private func get<T: Decodable>(_ path: String, as type: T.Type,
                                   query: [URLQueryItem] = []) async throws -> T {
        let req = try URLRequest.build(base: environment.baseURL, path: path,
                                       method: "GET", query: query, headers: await headers())
        return try await http.send(req, as: T.self)
    }

    private func post<T: Decodable>(_ path: String, body: Encodable, as type: T.Type) async throws -> T {
        let req = try URLRequest.build(base: environment.baseURL, path: path, method: "POST",
                                       headers: await headers(), body: try Self.encoder.encode(body))
        return try await http.send(req, as: T.self)
    }

    private func postVoid(_ path: String, body: Encodable) async throws {
        let req = try URLRequest.build(base: environment.baseURL, path: path, method: "POST",
                                       headers: await headers(), body: try Self.encoder.encode(body))
        try await http.sendVoid(req)
    }

    // MARK: Config / coverage / status

    public func uiConfig() async throws -> UIConfig { try await get("/api/ui-config", as: UIConfig.self) }
    public func coverage() async throws -> Coverage { try await get("/api/public/coverage", as: Coverage.self) }
    public func status() async throws -> ScrapeStatus { try await get("/api/status", as: ScrapeStatus.self) }

    // MARK: Bets / lines

    /// First-paint payload: bets + meta.
    public func bootstrapCore() async throws -> CoreBundle {
        try await get("/api/bootstrap/core", as: CoreBundle.self)
    }
    public func bets() async throws -> CoreBundle { try await get("/api/bets", as: CoreBundle.self) }

    public func lines(_ source: BoardSource) async throws -> [MarketLine] {
        if source == .combined {
            return try await get(source.path, as: MatchedEnvelope.self).matches
        }
        return try await get(source.path, as: LinesEnvelope.self).lines
    }

    // MARK: Backtest

    public func backtestKeys() async throws -> [String] {
        try await get("/api/backtest/keys", as: BacktestKeysEnvelope.self).keys
    }
    public func backtestSlips() async throws -> [BacktestSlip] {
        try await get("/api/backtest/slips", as: BacktestSlipsEnvelope.self).slips
    }

    /// Logs a manual slip. The server responds with `{"slip": {...}}`, but the
    /// app reloads `/api/backtest/slips` afterwards, so the body is not decoded
    /// here — a non-throwing return means success.
    public func addSlip(betIds: [String], slipType: String) async throws {
        struct Body: Encodable {
            let betIds: [String]; let slipType: String
            enum CodingKeys: String, CodingKey { case betIds = "bet_ids", slipType = "slip_type" }
        }
        try await postVoid("/api/backtest/add-slip", body: Body(betIds: betIds, slipType: slipType))
    }

    public func deleteSlip(id: String) async throws {
        let req = try URLRequest.build(base: environment.baseURL,
                                       path: "/api/backtest/slip/\(id)", method: "DELETE",
                                       headers: await headers())
        try await http.sendVoid(req)
    }

    // MARK: User config

    public func userConfig() async throws -> UserConfig { try await get("/api/config", as: UserConfig.self) }

    /// `POST /api/config` — note this mutates **global** pipeline state, not the
    /// per-user row (kept for parity with the web app). `activeLeagues` uses a
    /// plain encoder so keys like "NBA" are preserved verbatim.
    public func updateGlobalConfig(intervalMin: Int? = nil, minEvPct: Double? = nil,
                                   activeLeagues: [String: Bool]? = nil) async throws {
        struct Body: Encodable {
            let intervalMin: Int?; let minEvPct: Double?; let activeLeagues: [String: Bool]?
            enum CodingKeys: String, CodingKey {
                case intervalMin = "interval_min", minEvPct = "min_ev_pct", activeLeagues = "active_leagues"
            }
        }
        try await postVoid("/api/config",
                           body: Body(intervalMin: intervalMin, minEvPct: minEvPct, activeLeagues: activeLeagues))
    }

    public func setAutoBacktest(_ on: Bool) async throws {
        struct Body: Encodable {
            let autoBacktest: Bool
            enum CodingKeys: String, CodingKey { case autoBacktest = "auto_backtest" }
        }
        try await postVoid("/api/user/auto-backtest", body: Body(autoBacktest: on))
    }

    public func setSlipPrefs(_ prefs: SlipPrefsUpdate) async throws {
        struct Body: Encodable {
            let autoSlipType: String; let autoSlipLegs: Int
            let autoSlipMinProb: Double?; let autoBacktestGreenDevils: Bool?
            enum CodingKeys: String, CodingKey {
                case autoSlipType = "auto_slip_type", autoSlipLegs = "auto_slip_legs"
                case autoSlipMinProb = "auto_slip_min_prob"
                case autoBacktestGreenDevils = "auto_backtest_green_devils"
            }
        }
        try await postVoid("/api/user/slip-prefs", body: Body(
            autoSlipType: prefs.autoSlipType, autoSlipLegs: prefs.autoSlipLegs,
            autoSlipMinProb: prefs.autoSlipMinProb, autoBacktestGreenDevils: prefs.autoBacktestGreenDevils))
    }

    // MARK: Slip EV (server, correlation-aware)

    public func buildSlip(betIds: [String], bankroll: Double = 100) async throws -> SlipResult {
        struct Body: Encodable { let betIds: [String]; let bankroll: Double
            enum CodingKeys: String, CodingKey { case betIds = "bet_ids", bankroll } }
        return try await post("/api/slip", body: Body(betIds: betIds, bankroll: bankroll), as: SlipResult.self)
    }

    public func autoBuildSlip(betIds: [String], bankroll: Double = 100) async throws -> SlipResult {
        struct Body: Encodable { let betIds: [String]; let bankroll: Double
            enum CodingKeys: String, CodingKey { case betIds = "bet_ids", bankroll } }
        return try await post("/api/slip/auto", body: Body(betIds: betIds, bankroll: bankroll), as: SlipResult.self)
    }

    // MARK: Billing

    public func billingConfig() async throws -> BillingConfig { try await get("/api/billing/config", as: BillingConfig.self) }
    public func billingStatus() async throws -> BillingStatus { try await get("/api/billing/status", as: BillingStatus.self) }

    public func checkout(plan: String) async throws -> CheckoutResponse {
        struct Body: Encodable { let plan: String }
        return try await post("/api/billing/checkout", body: Body(plan: plan), as: CheckoutResponse.self)
    }

    public func billingPortal() async throws -> PortalResponse {
        struct Empty: Encodable {}
        return try await post("/api/billing/portal", body: Empty(), as: PortalResponse.self)
    }
}

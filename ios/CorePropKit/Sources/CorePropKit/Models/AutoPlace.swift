import Foundation

/// `GET /api/auto-place/status` — the current auto-place arming state + caps.
/// Auto-placement is a companion **desktop browser-extension** feature: it
/// stages slips on PrizePicks for the armed stake. This app arms/configures it;
/// the extension does the staging on a PC.
public struct AutoPlaceStatus: Codable, Sendable {
    public let armed: Bool?
    public let mode: String?            // "off" | "paper" | "live"
    public let stake: Double?
    public let maxStake: Double?
    public let dailyCap: Double?
    public let spentToday: Double?
    public let remainingToday: Double?
    public let failStreak: Int?
    /// Non-nil when the bot is stood down; also carries "auto-placement is
    /// disabled server-side" when the operator hasn't enabled the feature.
    public let blockedReason: String?

    public var modeValue: String { (mode ?? "off").lowercased() }
    public var isArmed: Bool { armed ?? false }
    /// The operator hasn't enabled auto-place on the server at all.
    public var disabledServerSide: Bool {
        (blockedReason ?? "").lowercased().contains("disabled server-side")
    }
}

/// Request body for `POST /api/user/auto-place-prefs`.
public struct AutoPlacePrefsUpdate: Codable, Sendable {
    public var mode: String             // "off" | "paper" | "live"
    public var stake: Double?
    public var dailyCap: Double?
    public var consent: Bool?

    public init(mode: String, stake: Double? = nil, dailyCap: Double? = nil, consent: Bool? = nil) {
        self.mode = mode; self.stake = stake; self.dailyCap = dailyCap; self.consent = consent
    }
}

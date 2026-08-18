import Foundation

/// `GET /api/billing/status`. `active` is the single unlock flag the UI gates
/// on — a `comped` account has `status == nil` but `active == true`. There is
/// no `trial_ends_at`: a trial's end is `status == "trialing"` + `current_period_end`.
public struct BillingStatus: Codable, Sendable {
    public let status: String?
    public let plan: String?
    public let currentPeriodEnd: String?
    public let active: Bool?
    public let comped: Bool?
    public let enforce: Bool?
    public let configured: Bool?

    /// Whether the app is unlocked. Defaults to `true` so an unconfigured /
    /// unreachable backend never falsely locks the user out (mirrors the web
    /// client's `{active:true}` fallback).
    public var isUnlocked: Bool { active ?? true }
    public var isComped: Bool { comped ?? false }
    public var isTrialing: Bool { (status ?? "").lowercased() == "trialing" }
    public var currentPeriodEndDate: Date? { ISO8601Date.parse(currentPeriodEnd) }

    public init(status: String?, plan: String?, currentPeriodEnd: String?,
                active: Bool?, comped: Bool?, enforce: Bool?, configured: Bool?) {
        self.status = status; self.plan = plan; self.currentPeriodEnd = currentPeriodEnd
        self.active = active; self.comped = comped; self.enforce = enforce
        self.configured = configured
    }
}

/// `GET /api/billing/config` — publishable info for rendering the CTA.
public struct BillingConfig: Codable, Sendable {
    public let enabled: Bool?
    public let enforce: Bool?
    public let publishableKey: String?
    public let priceMonthly: String?
    public let priceYearly: String?
    public let trialDays: Int?
}

/// `POST /api/billing/checkout` → hosted Stripe Checkout URL.
public struct CheckoutResponse: Codable, Sendable {
    public let url: String?
    public let id: String?
}

/// `POST /api/billing/portal` → Stripe Customer Portal URL.
public struct PortalResponse: Codable, Sendable {
    public let url: String?
}

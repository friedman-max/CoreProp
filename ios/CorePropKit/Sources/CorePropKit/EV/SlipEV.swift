import Foundation

/// Client-side slip EV, mirroring the independence formulas in
/// `engine/ev_calculator.py` and the JSX calculator in `web/static/ev-page.jsx`.
///
/// This is the *independence* model (no same-game correlation). The server's
/// `/api/slip` applies a correlation-aware Gaussian-copula Monte Carlo on top;
/// the app uses this for instant local feedback and offers the server call as
/// an optional "optimize" refinement.
public enum SlipEV {
    /// Power EV: `∏p · payout − 1`. Nil when the leg count is unsupported.
    public static func powerEV(_ probs: [Double]) -> Double? {
        guard let payout = Payouts.power[probs.count] else { return nil }
        let combined = probs.reduce(1.0, *)
        return combined * payout - 1.0
    }

    /// Flex EV via full enumeration over all `2^n` independent outcomes.
    ///
    /// `n == 2` has no Flex table (it degenerates to Power-2), so — matching
    /// both web frontends — it short-circuits to `powerEV`. Nil for other
    /// unsupported sizes.
    public static func flexEV(_ probs: [Double]) -> Double? {
        let n = probs.count
        if n == 2 { return powerEV(probs) }
        guard let tiers = Payouts.flex[n] else { return nil }

        var ev = -1.0  // cost of the bet
        for mask in 0..<(1 << n) {
            var prob = 1.0
            var k = 0
            for i in 0..<n {
                if (mask >> i) & 1 == 1 { prob *= probs[i]; k += 1 }
                else { prob *= (1.0 - probs[i]) }
            }
            if let pay = tiers[k] { ev += prob * pay }
        }
        return ev
    }

    /// EV for the given slip type, or nil if unsupported for this leg count.
    public static func ev(_ probs: [Double], type: SlipType) -> Double? {
        switch type {
        case .power: return powerEV(probs)
        case .flex:  return flexEV(probs)
        }
    }

    /// The better of Power / Flex for these legs, matching `calculate_slip`'s
    /// tie-break (Power wins ties). Nil when neither type is supported.
    public static func best(_ probs: [Double]) -> (type: SlipType, ev: Double)? {
        let p = powerEV(probs)
        let f = flexEV(probs)
        switch (p, f) {
        case let (.some(pe), .some(fe)): return pe >= fe ? (.power, pe) : (.flex, fe)
        case let (.some(pe), .none):     return (.power, pe)
        case let (.none, .some(fe)):     return (.flex, fe)
        default:                          return nil
        }
    }

    /// The per-leg break-even for `(n, type)`, falling back to the legacy
    /// 6-Power ruler for unlisted pairs (matches `per_leg_break_even`).
    public static func breakEven(n: Int, type: SlipType) -> Double {
        Payouts.breakEven[PayoutKey(n, type)] ?? Payouts.optimalBreakEven
    }

    /// Context-aware per-leg EV%: `p / breakEven(n, type) − 1` (matches
    /// `engine.constants.score_leg`). This is the leg's EV as a fraction of
    /// stake given the slip it will deploy in.
    public static func scoreLeg(_ p: Double, n: Int = 6, type: SlipType = .power) -> Double {
        let be = breakEven(n: n, type: type)
        guard be > 0 else { return 0 }
        return p / be - 1.0
    }
}

import Foundation

/// PrizePicks slip type.
public enum SlipType: String, CaseIterable, Sendable {
    case power = "Power"
    case flex = "Flex"

    public var apiValue: String { rawValue }
}

/// The PrizePicks payout tables — the single ruler, mirrored from
/// `engine/constants.py` (`POWER_PAYOUTS`, `FLEX_PAYOUTS`, `BREAK_EVEN`) and the
/// two JSX mirrors (`ev-page.jsx`, `page-backtest.jsx`). PrizePicks lowered
/// 6-Power from 40x to 37.5x; break-evens move with the table.
///
/// `CorePropKitVerify` re-derives `breakEven` from these payouts (closed form
/// for Power, bisection for Flex), the same contract
/// `tests/engine_tests/test_payout_table_mirror.py` enforces server-side, so a
/// typo in either table fails verification.
public enum Payouts {
    /// n_picks → multiplier (all legs must hit).
    public static let power: [Int: Double] = [
        2: 3.0,
        3: 6.0,
        4: 10.0,
        5: 20.0,
        6: 37.5,
    ]

    /// n_picks → (k_correct → multiplier). Missing k pays 0. There is no
    /// 2-leg Flex — it degenerates to Power-2 (see `flexEV`).
    public static let flex: [Int: [Int: Double]] = [
        3: [2: 1.0, 3: 3.0],
        4: [3: 1.5, 4: 6.0],
        5: [3: 0.4, 4: 2.0, 5: 10.0],
        6: [4: 0.4, 5: 2.0, 6: 25.0],
    ]

    /// Per-leg break-even probability, derived from the payouts above. Power is
    /// closed form `p = (1/payout)^(1/n)`; Flex is solved numerically where
    /// `E[payout] = 1`. Rounded to 4 dp to match `engine/constants.BREAK_EVEN`.
    public static let breakEven: [PayoutKey: Double] = [
        PayoutKey(2, .power): 0.5774,
        PayoutKey(3, .power): 0.5503,
        PayoutKey(3, .flex):  0.5774,
        PayoutKey(4, .power): 0.5623,
        PayoutKey(4, .flex):  0.5503,
        PayoutKey(5, .power): 0.5493,
        PayoutKey(5, .flex):  0.5425,
        PayoutKey(6, .power): 0.5466,
        PayoutKey(6, .flex):  0.5421,
    ]

    /// The legacy 6-Power per-leg break-even, used as the fallback ruler when a
    /// (n, type) pair has no explicit entry — matches `OPTIMAL_BREAK_EVEN`.
    public static let optimalBreakEven = 1.0 / 1.849  // ≈ 0.54083

    /// Supported Power slip sizes, ascending.
    public static var powerSizes: [Int] { power.keys.sorted() }
    /// Supported Flex slip sizes, ascending (3…6).
    public static var flexSizes: [Int] { flex.keys.sorted() }
}

/// Hashable key for the break-even table: `(n_picks, slipType)`.
public struct PayoutKey: Hashable, Sendable {
    public let n: Int
    public let type: SlipType
    public init(_ n: Int, _ type: SlipType) { self.n = n; self.type = type }
}

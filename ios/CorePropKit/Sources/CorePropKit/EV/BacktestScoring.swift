import Foundation

/// The recomputed outcome of a logged slip. Matches `page-backtest.jsx`
/// `btComputeSlipOutcome` (and the server's advisory copy in
/// `get_backtest_slips`), computed from the legs against the payout tables —
/// the client ignores the server's `payout`/`hits`/`completed` fields so the
/// payout table stays the single ruler.
public struct SlipOutcome: Equatable, Sendable {
    /// All legs resolved (every leg is hit/miss/push/dnp).
    public let completed: Bool
    /// Multiplier returned (includes stake): 3.0 means 3× the stake back.
    /// Nil until the slip is completed.
    public let payout: Double?
    /// Effective hits (excludes push/dnp). Nil until completed.
    public let hits: Int?
    /// Effective leg count (excludes push/dnp).
    public let effectiveLegs: Int
    /// Slip settlement bucket for tinting.
    public let status: SlipStatus

    public var isWin: Bool { (payout ?? 0) > 0 && completed }
    /// Net profit as a fraction of a 1-unit stake (payout − 1). Nil until done.
    public var netProfitUnits: Double? { payout.map { $0 - 1.0 } }
}

public enum SlipStatus: String, Sendable {
    case win, loss, push, pending
}

public enum BacktestScoring {
    /// Score a slip's legs. `slipType` is "Power"/"Flex"/"Manual" (Manual and
    /// anything non-Power is treated as Flex, matching the web).
    public static func outcome(legs: [SlipLeg], slipType: String?) -> SlipOutcome {
        let results = legs.map { $0.result }
        let completed = !results.isEmpty && results.allSatisfy {
            $0 == .hit || $0 == .miss || $0 == .push || $0 == .dnp
        }
        let isPower = (slipType ?? "").lowercased() == "power"

        guard completed else {
            return SlipOutcome(completed: false, payout: nil, hits: nil,
                               effectiveLegs: 0, status: .pending)
        }

        // Push/dnp legs drop out of the effective slip entirely.
        let effective = results.filter { $0 != .push && $0 != .dnp }
        let nEff = effective.count
        let hitsEff = effective.filter { $0 == .hit }.count

        let payout: Double
        if nEff < 2 {
            // 0 effective legs (all void) refunds; a lone surviving hit refunds.
            payout = (nEff == 0 || (nEff == 1 && hitsEff == 1)) ? 1.0 : 0.0
        } else if isPower {
            payout = hitsEff == nEff ? (Payouts.power[nEff] ?? 0) : 0
        } else { // flex (and Manual)
            if nEff == 2 {
                payout = hitsEff == 2 ? (Payouts.power[2] ?? 0) : 0
            } else {
                payout = Payouts.flex[nEff]?[hitsEff] ?? 0
            }
        }

        let status: SlipStatus
        if payout > 1.0 { status = .win }
        else if payout == 1.0 { status = .push }   // full refund → treated as push
        else { status = .loss }

        return SlipOutcome(completed: true, payout: payout, hits: hitsEff,
                           effectiveLegs: nEff, status: status)
    }

    public static func outcome(for slip: BacktestSlip) -> SlipOutcome {
        outcome(legs: slip.legs, slipType: slip.slipType)
    }
}

/// Aggregate performance over a set of logged slips, for the Backtest summary
/// strip (Slip Hit Rate, Leg Hit Rate, Actual ROI).
public struct BacktestSummary: Sendable {
    public let completedSlips: Int
    public let slipWins: Int
    public let legHits: Int
    public let legDecisions: Int   // hit + miss (push/dnp/pending excluded)
    public let roi: Double?        // (Σpayout − n) / n over completed slips

    /// slipWins / completedSlips, or nil when nothing is settled.
    public var slipHitRate: Double? {
        completedSlips > 0 ? Double(slipWins) / Double(completedSlips) : nil
    }
    /// legHits / legDecisions, or nil when nothing is settled.
    public var legHitRate: Double? {
        legDecisions > 0 ? Double(legHits) / Double(legDecisions) : nil
    }

    public static func compute(_ slips: [BacktestSlip]) -> BacktestSummary {
        var completed = 0, wins = 0, legHits = 0, legDecisions = 0
        var payoutSum = 0.0
        for slip in slips {
            let o = BacktestScoring.outcome(for: slip)
            if o.completed {
                completed += 1
                if o.isWin { wins += 1 }
                payoutSum += o.payout ?? 0
                for leg in slip.legs {
                    switch leg.result {
                    case .hit:  legHits += 1; legDecisions += 1
                    case .miss: legDecisions += 1
                    default: break  // push/dnp/pending excluded
                    }
                }
            }
        }
        let roi = completed > 0 ? (payoutSum - Double(completed)) / Double(completed) : nil
        return BacktestSummary(completedSlips: completed, slipWins: wins,
                               legHits: legHits, legDecisions: legDecisions, roi: roi)
    }
}

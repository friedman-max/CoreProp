import Foundation

/// `GET /api/analytics` (`engine/calibration.py::evaluate_analytics`): calibration
/// metrics + a cumulative P&L timeline + CLV + per-leg arrays. All fields are
/// optional/lenient so a partial or empty payload never fails the decode.
public struct AnalyticsData: Codable, Sendable {
    // Calibration / accuracy.
    public let brierScore: Double?
    public let logLoss: Double?
    public let nResolved: Int?
    public let nWon: Int?
    public let nLost: Int?
    public let hitRate: Double?
    public let avgPredictedProb: Double?
    public let calibrationBuckets: [CalibrationBucket]?

    // Closing-line value.
    public let nClvTracked: Int?
    public let nClvMoved: Int?
    public let nClvStale: Int?
    public let clvPlusRate: Double?
    public let avgClvPct: Double?
    public let avgClvPctMoved: Double?
    public let nLoggedLegs: Int?
    public let clvCoveragePct: Double?

    // P&L.
    public let pnlTimeline: [PnlPoint]?
    public let resolvedSlips: Int?
    public let wonSlips: Int?
    public let roiPerSlip: Double?

    // Per-leg arrays (with slip-inherited timestamps) for client-side recompute.
    public let resolvedLegs: [ResolvedLeg]?
    public let clvLegs: [ClvLeg]?

    /// Calibration buckets that actually have data (non-empty), for the curve.
    public var populatedBuckets: [CalibrationBucket] {
        (calibrationBuckets ?? []).filter { ($0.count ?? 0) > 0 && $0.predictedAvg != nil && $0.actualAvg != nil }
    }

    public var hasResolvedData: Bool { (nResolved ?? 0) > 0 }
}

public struct CalibrationBucket: Codable, Sendable, Identifiable {
    public let bucket: String?
    public let predictedAvg: Double?
    public let actualAvg: Double?
    public let count: Int?
    public var id: String { bucket ?? UUID().uuidString }
}

public struct PnlPoint: Codable, Sendable, Identifiable {
    public let slipId: String?
    public let timestamp: String?
    public let pnl: Double?
    public let cumPnl: Double?
    public var id: String { slipId ?? (timestamp ?? UUID().uuidString) }
    public var date: Date? { ISO8601Date.parse(timestamp) }
}

public struct ResolvedLeg: Codable, Sendable {
    public let trueProb: Double?
    public let outcome: Int?
    public let timestamp: String?
}

public struct ClvLeg: Codable, Sendable {
    public let closingProb: Double?
    public let clvPct: Double?
    public let timestamp: String?
}

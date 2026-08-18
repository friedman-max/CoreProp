import Foundation
import Combine
import CorePropKit

/// The shared slip selection, bridging the Bets tab (where legs are added) and
/// the Slip tab (where EV is computed and the slip is logged). PrizePicks slips
/// are 2–6 legs, so selection is capped at 6.
@MainActor
final class SlipStore: ObservableObject {
    static let maxLegs = 6

    @Published private(set) var bets: [Bet] = []

    var count: Int { bets.count }
    var betIds: [String] { bets.map(\.betId) }
    var isFull: Bool { bets.count >= Self.maxLegs }
    var trueProbs: [Double] { bets.map(\.trueProb) }

    func contains(_ bet: Bet) -> Bool { bets.contains { $0.betId == bet.betId } }

    /// Add if room, remove if already present. Returns false if the add was
    /// blocked because the slip is full.
    @discardableResult
    func toggle(_ bet: Bet) -> Bool {
        if let idx = bets.firstIndex(where: { $0.betId == bet.betId }) {
            bets.remove(at: idx)
            return true
        }
        guard bets.count < Self.maxLegs else { return false }
        bets.append(bet)
        return true
    }

    func remove(_ bet: Bet) {
        bets.removeAll { $0.betId == bet.betId }
    }

    func remove(id: String) {
        bets.removeAll { $0.betId == id }
    }

    func clear() { bets.removeAll() }
}

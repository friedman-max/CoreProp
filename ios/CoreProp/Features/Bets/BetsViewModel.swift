import Foundation
import Combine
import CorePropKit

@MainActor
final class BetsViewModel: ObservableObject {
    @Published private(set) var bets: [Bet] = []
    @Published private(set) var loggedKeys: Set<String> = []
    @Published private(set) var state: LoadState = .idle
    @Published private(set) var lastRefresh: Date?

    // Filters
    @Published var selectedLeagues: Set<String> = []   // empty ⇒ all
    @Published var selectedProps: Set<String> = []     // empty ⇒ all
    @Published var minTruePct: Double = 50
    @Published var sortDescending = true
    @Published var includeGreenDevils = true
    @Published var hideLogged = false
    @Published var searchText = ""

    var availableLeagues: [String] { Array(Set(bets.map(\.league))).sorted() }

    /// Prop types present in the CURRENT league selection, so the prop chips
    /// track the league filter instead of listing every league's props.
    var availableProps: [String] {
        let scoped = selectedLeagues.isEmpty ? bets : bets.filter { selectedLeagues.contains($0.league) }
        return Array(Set(scoped.map(\.propType))).sorted()
    }

    func load(client: CoreClient, model: AppModel) async {
        if bets.isEmpty { state = .loading }
        do {
            let core = try await client.bootstrapCore()
            bets = core.bets
            lastRefresh = core.lastRefreshDate
            state = core.bets.isEmpty ? .empty : .loaded
            // Best-effort: join the user's logged bets so rows can be tinted.
            if let keys = try? await client.backtestKeys() {
                loggedKeys = Set(keys)
            }
        } catch let e as APIError {
            if e.isPaymentRequired {
                await model.handlePaymentRequired()
                state = .failed("A CoreProp subscription is required to view +EV bets.")
            } else {
                state = .failed(e.display)
            }
        } catch {
            state = .failed(error.localizedDescription)
        }
    }

    func isLogged(_ bet: Bet) -> Bool {
        bet.betKey.map(loggedKeys.contains) ?? false
    }

    var filtered: [Bet] {
        let q = searchText.trimmingCharacters(in: .whitespaces).lowercased()
        return bets.filter { b in
            (selectedLeagues.isEmpty || selectedLeagues.contains(b.league)) &&
            (selectedProps.isEmpty || selectedProps.contains(b.propType)) &&
            b.truePct >= minTruePct - 0.0001 &&
            (includeGreenDevils || !b.isGreenDevil) &&
            (!hideLogged || !isLogged(b)) &&
            (q.isEmpty || b.playerName.lowercased().contains(q) || b.propType.lowercased().contains(q))
        }
        .sorted { sortDescending ? $0.trueProb > $1.trueProb : $0.trueProb < $1.trueProb }
    }

    /// Drop any selected props that aren't in the current league scope (called
    /// when the league filter changes) so a stale prop can't hide every row.
    func pruneProps() {
        let valid = Set(availableProps)
        selectedProps.formIntersection(valid)
    }

    func resetFilters() {
        selectedLeagues.removeAll()
        selectedProps.removeAll()
        minTruePct = 50
        includeGreenDevils = true
        hideLogged = false
        searchText = ""
    }
}

import Foundation
import Combine
import CorePropKit

@MainActor
final class LinesViewModel: ObservableObject {
    enum SortField: String, CaseIterable {
        case trueProb = "True %"
        case best = "Best odds"
        case player = "Player"
    }

    @Published private(set) var lines: [MarketLine] = []
    @Published private(set) var state: LoadState = .idle
    @Published var searchText = ""
    @Published var selectedLeagues: Set<String> = []
    @Published var sortField: SortField = .trueProb

    private(set) var loadedSource: BoardSource?

    var availableLeagues: [String] {
        Array(Set(lines.compactMap { $0.league })).sorted()
    }

    func load(source: BoardSource, client: CoreClient, model: AppModel) async {
        if lines.isEmpty || loadedSource != source { state = .loading }
        loadedSource = source
        do {
            let result = try await client.lines(source)
            lines = result
            state = result.isEmpty ? .empty : .loaded
        } catch let e as APIError {
            if e.isPaymentRequired {
                await model.handlePaymentRequired()
                state = .failed("A CoreProp subscription is required to view lines.")
            } else {
                state = .failed(e.display)
            }
        } catch {
            state = .failed(error.localizedDescription)
        }
    }

    var filtered: [MarketLine] {
        let q = searchText.trimmingCharacters(in: .whitespaces).lowercased()
        let matched = lines.filter { l in
            (selectedLeagues.isEmpty || (l.league.map { selectedLeagues.contains($0) } ?? false)) &&
            (q.isEmpty || (l.playerName ?? "").lowercased().contains(q) || l.prop.lowercased().contains(q))
        }
        switch sortField {
        case .trueProb:
            return matched.sorted { ($0.truePct ?? -1) > ($1.truePct ?? -1) }
        case .best:
            return matched.sorted { ($0.best ?? Int.min) > ($1.best ?? Int.min) }
        case .player:
            return matched.sorted { ($0.playerName ?? "") < ($1.playerName ?? "") }
        }
    }
}

import Foundation
import Combine
import CorePropKit

@MainActor
final class BacktestViewModel: ObservableObject {
    @Published private(set) var slips: [BacktestSlip] = []
    @Published private(set) var state: LoadState = .idle
    @Published var deleting: Set<String> = []

    var summary: BacktestSummary { BacktestSummary.compute(slips) }

    func load(client: CoreClient, model: AppModel) async {
        if slips.isEmpty { state = .loading }
        do {
            let result = try await client.backtestSlips()
            slips = result
            state = result.isEmpty ? .empty : .loaded
        } catch let e as APIError {
            if e.isPaymentRequired {
                await model.handlePaymentRequired()
                state = .failed("A CoreProp subscription is required to view your backtest.")
            } else {
                state = .failed(e.display)
            }
        } catch {
            state = .failed(error.localizedDescription)
        }
    }

    func delete(_ slip: BacktestSlip, client: CoreClient, model: AppModel) async {
        deleting.insert(slip.id)
        defer { deleting.remove(slip.id) }
        do {
            try await client.deleteSlip(id: slip.id)
            slips.removeAll { $0.id == slip.id }
            if slips.isEmpty { state = .empty }
        } catch let e as APIError {
            if e.isPaymentRequired { await model.handlePaymentRequired() }
        } catch { /* leave the slip in place on failure */ }
    }
}

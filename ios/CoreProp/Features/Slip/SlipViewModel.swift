import Foundation
import Combine
import CorePropKit

@MainActor
final class SlipViewModel: ObservableObject {
    @Published var slipType: SlipType = .power
    @Published private(set) var serverResult: SlipResult?
    @Published var optimizing = false
    @Published var logging = false
    @Published var banner: Banner?

    struct Banner: Identifiable, Equatable {
        let id = UUID()
        let text: String
        let isError: Bool
    }

    /// Ask the server for a correlation-aware best subset from the current legs.
    func optimize(betIds: [String], client: CoreClient, model: AppModel) async {
        guard betIds.count >= 2 else { return }
        optimizing = true
        defer { optimizing = false }
        do {
            serverResult = try await client.autoBuildSlip(betIds: betIds)
        } catch let e as APIError {
            if e.isPaymentRequired { await model.handlePaymentRequired() }
            banner = Banner(text: e.display, isError: true)
        } catch {
            banner = Banner(text: error.localizedDescription, isError: true)
        }
    }

    /// Log the current legs to the backtest as the chosen slip type.
    func log(betIds: [String], client: CoreClient, model: AppModel) async -> Bool {
        guard (2...6).contains(betIds.count) else {
            banner = Banner(text: "A slip needs 2–6 legs to log.", isError: true)
            return false
        }
        logging = true
        defer { logging = false }
        do {
            try await client.addSlip(betIds: betIds, slipType: slipType.apiValue)
            banner = Banner(text: "Logged \(betIds.count)-leg \(slipType.apiValue) slip to your backtest.", isError: false)
            return true
        } catch let e as APIError {
            if e.isPaymentRequired { await model.handlePaymentRequired() }
            banner = Banner(text: e.display, isError: true)
            return false
        } catch {
            banner = Banner(text: error.localizedDescription, isError: true)
            return false
        }
    }

    func clearServerResult() { serverResult = nil }
}

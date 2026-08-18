import Foundation
import Combine

/// Cross-cutting navigation state so a notification tap (handled in the app
/// delegate, outside the SwiftUI tree) can switch tabs. A shared singleton
/// because the delegate has no `@EnvironmentObject` access.
@MainActor
final class AppRouter: ObservableObject {
    static let shared = AppRouter()

    enum Tab: Hashable { case bets, lines, slip, backtest, account }

    @Published var selectedTab: Tab = .bets

    private init() {}

    /// Route to the Backtest tab (a slip-alert tap means "show my slips").
    func openBacktest() { selectedTab = .backtest }
}

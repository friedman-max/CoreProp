import SwiftUI

/// The five-tab signed-in shell. The three web board tabs (Combined /
/// PrizePicks / Sportsbooks) are consolidated under **Lines** as a segmented
/// control; **Analytics** is reachable from Account.
struct MainTabView: View {
    @EnvironmentObject private var slip: SlipStore
    @StateObject private var router = AppRouter.shared

    var body: some View {
        TabView(selection: $router.selectedTab) {
            BetsView()
                .tabItem { Label("Bets", systemImage: "chart.line.uptrend.xyaxis") }
                .tag(AppRouter.Tab.bets)

            LinesView()
                .tabItem { Label("Lines", systemImage: "list.bullet.rectangle") }
                .tag(AppRouter.Tab.lines)

            SlipView()
                .tabItem { Label("Slip", systemImage: "rectangle.stack") }
                .badge(slip.count == 0 ? 0 : slip.count)
                .tag(AppRouter.Tab.slip)

            BacktestView()
                .tabItem { Label("Backtest", systemImage: "clock.arrow.circlepath") }
                .tag(AppRouter.Tab.backtest)

            AccountView()
                .tabItem { Label("Account", systemImage: "person.crop.circle") }
                .tag(AppRouter.Tab.account)
        }
    }
}

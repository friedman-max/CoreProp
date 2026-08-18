import SwiftUI

/// The five-tab signed-in shell. The three web board tabs (Combined /
/// PrizePicks / Sportsbooks) are consolidated under **Lines** as a segmented
/// control; **Analytics** is reachable from Account.
struct MainTabView: View {
    @EnvironmentObject private var slip: SlipStore

    var body: some View {
        TabView {
            BetsView()
                .tabItem { Label("Bets", systemImage: "chart.line.uptrend.xyaxis") }

            LinesView()
                .tabItem { Label("Lines", systemImage: "list.bullet.rectangle") }

            SlipView()
                .tabItem { Label("Slip", systemImage: "rectangle.stack") }
                .badge(slip.count == 0 ? 0 : slip.count)

            BacktestView()
                .tabItem { Label("Backtest", systemImage: "clock.arrow.circlepath") }

            AccountView()
                .tabItem { Label("Account", systemImage: "person.crop.circle") }
        }
    }
}

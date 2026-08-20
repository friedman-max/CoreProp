import SwiftUI
import CorePropKit

/// Full detail for one bet: the decision probability, fair odds, edge, every
/// book's price, and the per-leg EV this leg carries in each slip size/type
/// (`SlipEV.scoreLeg`). Adds/removes the bet from the slip.
struct BetDetailView: View {
    let bet: Bet
    @EnvironmentObject private var slip: SlipStore
    @State private var slipFullAlert = false

    var body: some View {
        ScrollView {
            VStack(spacing: 16) {
                header
                keyNumbers
                if !bet.bookOdds.isEmpty { bookOdds }
                evBySlip
                addButton
            }
            .padding(16)
        }
        .background(Theme.bg.ignoresSafeArea())
        .navigationTitle(bet.playerName)
        .navigationBarTitleDisplayMode(.inline)
        .alert("Slip is full", isPresented: $slipFullAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("A PrizePicks slip holds up to \(SlipStore.maxLegs) legs.")
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                LeaguePill(league: bet.league)
                if let team = bet.team, !team.isEmpty {
                    Text(team).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
                }
                if bet.isGreenDevil {
                    Text("GOBLIN").font(Theme.ui(9, .bold)).foregroundColor(Theme.green)
                        .padding(.horizontal, 5).padding(.vertical, 2)
                        .background(Theme.greenHi).clipShape(Capsule())
                }
                Spacer()
            }
            Text(bet.playerName).font(Theme.ui(22, .bold)).foregroundColor(Theme.text)
            HStack(spacing: 8) {
                SideBadge(side: bet.sideLabel)
                Text(bet.propType).font(Theme.ui(15)).foregroundColor(Theme.text2)
                Text(Fmt.line(bet.ppLine)).font(Theme.mono(15, .bold)).foregroundColor(Theme.text)
            }
            if let start = bet.startDate {
                Label(Fmt.gameTime(start), systemImage: "clock")
                    .font(Theme.mono(12)).foregroundColor(Theme.text3)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private var keyNumbers: some View {
        HStack(spacing: 12) {
            numberTile("True prob", Fmt.percentValue(bet.truePct), Theme.primary2)
            numberTile("Fair odds", Fmt.americanOdds(bet.trueOdds), Theme.text)
            numberTile("Edge", Fmt.signedPercent(bet.edge),
                       (bet.edge ?? 0) >= 0 ? Theme.green : Theme.red2)
        }
    }

    private func numberTile(_ label: String, _ value: String, _ color: Color) -> some View {
        VStack(spacing: 6) {
            Text(label.uppercased()).font(Theme.ui(10, .semibold)).kerning(0.5).foregroundColor(Theme.text3)
            Text(value).font(Theme.mono(20, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
        .cpCard(radius: 12, padding: 14)
    }

    private var bookOdds: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("BOOK PRICES").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
            ForEach(bet.bookOdds, id: \.book) { bo in
                HStack {
                    BookBadgeView(book: bo.book)
                    Text(bo.book.fullName).font(Theme.ui(13)).foregroundColor(Theme.text2)
                    Spacer()
                    Text(Fmt.americanOdds(bo.odds)).font(Theme.mono(14, .semibold)).foregroundColor(Theme.text)
                }
            }
            if let raw = bet.rawTrueProb {
                Divider().overlay(Theme.hair)
                HStack {
                    Text("Raw consensus").font(Theme.ui(12)).foregroundColor(Theme.text3)
                    Spacer()
                    Text(Fmt.percent(raw)).font(Theme.mono(12, .medium)).foregroundColor(Theme.text3)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private var evBySlip: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("PER-LEG EV BY SLIP").font(Theme.ui(10.5, .semibold)).kerning(0.6).foregroundColor(Theme.text3)
            Text("This leg's EV as a share of stake in each slip size (independence model).")
                .font(Theme.ui(11)).foregroundColor(Theme.text3)
            HStack {
                Text("SIZE").frame(width: 44, alignment: .leading)
                Text("POWER").frame(maxWidth: .infinity, alignment: .trailing)
                Text("FLEX").frame(maxWidth: .infinity, alignment: .trailing)
            }
            .font(Theme.ui(10, .semibold)).foregroundColor(Theme.text4)

            ForEach(2...6, id: \.self) { n in
                HStack {
                    Text("\(n)").font(Theme.mono(13, .semibold)).foregroundColor(Theme.text2)
                        .frame(width: 44, alignment: .leading)
                    evCell(SlipEV.scoreLeg(bet.trueProb, n: n, type: .power))
                    if n >= 3 {
                        evCell(SlipEV.scoreLeg(bet.trueProb, n: n, type: .flex))
                    } else {
                        Text("—").font(Theme.mono(13)).foregroundColor(Theme.text4)
                            .frame(maxWidth: .infinity, alignment: .trailing)
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard()
    }

    private func evCell(_ ev: Double) -> some View {
        Text(Fmt.signedPercent(ev))
            .font(Theme.mono(13, .semibold))
            .foregroundColor(ev >= 0 ? Theme.green : Theme.red2)
            .frame(maxWidth: .infinity, alignment: .trailing)
    }

    private var addButton: some View {
        Button {
            if slip.contains(bet) { slip.remove(bet) }
            else if !slip.toggle(bet) { slipFullAlert = true }
        } label: {
            Label(slip.contains(bet) ? "Remove from slip" : "Add to slip",
                  systemImage: slip.contains(bet) ? "minus.circle" : "plus.circle")
        }
        .buttonStyle(PrimaryButtonStyle())
    }
}

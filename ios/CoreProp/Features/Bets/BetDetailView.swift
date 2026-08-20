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
            VStack(spacing: Theme.s4) {
                header
                keyNumbers
                if !bet.bookOdds.isEmpty { bookOdds }
                evBySlip
                addButton
            }
            .padding(Theme.s4)
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
        VStack(alignment: .leading, spacing: Theme.s3) {
            HStack(spacing: Theme.s2) {
                LeaguePill(league: bet.league)
                if let team = bet.team, !team.isEmpty {
                    Text(team).font(Theme.ui(12, .semibold)).foregroundColor(Theme.text3)
                }
                if bet.isGreenDevil {
                    // Intra-badge padding, as in BetRow — these size the pill
                    // itself, so they stay off the spacing scale.
                    Text("GOBLIN").font(Theme.ui(9, .bold)).foregroundColor(Theme.green)
                        .padding(.horizontal, 5).padding(.vertical, 2)
                        .background(Theme.greenHi).clipShape(Capsule())
                }
                Spacer()
            }
            Text(bet.playerName).font(Theme.ui(22, .bold)).foregroundColor(Theme.text)
            HStack(spacing: Theme.s2) {
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
        HStack(spacing: Theme.s3) {
            numberTile("True prob", Fmt.percentValue(bet.truePct), Theme.primary2)
            numberTile("Fair odds", Fmt.americanOdds(bet.trueOdds), Theme.text)
            numberTile("Edge", Fmt.signedPercent(bet.edge),
                       (bet.edge ?? 0) >= 0 ? Theme.green : Theme.red2)
        }
    }

    private func numberTile(_ label: String, _ value: String, _ color: Color) -> some View {
        VStack(spacing: Theme.s2) {
            // De-capsed, and the letter-spacing goes with the caps: "True prob"
            // / "Fair odds" / "Edge" are micro-labels for the 20pt number beside
            // them, not section headers, and an all-caps tracked label competes
            // with the number it labels. Web de-capsed its stat-tile labels for
            // the same reason (see StatTile in Components.swift). Sentence case
            // wants no .04em tracking, so the `.kerning(0.5)` is simply gone
            // rather than converted.
            Text(label).font(Theme.ui(10, .semibold)).foregroundColor(Theme.text3)
            Text(value).font(Theme.mono(20, .bold)).foregroundColor(color)
        }
        .frame(maxWidth: .infinity)
        // `Theme.rMd` rather than a bare 12 — same value, now on the scale.
        // padding stays 14: that is cpCard's own default and what StatTile uses,
        // so moving this one tile to s4 would desync the two tile shapes.
        .cpCard(radius: Theme.rMd, padding: 14)
    }

    private var bookOdds: some View {
        VStack(alignment: .leading, spacing: Theme.s3) {
            // Stays UPPERCASE: this is a real section header, unlike numberTile's
            // labels. `.tracking`, not `.kerning` — tracking is letter-spacing,
            // kerning adjusts glyph pairs; .04em at 10.5pt is 0.42.
            Text("BOOK PRICES").font(Theme.ui(10.5, .semibold)).tracking(0.42).foregroundColor(Theme.text3)
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
        VStack(alignment: .leading, spacing: Theme.s3) {
            // Also a real section header — uppercase stays; kerning -> tracking.
            Text("PER-LEG EV BY SLIP").font(Theme.ui(10.5, .semibold)).tracking(0.42).foregroundColor(Theme.text3)
            Text("This leg's EV as a share of stake in each slip size (independence model).")
                .font(Theme.ui(11)).foregroundColor(Theme.text3)
            // `text3`, not `text4`: this is a column header a user reads, and
            // text4 is 2.3:1 — decorative/disabled glyphs only. 44 is the SIZE
            // column's width, so it stays literal.
            HStack {
                Text("SIZE").frame(width: 44, alignment: .leading)
                Text("POWER").frame(maxWidth: .infinity, alignment: .trailing)
                Text("FLEX").frame(maxWidth: .infinity, alignment: .trailing)
            }
            .font(Theme.ui(10, .semibold)).foregroundColor(Theme.text3)

            ForEach(2...6, id: \.self) { n in
                HStack {
                    Text("\(n)").font(Theme.mono(13, .semibold)).foregroundColor(Theme.text2)
                        .frame(width: 44, alignment: .leading)
                    evCell(SlipEV.scoreLeg(bet.trueProb, n: n, type: .power))
                    if n >= 3 {
                        evCell(SlipEV.scoreLeg(bet.trueProb, n: n, type: .flex))
                    } else {
                        // The em dash is the one legitimate `text4` here: it is a
                        // decorative "no such cell" glyph (there is no 2-leg
                        // Flex), not text.
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

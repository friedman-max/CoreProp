import SwiftUI
import CorePropKit

/// One +EV bet row. Logged rows (already in the user's backtest) get a red left
/// accent + wash, matching the web board. The trailing control adds/removes the
/// bet from the slip.
struct BetRow: View {
    let bet: Bet
    let logged: Bool
    let selected: Bool
    let onToggleSlip: () -> Void

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Rectangle()
                .fill(logged ? Theme.red : (selected ? Theme.primary : Color.clear))
                .frame(width: 3)
                .clipShape(Capsule())

            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 8) {
                    Text(bet.playerName)
                        .font(Theme.ui(15, .semibold))
                        .foregroundColor(Theme.text)
                        .lineLimit(1)
                    if bet.isGreenDevil {
                        Text("GOBLIN")
                            .font(Theme.ui(9, .bold))
                            .foregroundColor(Theme.green)
                            .padding(.horizontal, 5).padding(.vertical, 2)
                            .background(Theme.greenHi)
                            .clipShape(Capsule())
                    }
                }
                HStack(spacing: 8) {
                    LeaguePill(league: bet.league)
                    Text(bet.propType)
                        .font(Theme.ui(13))
                        .foregroundColor(Theme.text2)
                        .lineLimit(1)
                }
                HStack(spacing: 6) {
                    SideBadge(side: bet.sideLabel)
                    Text(Fmt.line(bet.ppLine))
                        .font(Theme.mono(13, .semibold))
                        .foregroundColor(Theme.text)
                    if let start = bet.startDate {
                        Text("· \(Fmt.gameTime(start))")
                            .font(Theme.mono(11))
                            .foregroundColor(Theme.text3)
                            .lineLimit(1)
                    }
                }
                if !bet.bookOdds.isEmpty {
                    HStack(spacing: 6) {
                        ForEach(bet.bookOdds, id: \.book) { bo in
                            HStack(spacing: 3) {
                                BookBadgeView(book: bo.book)
                                Text(Fmt.americanOdds(bo.odds))
                                    .font(Theme.mono(11, .medium))
                                    .foregroundColor(Theme.text3)
                            }
                        }
                    }
                }
            }

            Spacer(minLength: 4)

            VStack(alignment: .trailing, spacing: 8) {
                VStack(alignment: .trailing, spacing: 0) {
                    Text(Fmt.percentValue(bet.truePct))
                        .font(Theme.mono(16, .bold))
                        .foregroundColor(Theme.primary2)
                    Text("true")
                        .font(Theme.ui(9, .semibold))
                        .foregroundColor(Theme.text3)
                }
                Button(action: onToggleSlip) {
                    Image(systemName: selected ? "checkmark" : "plus")
                        .font(.system(size: 13, weight: .bold))
                        .foregroundColor(selected ? .white : Theme.primary2)
                        .frame(width: 30, height: 30)
                        .background(selected ? Theme.primary : Theme.controlBg)
                        .clipShape(Circle())
                        .overlay(Circle().stroke(selected ? Color.clear : Theme.hair2, lineWidth: 1))
                }
                .buttonStyle(.plain)
                .accessibilityLabel(selected ? "Remove from slip" : "Add to slip")
            }
        }
        .padding(.vertical, 10)
        .padding(.horizontal, 12)
        .background(logged ? Theme.redHi : (selected ? Theme.primaryHi : Color.clear))
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }
}

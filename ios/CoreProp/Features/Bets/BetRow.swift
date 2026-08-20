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
        HStack(alignment: .top, spacing: Theme.s3) {
            Rectangle()
                .fill(logged ? Theme.red : (selected ? Theme.primary : Color.clear))
                // 3 is the bar's own width, not a gap — web's row accent is 3px
                // too. Intrinsic element sizes stay off the spacing scale.
                .frame(width: 3)
                .clipShape(Capsule())

            VStack(alignment: .leading, spacing: Theme.s2) {
                HStack(spacing: Theme.s2) {
                    Text(bet.playerName)
                        .font(Theme.ui(15, .semibold))
                        .foregroundColor(Theme.text)
                        .lineLimit(1)
                    if bet.isGreenDevil {
                        Text("GOBLIN")
                            .font(Theme.ui(9, .bold))
                            .foregroundColor(Theme.green)
                            // The 2pt vertical stays literal — it sizes the badge
                            // itself on a 9pt label, below the scale's 4pt floor,
                            // and rounding it up to s1 would double it. The
                            // horizontal 5 is not that: 5 is on the scale's own
                            // territory, so it takes the nearest step, s1 (4), 1pt
                            // tighter. GOBLIN stays the tightest badge in the app
                            // (4 against LeaguePill's and BookBadgeView's 8), which
                            // is the hierarchy it already had at 5 against 8.
                            // BetDetailView carries the identical pair — the two
                            // copies must stay in step until the badge is extracted
                            // into one component.
                            .padding(.horizontal, Theme.s1).padding(.vertical, 2)
                            .background(Theme.greenHi)
                            .clipShape(Capsule())
                    }
                }
                HStack(spacing: Theme.s2) {
                    LeaguePill(league: bet.league)
                    Text(bet.propType)
                        .font(Theme.ui(13))
                        .foregroundColor(Theme.text2)
                        .lineLimit(1)
                }
                HStack(spacing: Theme.s2) {
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
                    HStack(spacing: Theme.s2) {
                        ForEach(bet.bookOdds, id: \.book) { bo in
                            HStack(spacing: Theme.s1) {
                                BookBadgeView(book: bo.book)
                                Text(Fmt.americanOdds(bo.odds))
                                    .font(Theme.mono(11, .medium))
                                    .foregroundColor(Theme.text3)
                            }
                        }
                    }
                }
            }

            Spacer(minLength: Theme.s1)

            VStack(alignment: .trailing, spacing: Theme.s2) {
                // spacing 0 is deliberate, not an unmigrated literal: the caption
                // is the number's baseline label and rides directly under it.
                VStack(alignment: .trailing, spacing: 0) {
                    // 20pt, not 16: true% is the row's hero number and web sizes
                    // it as one. The COLOUR stays `primary2` — web encodes true%
                    // as an OKLCH green ramp, SwiftUI has no OKLCH space
                    // (`Color.mix(with:)` is iOS 18), and flattening the ramp to
                    // one blue would be a colour change, not a port. Size only.
                    Text(Fmt.percentValue(bet.truePct))
                        .font(Theme.mono(20, .bold))
                        .foregroundColor(Theme.primary2)
                    Text("true")
                        .font(Theme.ui(9, .semibold))
                        .foregroundColor(Theme.text3)
                }
                // Web's add control is a --r-md rounded square; this stays a
                // Circle() because a circular 30pt tap target is the iOS idiom
                // (and 30 is the control's own size, not a spacing step).
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
        // s4/s5, the airy-row geometry: web's --row-px is clamp(20px,2vw,24px)
        // and 20 is the fixed-width equivalent, so the horizontal step is one
        // above the vertical.
        .padding(.vertical, Theme.s4)
        .padding(.horizontal, Theme.s5)
        // `primaryLo` (.10), not `primaryHi` (.22): this tint sits behind the
        // row's *inherited* muted text, and .22 puts that text at 4.45:1, under
        // AA. `primaryHi` is for rings and badges that carry an explicit light
        // colour on top. The logged `redHi` is already .10 — lighter than web's
        // .16 — so it needs no change.
        .background(logged ? Theme.redHi : (selected ? Theme.primaryLo : Color.clear))
        .clipShape(RoundedRectangle(cornerRadius: Theme.radiusSm, style: .continuous))
    }
}

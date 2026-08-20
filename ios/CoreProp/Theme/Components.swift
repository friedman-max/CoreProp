import SwiftUI
import CorePropKit

// MARK: - Card surface

/// The neutral card surface: **flat** `Theme.card`, a 1px `--hair` border,
/// `--r-lg` corners, and the single card shadow.
///
/// This used to paint a two-stop vertical grey gradient, on the reading that
/// gradients were only banned on *accent* surfaces. Cards are flat now — every
/// web card is flat `--card` — so do not reintroduce it. The stroke also moved
/// from `hair2` (.10) to `hair` (.06) to match web, where every card border is
/// `--hair`; iOS's heavier hairline was the main reason its cards read as
/// boxier than web's.
///
/// `radius` resolves through the `Theme.radius` alias, so it became 16 (from
/// 14) when the radius scale landed — that moves all 13 bare `.cpCard()` call
/// sites at once, which is intended.
///
/// `padding` does the same thing on the spacing scale: 14 -> `Theme.s4` (16).
/// This is the single highest-leverage value in the app — it is the interior
/// inset of every card — and leaving the design system's own default off the
/// design system's own scale was the inconsistency this phase exists to remove.
/// Web reads the same way: `.an-panel` is `var(--s-5)` and both `.bt-card` and
/// `.bt-slip` are `14px 16px`, so 16 is the conservative choice rather than a
/// bold one. The default is declared TWICE (here and in the `cpCard`
/// convenience below) and the two must always agree; changing one silently
/// splits the default in two, which is why the invariant test reports both.
struct CardModifier: ViewModifier {
    var radius: CGFloat = Theme.radius
    var padding: CGFloat = Theme.s4
    func body(content: Content) -> some View {
        content
            .padding(padding)
            .background(Theme.card)
            .clipShape(RoundedRectangle(cornerRadius: radius, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: radius, style: .continuous)
                    .stroke(Theme.hair, lineWidth: 1)
            )
            .shadow(color: Theme.shadowColor, radius: Theme.shadowRadius, x: 0, y: Theme.shadowY)
    }
}

extension View {
    func cpCard(radius: CGFloat = Theme.radius, padding: CGFloat = Theme.s4) -> some View {
        modifier(CardModifier(radius: radius, padding: padding))
    }
}

// MARK: - Buttons

/// Primary CTA: flat `--primary` fill, white label, pill shape, darkens when
/// pressed (never lightens).
///
/// 12/18 -> `s3`/`s5`: the vertical is unchanged (s3 *is* 12) and the
/// horizontal gains 2pt. `GhostButtonStyle` below carries the identical pair
/// and must keep carrying it — the two styles sit side by side in every
/// confirm/cancel row, so a 2pt divergence between them reads as a
/// misalignment rather than as two button variants.
struct PrimaryButtonStyle: ButtonStyle {
    var fullWidth: Bool = true
    var enabled: Bool = true
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(Theme.ui(15, .semibold))
            .foregroundColor(.white)
            .frame(maxWidth: fullWidth ? .infinity : nil)
            .padding(.vertical, Theme.s3)
            .padding(.horizontal, Theme.s5)
            .background(
                (configuration.isPressed ? Theme.primaryHover : Theme.primary)
                    .opacity(enabled ? 1 : 0.4)
            )
            .clipShape(Capsule())
            .contentShape(Capsule())
    }
}

/// Secondary/ghost: transparent with a hairline border.
///
/// The `s3`/`s5` pair is deliberately identical to `PrimaryButtonStyle`'s — see
/// the note there.
struct GhostButtonStyle: ButtonStyle {
    var fullWidth: Bool = false
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(Theme.ui(15, .semibold))
            .foregroundColor(Theme.text)
            .frame(maxWidth: fullWidth ? .infinity : nil)
            .padding(.vertical, Theme.s3)
            .padding(.horizontal, Theme.s5)
            .background(configuration.isPressed ? Theme.hair : Color.clear)
            .clipShape(Capsule())
            .overlay(Capsule().stroke(Theme.hair2, lineWidth: 1))
            .contentShape(Capsule())
    }
}

// MARK: - Filter chip

struct FilterChip: View {
    let title: String
    let selected: Bool
    var accent: Color = Theme.primary
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Text(title)
                .font(Theme.ui(13, .semibold))
                .foregroundColor(selected ? .white : Theme.text2)
                // 13 -> s3 (12), 1pt tighter. Web's `.ev-chip` is `padding:0 13px`,
                // so this is the one place iOS deliberately lands 1pt off its web
                // twin: 13 is not on either side's scale, and s3 is the nearest
                // step. The 34pt height below is what actually holds the chip's
                // shape, so a 1pt horizontal change is not load-bearing.
                .padding(.horizontal, Theme.s3)
                // An explicit 34pt height replaces the old 7pt vertical padding
                // (which produced ~28pt). 34 is the filter-control contract: the
                // filter-menu button beside these chips in BetsView is 34x34, and
                // a 28pt chip next to a 34pt button has no shared baseline. Web
                // fixes its chip height to 34px for exactly this reason.
                .frame(height: 34)
                .background(selected ? accent : Theme.controlBg)
                .clipShape(Capsule())
                .overlay(Capsule().stroke(selected ? Color.clear : Theme.hair, lineWidth: 1))
        }
        .buttonStyle(.plain)
    }
}

// MARK: - League pill / book badge / side badge

struct LeaguePill: View {
    let league: String
    var body: some View {
        let color = Theme.leagueColor(league)
        Text(league.uppercased())
            .font(Theme.ui(11, .semibold))
            // tracking, not kerning: tracking is letter-spacing, kerning adjusts
            // specific glyph pairs. .04em at 11pt is 0.44.
            .tracking(0.44)
            .foregroundColor(color)
            // The 3pt vertical stays literal (a capsule's optical inset on an
            // 11pt label, below the 4pt floor of the scale); the horizontal 8 was
            // already `s2`'s value and is now spelled as the token.
            .padding(.vertical, 3)
            .padding(.horizontal, Theme.s2)
            .background(color.opacity(0.14))
            .clipShape(Capsule())
    }
}

struct BookBadgeView: View {
    let book: Book
    var body: some View {
        let c = Theme.bookColors(book)
        Text(book.label)
            .font(Theme.ui(10, .bold))
            // Web tracks this badge at .06em; at 10pt that is 0.6pt. iOS had no
            // tracking here at all, which made the 10pt all-caps label read
            // tighter and muddier than its web twin.
            .tracking(0.6)
            .foregroundColor(c.fg)
            // 6 -> s2 (8), 2pt wider each side. 6 is an exact tie between s1 and
            // s2 and ties round up, which also lands it on web's tokenized badge
            // inset (`.bt-slip-badge` is `padding:3px 8px`). Web's own book badge
            // `.cp-book` is `padding:2px 5px`, so iOS reads 3pt wider than that
            // twin — the tie could as defensibly have gone to s1, and this is the
            // move most worth a rendered check, because these badges wrap in rows
            // of three or four and each one now takes 4pt more width.
            // The 2pt vertical stays literal: it is the badge's optical inset on a
            // 10pt label, below the scale's 4pt floor.
            .padding(.vertical, 2)
            .padding(.horizontal, Theme.s2)
            .background(c.bg)
            .clipShape(RoundedRectangle(cornerRadius: Theme.radiusXs, style: .continuous))
    }
}

struct SideBadge: View {
    let side: String   // "OVER" / "UNDER"
    var body: some View {
        let over = side.uppercased() == "OVER"
        Text(side.uppercased())
            .font(Theme.ui(12, .bold))
            // Web tracks this at .05em; at 12pt that is 0.6pt.
            .tracking(0.6)
            .foregroundColor(over ? Theme.sideOver : Theme.sideUnder)
    }
}

// MARK: - Stat tile

struct StatTile: View {
    enum Tone { case neutral, good, bad }
    let label: String
    let value: String
    var tone: Tone = .neutral
    var loading: Bool = false

    private var valueColor: Color {
        switch tone {
        case .neutral: return Theme.text
        case .good:    return Theme.green
        case .bad:     return Theme.red2
        }
    }

    var body: some View {
        // 6 -> s2 (8): a tie between s1 and s2, rounded up. Web's counterpart
        // gap (`.bt-card-label`) is 6px, so the label/value pair opens up by 2pt.
        VStack(alignment: .leading, spacing: Theme.s2) {
            // Not `.uppercased()`: web's .bt-card-label was de-capsed in the
            // typography pass, because an all-caps tile label competes with the
            // 22pt number it is labelling. And tracking, not kerning — tracking
            // is the true letter-spacing analogue (kerning adjusts pairs). Web's
            // micro-label tracking is .04em, which at 10.5pt is 0.42pt, not the
            // 0.6 that was here.
            Text(label)
                .font(Theme.ui(10.5, .semibold))
                .tracking(0.42)
                .foregroundColor(Theme.text3)
            if loading {
                RoundedRectangle(cornerRadius: Theme.rSm).fill(Theme.hair).frame(width: 54, height: 20)
            } else {
                Text(value)
                    .font(Theme.mono(22, .bold))
                    .foregroundColor(valueColor)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        // `padding: Theme.s4` rather than a bare 14: this is cpCard's own default
        // spelled out, and it moves with it. BetDetailView's numberTile passes the
        // same pair so the two tile shapes stay identical.
        .cpCard(radius: Theme.rMd, padding: Theme.s4)
    }
}

// MARK: - Empty / error / loading states

struct EmptyStateView: View {
    var systemImage: String = "tray"
    let title: String
    var message: String? = nil
    var body: some View {
        // 10 -> s3 (12): a tie between s2 and s3, rounded up.
        VStack(spacing: Theme.s3) {
            Image(systemName: systemImage)
                .font(.system(size: 34, weight: .regular))
                .foregroundColor(Theme.text4)
            Text(title).font(Theme.ui(16, .semibold)).foregroundColor(Theme.text2)
            if let message {
                Text(message).font(Theme.ui(13)).foregroundColor(Theme.text3)
                    .multilineTextAlignment(.center)
            }
        }
        .frame(maxWidth: .infinity)
        // 44 -> s12 (48): 44 is exactly equidistant between s10 and s12 and ties
        // round up. 24 was already s6's value. `ErrorStateView` below lands on
        // s10/s6 — the two states are interchangeable in the same slot, so the
        // horizontal inset is deliberately identical; the 8pt vertical difference
        // between them (was 4pt) is the taller state having a taller glyph stack.
        .padding(.vertical, Theme.s12)
        .padding(.horizontal, Theme.s6)
    }
}

struct ErrorStateView: View {
    let message: String
    var retry: (() -> Void)? = nil
    var body: some View {
        // 12 is already s3's value.
        VStack(spacing: Theme.s3) {
            // The glyph is legitimately `amber` — this is the one place amber is
            // doing its real job as the warning colour (the push outcome, which
            // used to share it, now has `Theme.push`).
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 30))
                .foregroundColor(Theme.amber)
            // The message stays neutral `text2` rather than web's --red-2: this
            // is a full-panel empty/error state, not an inline field error, and
            // a wall of red body copy over a failed fetch overstates it. The
            // amber glyph already carries the signal.
            Text(message)
                .font(Theme.ui(14))
                .foregroundColor(Theme.text2)
                .multilineTextAlignment(.center)
            if let retry {
                Button("Try again", action: retry)
                    .buttonStyle(GhostButtonStyle())
            }
        }
        .frame(maxWidth: .infinity)
        // Both values were already on the scale (40 = s10, 24 = s6); only the
        // spelling changed. The horizontal matches `EmptyStateView`'s on purpose.
        .padding(.vertical, Theme.s10)
        .padding(.horizontal, Theme.s6)
    }
}

/// A shimmer-free skeleton row used while a board loads (avoids showing
/// misleading zeros).
struct SkeletonRow: View {
    var body: some View {
        // `Theme.rSm`, not a bare 4: web's .cp-skel is --r-sm, and 4 is off the
        // radius scale entirely.
        // Both 10s -> s3 (12): a tie between s2 and s3, rounded up. The skeleton
        // stands in for a real row, so its rhythm should be a scale step like the
        // row's. The `frame(width:height:)` values are intrinsic sizing, not
        // spacing, and stay literal.
        HStack(spacing: Theme.s3) {
            RoundedRectangle(cornerRadius: Theme.rSm).fill(Theme.hair).frame(width: 120, height: 14)
            Spacer()
            RoundedRectangle(cornerRadius: Theme.rSm).fill(Theme.hair).frame(width: 44, height: 14)
        }
        .padding(.vertical, Theme.s3)
    }
}

// MARK: - Data-age pill

struct DataAgePill: View {
    let date: Date?
    var body: some View {
        // 5 -> s1 (4) and 9 -> s2 (8): both are the nearest step (5 is 1pt from
        // s1 and 3pt from s2; 9 is 1pt from s2 and 3pt from s3), so the pill
        // tightens by 1pt on the dot gap and 1pt on each side. The vertical 4 was
        // already s1's value — a 4pt value is on the scale, not an exemption.
        HStack(spacing: Theme.s1) {
            Circle().fill(Theme.green).frame(width: 6, height: 6)
            Text("Updated \(Fmt.relativeAge(date))")
                .font(Theme.mono(11, .medium))
                .foregroundColor(Theme.text3)
        }
        .padding(.vertical, Theme.s1)
        .padding(.horizontal, Theme.s2)
        .background(Theme.controlBg)
        .clipShape(Capsule())
    }
}

// MARK: - Section header

struct SectionHeader: View {
    let title: String
    var subtitle: String? = nil
    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(title).font(Theme.ui(17, .bold)).foregroundColor(Theme.text)
            if let subtitle {
                Text(subtitle).font(Theme.ui(13)).foregroundColor(Theme.text3)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

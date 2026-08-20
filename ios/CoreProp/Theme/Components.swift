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
struct CardModifier: ViewModifier {
    var radius: CGFloat = Theme.radius
    var padding: CGFloat = 14
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
    func cpCard(radius: CGFloat = Theme.radius, padding: CGFloat = 14) -> some View {
        modifier(CardModifier(radius: radius, padding: padding))
    }
}

// MARK: - Buttons

/// Primary CTA: flat `--primary` fill, white label, pill shape, darkens when
/// pressed (never lightens).
struct PrimaryButtonStyle: ButtonStyle {
    var fullWidth: Bool = true
    var enabled: Bool = true
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(Theme.ui(15, .semibold))
            .foregroundColor(.white)
            .frame(maxWidth: fullWidth ? .infinity : nil)
            .padding(.vertical, 12)
            .padding(.horizontal, 18)
            .background(
                (configuration.isPressed ? Theme.primaryHover : Theme.primary)
                    .opacity(enabled ? 1 : 0.4)
            )
            .clipShape(Capsule())
            .contentShape(Capsule())
    }
}

/// Secondary/ghost: transparent with a hairline border.
struct GhostButtonStyle: ButtonStyle {
    var fullWidth: Bool = false
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(Theme.ui(15, .semibold))
            .foregroundColor(Theme.text)
            .frame(maxWidth: fullWidth ? .infinity : nil)
            .padding(.vertical, 12)
            .padding(.horizontal, 18)
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
                .padding(.horizontal, 13)
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
            .kerning(0.4)
            .foregroundColor(color)
            .padding(.vertical, 3)
            .padding(.horizontal, 8)
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
            .padding(.vertical, 2)
            .padding(.horizontal, 6)
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
        VStack(alignment: .leading, spacing: 6) {
            // Not `.uppercased()`: web's .bt-card-label was de-capsed in the
            // typography pass, because an all-caps tile label competes with the
            // 22pt number it is labelling. And `.tracking()`, not `.kerning()` —
            // tracking is the true letter-spacing analogue (kerning adjusts
            // pairs). Web's micro-label tracking is .04em, which at 10.5pt is
            // 0.42pt, not the 0.6 that was here.
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
        .cpCard(radius: Theme.rMd, padding: 14)
    }
}

// MARK: - Empty / error / loading states

struct EmptyStateView: View {
    var systemImage: String = "tray"
    let title: String
    var message: String? = nil
    var body: some View {
        VStack(spacing: 10) {
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
        .padding(.vertical, 44)
        .padding(.horizontal, 24)
    }
}

struct ErrorStateView: View {
    let message: String
    var retry: (() -> Void)? = nil
    var body: some View {
        VStack(spacing: 12) {
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
        .padding(.vertical, 40)
        .padding(.horizontal, 24)
    }
}

/// A shimmer-free skeleton row used while a board loads (avoids showing
/// misleading zeros).
struct SkeletonRow: View {
    var body: some View {
        // `Theme.rSm`, not a bare 4: web's .cp-skel is --r-sm, and 4 is off the
        // radius scale entirely.
        HStack(spacing: 10) {
            RoundedRectangle(cornerRadius: Theme.rSm).fill(Theme.hair).frame(width: 120, height: 14)
            Spacer()
            RoundedRectangle(cornerRadius: Theme.rSm).fill(Theme.hair).frame(width: 44, height: 14)
        }
        .padding(.vertical, 10)
    }
}

// MARK: - Data-age pill

struct DataAgePill: View {
    let date: Date?
    var body: some View {
        HStack(spacing: 5) {
            Circle().fill(Theme.green).frame(width: 6, height: 6)
            Text("Updated \(Fmt.relativeAge(date))")
                .font(Theme.mono(11, .medium))
                .foregroundColor(Theme.text3)
        }
        .padding(.vertical, 4)
        .padding(.horizontal, 9)
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

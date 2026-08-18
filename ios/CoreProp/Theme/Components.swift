import SwiftUI
import CorePropKit

// MARK: - Card surface

/// The neutral card surface: a subtle two-stop vertical grey gradient (allowed
/// on non-accent surfaces), a 1px hairline border, `--radius` corners, and the
/// single card shadow.
struct CardModifier: ViewModifier {
    var radius: CGFloat = Theme.radius
    var padding: CGFloat = 14
    func body(content: Content) -> some View {
        content
            .padding(padding)
            .background(
                LinearGradient(colors: [Theme.cardGradTop, Theme.cardGradBot],
                               startPoint: .top, endPoint: .bottom)
            )
            .clipShape(RoundedRectangle(cornerRadius: radius, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: radius, style: .continuous)
                    .stroke(Theme.hair2, lineWidth: 1)
            )
            .shadow(color: .black.opacity(0.35), radius: 24, x: 0, y: 16)
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
                .padding(.vertical, 7)
                .padding(.horizontal, 13)
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
        case .bad:     return Color(hex: 0xFCA5A5)
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(label.uppercased())
                .font(Theme.ui(10.5, .semibold))
                .kerning(0.6)
                .foregroundColor(Theme.text3)
            if loading {
                RoundedRectangle(cornerRadius: 4).fill(Theme.hair).frame(width: 54, height: 20)
            } else {
                Text(value)
                    .font(Theme.mono(22, .bold))
                    .foregroundColor(valueColor)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .cpCard(radius: 12, padding: 14)
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
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 30))
                .foregroundColor(Theme.amber)
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
        HStack(spacing: 10) {
            RoundedRectangle(cornerRadius: 4).fill(Theme.hair).frame(width: 120, height: 14)
            Spacer()
            RoundedRectangle(cornerRadius: 4).fill(Theme.hair).frame(width: 44, height: 14)
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

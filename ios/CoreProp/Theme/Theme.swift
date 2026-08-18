import SwiftUI

/// The CoreProp design system, ported verbatim from the `:root` block of
/// `web/static/index.html`. Dark-mode only (there is no light variant).
///
/// Load-bearing rules (from CLAUDE.md — breaking these is a visual/contrast
/// regression, not a style choice):
/// * `primary` (#1E6FB0) is **fill/border only** — white text on it clears
///   WCAG AA. Never use it as text.
/// * `primary2` (#6FBCEC) is **text/icon only** on the dark page — never put
///   white on it.
/// * `text4` (#4a4a59) is decorative/disabled glyphs only (2.3:1) — never for
///   text a user reads; readable muted text uses `text3`.
/// * Hover/pressed states **darken**; there are no gradients on accent
///   surfaces, no blurred orbs, no gradient-clipped text.
enum Theme {
    // Backgrounds & surfaces (elevation ladder).
    static let bg     = Color(hex: 0x0A0A0D)
    static let bg2    = Color(hex: 0x11111A)
    static let card   = Color(hex: 0x14141E)
    static let card2  = Color(hex: 0x1A1A25)
    static let card3  = Color(hex: 0x22222F)
    /// Neutral card gradient stops (allowed on non-accent surfaces).
    static let cardGradTop = Color(hex: 0x15151F)
    static let cardGradBot = Color(hex: 0x0F0F17)
    static let inputBg = Color(hex: 0x0E0E16)
    static let controlBg = Color(hex: 0x0A0A12)

    // Hairlines.
    static let hair  = Color.white.opacity(0.06)
    static let hair2 = Color.white.opacity(0.10)

    // Text.
    static let text  = Color(hex: 0xF4F4F8)
    static let text2 = Color(hex: 0xB9B9C8)
    static let text3 = Color(hex: 0x8A8A9B)   // readable muted (AA-safe)
    static let text4 = Color(hex: 0x4A4A59)   // decorative/disabled ONLY

    // Accent (flat, never gradient).
    static let primary   = Color(hex: 0x1E6FB0)   // fill/border only
    static let primaryHover = Color(hex: 0x195F97)
    static let primary2  = Color(hex: 0x6FBCEC)   // text/icon only
    static let primaryHi = Color(hex: 0x1E6FB0).opacity(0.22)

    // Semantic outcome colors.
    static let green   = Color(hex: 0x22C55E)
    static let greenHi = Color(hex: 0x22C55E).opacity(0.14)
    static let red     = Color(hex: 0xEF4444)
    static let redHi   = Color(hex: 0xEF4444).opacity(0.10)
    static let amber   = Color(hex: 0xF59E0B)
    static let saveGreen = Color(hex: 0x16A34A)

    // Bet side.
    static let sideOver  = green
    static let sideUnder = Color(hex: 0x60A5FA)
    static let pending   = Color(hex: 0x60A5FA)

    // Radii.
    static let radius: CGFloat = 14
    static let radiusSm: CGFloat = 10
    static let radiusXs: CGFloat = 8

    // MARK: Fonts
    //
    // Web uses Inter (UI) + JetBrains Mono (all numbers/odds/%). On iOS the
    // system font (SF Pro) covers UI, and monospaced-digit variants cover the
    // numeric role without bundling web fonts — the intent (tabular numerals
    // for odds/%) is preserved.

    static func ui(_ size: CGFloat, _ weight: Font.Weight = .regular) -> Font {
        .system(size: size, weight: weight)
    }

    /// Monospaced-design font for odds, percentages, lines, timestamps, EV,
    /// and stat values (the JetBrains Mono role).
    static func mono(_ size: CGFloat, _ weight: Font.Weight = .medium) -> Font {
        .system(size: size, weight: weight, design: .monospaced)
    }

    // MARK: Book / league colors (from components.jsx BookBadge / LeaguePill)

    static func bookColors(_ book: Book) -> (bg: Color, fg: Color) {
        switch book {
        case .fanduel:    return (Color(hex: 0xEF4444).opacity(0.16), Color(hex: 0xFCA5A5))
        case .draftkings: return (Color(hex: 0x22C55E).opacity(0.16), Color(hex: 0x86EFAC))
        case .pinnacle:   return (Color(hex: 0xFACC15).opacity(0.18), Color(hex: 0xFDE68A))
        case .novig:      return (Color(hex: 0x2DD4BF).opacity(0.18), Color(hex: 0x5EEAD4))
        }
    }

    static func leagueColor(_ league: String) -> Color {
        switch league.uppercased() {
        case "NBA":   return Color(hex: 0xF97316)
        case "NHL":   return Color(hex: 0x60A5FA)
        case "MLB":   return Color(hex: 0x34D399)
        case "WNBA":  return Color(hex: 0xF472B6)
        case "NCAAB": return Color(hex: 0xFBBF24)
        case "NFL":   return Color(hex: 0xFCD34D)
        default:      return Color(hex: 0x9CA3AF)
        }
    }
}

extension Color {
    /// Hex initializer, e.g. `Color(hex: 0x1E6FB0)`.
    init(hex: UInt32, alpha: Double = 1.0) {
        let r = Double((hex >> 16) & 0xFF) / 255.0
        let g = Double((hex >> 8) & 0xFF) / 255.0
        let b = Double(hex & 0xFF) / 255.0
        self.init(.sRGB, red: r, green: g, blue: b, opacity: alpha)
    }
}

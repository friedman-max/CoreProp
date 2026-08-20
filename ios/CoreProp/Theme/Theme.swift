import SwiftUI
import CorePropKit

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
/// * **Card surfaces are flat.** The narrower "no gradients on *accent*
///   surfaces" reading once licensed a two-stop grey gradient on every card;
///   it no longer does. The one permitted gradient in the app is the P&L
///   chart's area fill, which is legitimate because it is *semantic* (it
///   encodes up/down and changes colour with the sign) rather than decorative
///   accent.
enum Theme {
    // Backgrounds & surfaces (elevation ladder).
    static let bg     = Color(hex: 0x0A0A0D)
    static let bg2    = Color(hex: 0x11111A)
    static let card   = Color(hex: 0x14141E)
    static let card2  = Color(hex: 0x1A1A25)
    static let card3  = Color(hex: 0x22222F)
    /// Retained-but-unused. These were the two stops of the old card gradient,
    /// back when the rule was read as "gradients are fine on non-accent
    /// surfaces". Cards are FLAT now — the card surface is `card`, full stop —
    /// so nothing should reference these. They are kept only because this phase
    /// removed gradient *usage* without changing any hex value; they have no web
    /// counterpart (#15151F/#0F0F17 appear nowhere in index.html). Do not
    /// reintroduce a card gradient from them.
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
    /// Row-level accent tint, mirroring web's --primary-lo. Use this, not
    /// `primaryHi`, behind a whole row: `primaryHi` (.22) is for rings and
    /// badges where an explicit light colour sits on top, but behind a row it is
    /// inherited by muted text, which then measures 4.45:1 and fails AA.
    static let primaryLo = Color(hex: 0x1E6FB0, alpha: 0.10)

    // Semantic outcome colors.
    //
    // The `*2` variants are the lighter TEXT colours that read on top of a tint
    // of their base. The base (`green`/`red`/...) is the semantic colour for
    // bars, borders and fills; the `*2` is what goes on top of one. These four
    // were inlined as raw hexes at 22 sites before being named here.
    static let green   = Color(hex: 0x22C55E)
    static let greenHi = Color(hex: 0x22C55E).opacity(0.14)
    /// Web's --green-2. Hit / unlocked / positive-outcome label colour.
    static let green2  = Color(hex: 0x86EFAC)
    static let red     = Color(hex: 0xEF4444)
    static let redHi   = Color(hex: 0xEF4444).opacity(0.10)
    /// Web's --red-2. Every error message, miss chip and negative number.
    static let red2    = Color(hex: 0xFCA5A5)
    /// The WARNING colour. Not the push outcome — see `push`.
    static let amber   = Color(hex: 0xF59E0B)
    /// Web's --amber-2. The push label, on a `push` tint.
    static let amber2  = Color(hex: 0xFDE68A)
    /// Web's --blue-2. The pending label, on a `pending` tint.
    static let blue2   = Color(hex: 0x93C5FD)
    /// The push outcome is #FBBF24 on web, NOT --amber (#F59E0B). iOS conflated
    /// the two, which coupled an outcome to a warning; `amber` stays the warning
    /// colour and this is the outcome colour.
    static let push    = Color(hex: 0xFBBF24)
    static let saveGreen = Color(hex: 0x16A34A)

    // Bet side.
    static let sideOver  = green
    static let sideUnder = Color(hex: 0x60A5FA)
    static let pending   = Color(hex: 0x60A5FA)

    // MARK: Radii
    //
    // Mirrors web's --r-* scale. The three legacy names are kept as aliases so
    // that none of the existing call sites breaks: `radius` backs cpCard's
    // default (13 bare `.cpCard()` sites), `radiusSm` has 8 direct call sites
    // and `radiusXs` has 2 (BookBadgeView and the BetsView filter-menu button).
    // Note web DELETED its --radius-xs because it had zero consumers there;
    // here it has two, so it stays — the two decisions only look contradictory.
    //
    // Re-pointing is NOT value-neutral: `radius` goes 14 -> 16 and `radiusSm`
    // 10 -> 12, so every default-radius card moves at once. That is intended.
    // There is deliberately no `rPill` — `Capsule()` is already the idiom.
    static let rXl: CGFloat = 20
    static let rLg: CGFloat = 16
    static let rMd: CGFloat = 12
    static let rSm: CGFloat = 8
    static let radius: CGFloat = rLg      // was 14
    static let radiusSm: CGFloat = rMd    // was 10
    static let radiusXs: CGFloat = rSm    // was 8, unchanged

    // MARK: Spacing
    //
    // Mirrors web's --s-* scale exactly, including its gaps: there is no s7, s9
    // or s11 on either side, so don't "complete" the sequence. Literal padding
    // values across the app migrate onto these; 14 was the most common literal
    // and maps to s4 (16), so rows get roomier — web accepted the same ~10%
    // drop in rows-above-fold for the same reason.
    static let s1: CGFloat = 4
    static let s2: CGFloat = 8
    static let s3: CGFloat = 12
    static let s4: CGFloat = 16
    static let s5: CGFloat = 20
    static let s6: CGFloat = 24
    static let s8: CGFloat = 32
    static let s10: CGFloat = 40
    static let s12: CGFloat = 48

    // MARK: Elevation
    //
    // Web's --shadow-card is `0 12px 32px -18px rgba(0,0,0,.7)`. CSS's negative
    // spread has no SwiftUI analogue, so this approximates it as a tighter,
    // darker shadow than the one it replaces (black .35 / radius 24 / y 16).
    // Worth knowing: web puts --shadow-card on only TWO surfaces (.cp-modal and
    // .pp-card) and most web cards carry no shadow at all, where iOS shadows
    // every card through cpCard — so iOS reads heavier than web even at parity.
    static let shadowColor = Color.black.opacity(0.7)
    static let shadowRadius: CGFloat = 16
    static let shadowY: CGFloat = 6
    /// Focus-ring width, mirroring --ring's 4px on --primary-hi.
    static let ringWidth: CGFloat = 4

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

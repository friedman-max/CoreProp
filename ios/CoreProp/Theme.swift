import SwiftUI

/// Mirrors the CSS custom properties at the top of web/static/index.html.
/// If those tokens change, change these too — the native chrome sits flush
/// against the web content and any drift shows up as a visible seam.
enum Theme {
    /// --bg
    static let background = Color(red: 0x0A / 255, green: 0x0A / 255, blue: 0x0D / 255)
    /// --card
    static let card = Color(red: 0x14 / 255, green: 0x14 / 255, blue: 0x1E / 255)
    /// --primary
    static let accent = Color(red: 0x1E / 255, green: 0x6F / 255, blue: 0xB0 / 255)
    /// --text-2
    static let secondaryText = Color(white: 0.68)
}

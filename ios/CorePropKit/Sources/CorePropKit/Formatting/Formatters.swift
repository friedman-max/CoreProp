import Foundation

/// Display formatting shared by every screen. Pure functions so they can be
/// verified without a UI.
public enum Fmt {
    // MARK: American odds

    /// American odds as `+150` / `-200`, or `—` for missing. American odds are
    /// always whole numbers; the devig math produces floats, so we round.
    public static func americanOdds(_ v: Double?) -> String {
        guard let v else { return "—" }
        let n = Int(v.rounded())
        return n > 0 ? "+\(n)" : "\(n)"
    }

    public static func americanOdds(_ v: Int?) -> String {
        guard let v else { return "—" }
        return v > 0 ? "+\(v)" : "\(v)"
    }

    // MARK: Percentages

    /// A 0…1 probability as a percentage string, e.g. `54.7%`.
    public static func percent(_ p: Double?, decimals: Int = 1) -> String {
        guard let p else { return "—" }
        return String(format: "%.\(decimals)f%%", p * 100)
    }

    /// An already-scaled percent value (0…100).
    public static func percentValue(_ v: Double?, decimals: Int = 1) -> String {
        guard let v else { return "—" }
        return String(format: "%.\(decimals)f%%", v)
    }

    /// A signed EV/edge fraction (e.g. 0.083) as `+8.3%` / `-2.1%`.
    public static func signedPercent(_ frac: Double?, decimals: Int = 1) -> String {
        guard let frac else { return "—" }
        let pct = frac * 100
        let sign = pct >= 0 ? "+" : ""
        return "\(sign)\(String(format: "%.\(decimals)f", pct))%"
    }

    // MARK: Currency

    public static func currency(_ v: Double?, maximumFractionDigits: Int = 2) -> String {
        guard let v else { return "—" }
        let nf = NumberFormatter()
        nf.numberStyle = .currency
        nf.currencyCode = "USD"
        nf.maximumFractionDigits = maximumFractionDigits
        return nf.string(from: NSNumber(value: v)) ?? String(format: "$%.2f", v)
    }

    /// A signed dollar amount, e.g. `+$41.50` / `-$12.00`.
    public static func signedCurrency(_ v: Double?) -> String {
        guard let v else { return "—" }
        let sign = v >= 0 ? "+" : "-"
        return sign + currency(abs(v))
    }

    // MARK: Lines / numbers

    /// A prop line: `3` when whole, `3.5` otherwise.
    public static func line(_ v: Double?) -> String {
        guard let v else { return "—" }
        if v == v.rounded() { return String(Int(v)) }
        return String(format: "%g", v)
    }

    // MARK: Dates

    /// A game time relative to now: `Today 7:30 PM`, `Tomorrow 1:05 PM`,
    /// `Wed 7:30 PM`, or `Aug 24, 7:30 PM` for anything further out.
    public static func gameTime(_ date: Date?, now: Date = Date(),
                                calendar: Calendar = .current) -> String {
        guard let date else { return "—" }
        let time = DateFormatter()
        time.locale = Locale(identifier: "en_US_POSIX")
        time.dateFormat = "h:mm a"
        let t = time.string(from: date)

        if calendar.isDateInToday(date) { return "Today \(t)" }
        if calendar.isDateInTomorrow(date) { return "Tomorrow \(t)" }
        if calendar.isDateInYesterday(date) { return "Yesterday \(t)" }

        let days = calendar.dateComponents([.day], from: now, to: date).day ?? 0
        let df = DateFormatter()
        df.locale = Locale(identifier: "en_US_POSIX")
        if abs(days) < 7 {
            df.dateFormat = "EEE h:mm a"    // Wed 7:30 PM
        } else {
            df.dateFormat = "MMM d, h:mm a" // Aug 24, 7:30 PM
        }
        return df.string(from: date)
    }

    /// "Updated 2m ago" style relative age for the data-freshness pill.
    public static func relativeAge(_ date: Date?, now: Date = Date()) -> String {
        guard let date else { return "—" }
        let secs = Int(now.timeIntervalSince(date))
        if secs < 5 { return "just now" }
        if secs < 60 { return "\(secs)s ago" }
        let mins = secs / 60
        if mins < 60 { return "\(mins)m ago" }
        let hours = mins / 60
        if hours < 24 { return "\(hours)h ago" }
        return "\(hours / 24)d ago"
    }

    /// A short date like `Aug 18` for grouping logged slips.
    public static func shortDate(_ date: Date?) -> String {
        guard let date else { return "—" }
        let df = DateFormatter()
        df.locale = Locale(identifier: "en_US_POSIX")
        df.dateFormat = "MMM d"
        return df.string(from: date)
    }
}

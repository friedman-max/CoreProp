import Foundation

/// Tolerant ISO-8601 parsing. The backend emits several shapes:
/// `2026-08-18T19:30:00+00:00`, `...Z`, with fractional seconds
/// (`...:00.123456+00:00`), and occasionally naive (no timezone) timestamps
/// from `datetime.isoformat()`. `ISO8601DateFormatter` alone rejects several of
/// these, so we try a small ladder of parsers.
public enum ISO8601Date {
    private static let withFractional: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }()

    private static let plain: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f
    }()

    /// Handles naive timestamps (no timezone), interpreted as UTC.
    private static let naive: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        return f
    }()

    private static let naiveFractional: DateFormatter = {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.timeZone = TimeZone(identifier: "UTC")
        f.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        return f
    }()

    public static func parse(_ string: String?) -> Date? {
        guard let s = string, !s.isEmpty else { return nil }
        if let d = withFractional.date(from: s) { return d }
        if let d = plain.date(from: s) { return d }
        if let d = naiveFractional.date(from: s) { return d }
        if let d = naive.date(from: s) { return d }
        return nil
    }
}

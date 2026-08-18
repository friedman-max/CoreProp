import Foundation

/// Helpers for decoding fields whose JSON type is not stable across the
/// backend's history. Some persisted columns (`line`, `stat_actual`) can arrive
/// as a JSON number, a numeric string, or null depending on how old the row is
/// and which write path produced it, so a plain `Double?` decode would throw on
/// the string form and drop an otherwise valid slip.
extension KeyedDecodingContainer {
    /// Decode a value that may be a number, a numeric string, or absent/null.
    func decodeLenientDouble(forKey key: Key) -> Double? {
        if let d = try? decodeIfPresent(Double.self, forKey: key) {
            return d
        }
        if let s = try? decodeIfPresent(String.self, forKey: key) {
            let trimmed = s.trimmingCharacters(in: .whitespaces)
            return Double(trimmed)
        }
        return nil
    }

    /// Decode an integer that may arrive as a number or a numeric string.
    func decodeLenientInt(forKey key: Key) -> Int? {
        if let i = try? decodeIfPresent(Int.self, forKey: key) {
            return i
        }
        if let d = try? decodeIfPresent(Double.self, forKey: key) {
            return Int(d)
        }
        if let s = try? decodeIfPresent(String.self, forKey: key) {
            return Int(s.trimmingCharacters(in: .whitespaces))
        }
        return nil
    }

    /// Decode a string that may arrive as a number (e.g. a `line` rendered as a
    /// caption). Returns nil for absent/null.
    func decodeLenientString(forKey key: Key) -> String? {
        if let s = try? decodeIfPresent(String.self, forKey: key) {
            return s
        }
        if let d = try? decodeIfPresent(Double.self, forKey: key) {
            // Render 3.0 as "3" and 3.5 as "3.5", matching the web UI.
            if d == d.rounded() { return String(Int(d)) }
            return String(d)
        }
        return nil
    }
}

public extension JSONDecoder {
    /// The one decoder used for every backend + Supabase payload. Snake-case
    /// conversion means the models can use idiomatic camelCase property names
    /// while the wire format stays snake_case (matching FastAPI / GoTrue).
    static func coreProp() -> JSONDecoder {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }
}

// NOTE: there is deliberately no `JSONEncoder.coreProp()` with
// `.convertToSnakeCase`. Request bodies with case-sensitive dictionary keys
// (e.g. `active_leagues` → {"NBA": true}) must not be run through a key
// strategy, whose behavior on all-caps/dictionary keys varies across Swift
// versions and platforms. All request bodies use a plain `JSONEncoder()` with
// explicit snake_case `CodingKeys` where a mapping is needed.

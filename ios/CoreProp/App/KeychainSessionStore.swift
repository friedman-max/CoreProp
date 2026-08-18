import Foundation
import Security
import CorePropKit

/// Persists the Supabase session in the Keychain (not UserDefaults — the
/// refresh token is a long-lived credential). Encoded/decoded with a plain
/// JSON coder we own on both sides, so the camelCase property names round-trip
/// (this is *not* the snake_case wire format from GoTrue).
final class KeychainSessionStore: SessionStore {
    private let service = "me.coreprop.app"
    private let account = "supabase.session"

    func load() -> AuthSession? {
        var query: [String: Any] = baseQuery()
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne
        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        guard status == errSecSuccess, let data = item as? Data else { return nil }
        return try? JSONDecoder().decode(AuthSession.self, from: data)
    }

    func save(_ session: AuthSession?) {
        guard let session else {
            SecItemDelete(baseQuery() as CFDictionary)
            return
        }
        guard let data = try? JSONEncoder().encode(session) else { return }

        // Update if present, else add.
        let attrs: [String: Any] = [kSecValueData as String: data]
        let updateStatus = SecItemUpdate(baseQuery() as CFDictionary, attrs as CFDictionary)
        if updateStatus == errSecItemNotFound {
            var add = baseQuery()
            add[kSecValueData as String] = data
            add[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
            SecItemAdd(add as CFDictionary, nil)
        }
    }

    private func baseQuery() -> [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]
    }
}

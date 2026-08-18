import Foundation

/// Where the current Supabase session is persisted. The app supplies a
/// Keychain-backed implementation (`KeychainSessionStore` in the app target);
/// `InMemorySessionStore` is used for previews, tests, and the verifier.
public protocol SessionStore: AnyObject {
    func load() -> AuthSession?
    func save(_ session: AuthSession?)
}

/// Non-persistent store. Thread-safe via an internal lock.
public final class InMemorySessionStore: SessionStore, @unchecked Sendable {
    private let lock = NSLock()
    private var session: AuthSession?

    public init(_ initial: AuthSession? = nil) { self.session = initial }

    public func load() -> AuthSession? {
        lock.lock(); defer { lock.unlock() }
        return session
    }

    public func save(_ session: AuthSession?) {
        lock.lock(); defer { lock.unlock() }
        self.session = session
    }
}

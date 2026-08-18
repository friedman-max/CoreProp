import Foundation
import CorePropKit

/// Simple async-load state for a screen's primary data.
enum LoadState: Equatable {
    case idle
    case loading
    case loaded
    case empty
    case failed(String)

    var isLoading: Bool { self == .loading }
}

extension APIError {
    /// A short, user-facing string for a load failure.
    var display: String { errorDescription ?? "Something went wrong." }
}

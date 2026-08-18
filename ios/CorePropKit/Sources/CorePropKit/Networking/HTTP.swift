import Foundation

/// A thin async wrapper over `URLSession` that maps failures to `APIError` and
/// decodes with the shared snake_case decoder. Shared by `CoreClient`
/// (FastAPI backend) and `SupabaseAuthClient` (GoTrue).
public struct HTTPClient: Sendable {
    public let session: URLSession
    public init(session: URLSession = .shared) { self.session = session }

    /// Perform the request, returning the raw body + response, or throwing an
    /// `APIError` for transport failures and non-2xx status codes.
    public func perform(_ request: URLRequest) async throws -> (Data, HTTPURLResponse) {
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch let urlError as URLError {
            throw APIError.transport(Self.friendlyTransportMessage(urlError))
        } catch {
            throw APIError.transport(error.localizedDescription)
        }
        guard let http = response as? HTTPURLResponse else {
            throw APIError.transport("Malformed server response.")
        }
        guard (200...299).contains(http.statusCode) else {
            throw APIError.http(status: http.statusCode, message: Self.extractMessage(from: data))
        }
        return (data, http)
    }

    /// Perform + decode into `T`.
    public func send<T: Decodable>(_ request: URLRequest, as type: T.Type) async throws -> T {
        let (data, _) = try await perform(request)
        // 204 / empty body but a decodable type expected: surface a clear error.
        if data.isEmpty {
            throw APIError.decoding("Empty response body.")
        }
        do {
            return try JSONDecoder.coreProp().decode(T.self, from: data)
        } catch {
            throw APIError.decoding(String(describing: error))
        }
    }

    /// Perform, ignoring the body (for 2xx/204 endpoints).
    public func sendVoid(_ request: URLRequest) async throws {
        _ = try await perform(request)
    }

    // MARK: Message extraction

    /// Pull a human-readable message out of a FastAPI (`{"detail": ...}`) or
    /// GoTrue error body, falling back to a truncated raw string.
    static func extractMessage(from data: Data) -> String? {
        guard !data.isEmpty else { return nil }
        if let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            // FastAPI: detail can be a string or a list of validation errors.
            if let detail = obj["detail"] as? String { return detail }
            if let detail = obj["detail"] as? [[String: Any]] {
                let msgs = detail.compactMap { $0["msg"] as? String }
                if !msgs.isEmpty { return msgs.joined(separator: "; ") }
            }
            // GoTrue: error_description / msg / message / error.
            for key in ["error_description", "msg", "message", "error"] {
                if let s = obj[key] as? String { return s }
            }
        }
        let raw = String(data: data, encoding: .utf8) ?? ""
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : String(trimmed.prefix(200))
    }

    static func friendlyTransportMessage(_ error: URLError) -> String {
        switch error.code {
        case .notConnectedToInternet: return "You appear to be offline."
        case .timedOut:               return "The request timed out. Try again."
        case .cannotFindHost, .cannotConnectToHost, .dnsLookupFailed:
            return "Couldn't reach CoreProp. Check the server address and your connection."
        case .networkConnectionLost:  return "The network connection was lost."
        case .secureConnectionFailed, .serverCertificateUntrusted:
            return "A secure connection to the server failed."
        default:                      return error.localizedDescription
        }
    }
}

// MARK: - Request building helpers

extension URLRequest {
    /// Build a request against `base`, appending `path` and optional query items.
    static func build(
        base: URL,
        path: String,
        method: String = "GET",
        query: [URLQueryItem] = [],
        headers: [String: String] = [:],
        body: Data? = nil,
        timeout: TimeInterval = 20
    ) throws -> URLRequest {
        guard var comps = URLComponents(url: base.appendingPathComponent(path),
                                        resolvingAgainstBaseURL: false) else {
            throw APIError.invalidURL(base.absoluteString + path)
        }
        if !query.isEmpty { comps.queryItems = query }
        guard let url = comps.url else {
            throw APIError.invalidURL(base.absoluteString + path)
        }
        var req = URLRequest(url: url, timeoutInterval: timeout)
        req.httpMethod = method
        for (k, v) in headers { req.setValue(v, forHTTPHeaderField: k) }
        if let body {
            req.httpBody = body
            if req.value(forHTTPHeaderField: "Content-Type") == nil {
                req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            }
        }
        return req
    }
}

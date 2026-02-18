import Foundation

extension BackendService {
    private struct EmailExistsRequest: Codable {
        let email: String
    }

    private struct EmailExistsResponse: Codable {
        let exists: Bool
    }

    func checkEmailExists(email: String) async throws -> Bool {
        let normalizedEmail = email.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()

        func makeRequest(at base: URL) throws -> URLRequest {
            let url = base
                .appendingPathComponent("auth")
                .appendingPathComponent("email-exists")
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.timeoutInterval = BackendService.coreRequestTimeoutSeconds
            request.httpBody = try jsonEncoder.encode(EmailExistsRequest(email: normalizedEmail))
            return request
        }

        var request = try makeRequest(at: baseURL)
        var (data, response) = try await urlSession.data(for: request)
        var http = response as? HTTPURLResponse

        if let h = http, h.statusCode == 404 {
            request = try makeRequest(at: baseURL.appendingPathComponent("api"))
            (data, response) = try await urlSession.data(for: request)
            http = response as? HTTPURLResponse
        }

        guard let final = http else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }

        guard (200..<300).contains(final.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: final.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        return try jsonDecoder.decode(EmailExistsResponse.self, from: data).exists
    }
}

import Foundation

extension BackendService {
    struct InviteInfo: Codable {
        let inviter_name: String
    }

    func fetchInviteInfo(inviteToken: String) async throws -> InviteInfo {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("invite-info")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "code", value: inviteToken)]
        let finalURL = components.url!
        var request = URLRequest(url: finalURL)
        request.httpMethod = "GET"

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        return try jsonDecoder.decode(InviteInfo.self, from: data)
    }

    func createLinkInvite(accessToken: String) async throws -> URL {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("send-invite")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        let decoded = try jsonDecoder.decode(CreateLinkInviteResponseBody.self, from: data)
        guard let shareURL = URL(string: decoded.share_url) else {
            throw NSError(domain: "Backend", code: -2, userInfo: [NSLocalizedDescriptionKey: "Invalid share URL from server"])
        }
        return shareURL
    }

    func acceptLinkInvite(inviteToken: String, accessToken: String) async throws {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("accept-invite")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let payload = AcceptLinkInviteRequestBody(invite_token: inviteToken)
        request.httpBody = try jsonEncoder.encode(payload)

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        let decoded = try jsonDecoder.decode(AcceptLinkInviteResponseBody.self, from: data)
        guard decoded.success else {
            throw NSError(domain: "Backend", code: -3, userInfo: [NSLocalizedDescriptionKey: "Failed to accept link invite"])
        }
    }

    func unlink(accessToken: String) async throws -> Bool {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("unlink-pair")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        struct UnlinkResponseBody: Codable { let success: Bool; let unlinked: Bool }
        let decoded = try jsonDecoder.decode(UnlinkResponseBody.self, from: data)
        guard decoded.success else {
            throw NSError(domain: "Backend", code: -3, userInfo: [NSLocalizedDescriptionKey: "Failed to unlink"])
        }
        return decoded.unlinked
    }

    func fetchLinkStatus(accessToken: String) async throws -> (linked: Bool, relationshipId: UUID?, linkedAt: Date?) {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("status")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        struct StatusBody: Codable { let success: Bool; let linked: Bool; let relationship_id: UUID?; let linked_at: String? }
        let decoded = try jsonDecoder.decode(StatusBody.self, from: data)
        guard decoded.success else {
            throw NSError(domain: "Backend", code: -3, userInfo: [NSLocalizedDescriptionKey: "Failed to fetch link status"])
        }
        var linkedDate: Date? = nil
        if let iso = decoded.linked_at, !iso.isEmpty {
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            linkedDate = formatter.date(from: iso) ?? ISO8601DateFormatter().date(from: iso)
        }
        return (decoded.linked, decoded.relationship_id, linkedDate)
    }
}

private struct CreateLinkInviteResponseBody: Codable {
    let invite_token: String
    let share_url: String
}

private struct AcceptLinkInviteRequestBody: Codable {
    let invite_token: String
}

private struct AcceptLinkInviteResponseBody: Codable {
    let success: Bool
    let relationship_id: UUID?
}


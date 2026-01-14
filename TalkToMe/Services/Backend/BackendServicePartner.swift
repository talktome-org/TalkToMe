import Foundation

extension BackendService {
    struct PartnerRequestBody: Codable { let message: String; let session_id: UUID }
    struct PartnerRequestResponse: Codable { let success: Bool; let request_id: UUID }
    struct PartnerPendingRequest: Codable {
        let id: UUID
        let sender_user_id: UUID
        let sender_session_id: UUID
        let content: String
        let created_at: String
        let status: String
        let recipient_session_id: UUID?
        let created_message_id: UUID?
    }

    struct PartnerPendingRequestsResponse: Codable { let requests: [PartnerPendingRequest] }

    func streamPartnerRequest(_ body: PartnerRequestBody, accessToken: String) -> AsyncStream<StreamEvent> {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("partner")
            .appendingPathComponent("request")
            .appendingPathComponent("stream"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        request.httpBody = try? jsonEncoder.encode(body)
        return SSEService.shared.stream(request: request)
    }

    func getPartnerPendingRequests(accessToken: String) async throws -> PartnerPendingRequestsResponse {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("partner")
            .appendingPathComponent("pending"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        return try jsonDecoder.decode(PartnerPendingRequestsResponse.self, from: data)
    }

    func acceptPartnerRequest(requestId: UUID, accessToken: String) async throws -> UUID {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("partner")
            .appendingPathComponent("requests")
            .appendingPathComponent(requestId.uuidString)
            .appendingPathComponent("accept"))
        request.httpMethod = "POST"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        struct Body: Codable { let success: Bool; let recipient_session_id: UUID }
        let decoded = try jsonDecoder.decode(Body.self, from: data)
        return decoded.recipient_session_id
    }
}


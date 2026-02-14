import Foundation

extension BackendService {
    struct SendPartnerMessageRequest: Codable {
        let message: String
        let session_id: UUID?
        let friend_user_id: UUID?
    }

    struct SendPartnerMessageResponse: Codable {
        let success: Bool
        let recipient_session_id: UUID
    }

    func sendPartnerMessage(
        message: String,
        sessionId: UUID?,
        friendUserId: UUID? = nil,
        accessToken: String
    ) async throws -> SendPartnerMessageResponse {
        let url = baseURL
            .appendingPathComponent("partner")
            .appendingPathComponent("send-message")

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let payload = SendPartnerMessageRequest(message: message, session_id: sessionId, friend_user_id: friendUserId)
        request.httpBody = try jsonEncoder.encode(payload)

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        return try jsonDecoder.decode(SendPartnerMessageResponse.self, from: data)
    }

    private struct UnsendPartnerMessageRequest: Codable {
        let session_id: UUID
        let message_text: String
    }

    func unsendPartnerMessage(
        sessionId: UUID,
        messageText: String,
        accessToken: String
    ) async throws {
        let url = baseURL
            .appendingPathComponent("partner")
            .appendingPathComponent("unsend-message")

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let payload = UnsendPartnerMessageRequest(session_id: sessionId, message_text: messageText)
        request.httpBody = try jsonEncoder.encode(payload)

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
    }
}


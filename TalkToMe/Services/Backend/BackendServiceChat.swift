import Foundation

extension BackendService {
    struct ChatHistoryMessage: Codable {
        let role: String
        let content: String
    }

    struct ChatMessageDTO: Codable {
        let id: UUID
        let user_id: UUID
        let session_id: UUID
        let role: String
        let content: String
        let created_at: String?
    }

    struct ChatSessionDTO: Codable {
        let id: UUID
        let title: String?
        let last_message_at: String?
        let last_message_content: String?
    }
}

// MARK: - Chat streaming + sessions

extension BackendService {
    func streamChatMessage(
        _ message: String,
        sessionId: UUID?,
        chatHistory: [ChatHistoryMessage]?,
        attachments: [ChatAttachment]? = nil,
        accessToken: String,
        previousResponseId: String? = nil,
        friendUserId: UUID? = nil,
        messageId: UUID? = nil,
        ephemeral: Bool = false,
        voiceAgent: String? = nil,
        ghostName: String? = nil
    ) -> AsyncStream<StreamEvent> {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
            .appendingPathComponent("message")
            .appendingPathComponent("stream"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        let payload = ChatRequestBody(
            message: message,
            session_id: sessionId,
            message_id: messageId,
            chat_history: chatHistory,
            previous_response_id: previousResponseId,
            attachments: attachments,
            friend_user_id: friendUserId,
            ephemeral: ephemeral ? true : nil,
            voice_agent: (voiceAgent?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == true) ? nil : voiceAgent,
            ghost_name: (ghostName?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == true) ? nil : ghostName
        )
        request.httpBody = try? jsonEncoder.encode(payload)
        return SSEService.shared.stream(request: request)
    }

    /// Ephemeral streaming for speak mode - no persistence, just AI response
    func streamEphemeralMessage(
        _ message: String,
        chatHistory: [ChatHistoryMessage]?,
        accessToken: String,
        voiceAgent: String?
    ) -> AsyncStream<StreamEvent> {
        return streamChatMessage(
            message,
            sessionId: nil,
            chatHistory: chatHistory,
            attachments: nil,
            accessToken: accessToken,
            previousResponseId: nil,
            friendUserId: nil,
            messageId: nil,
            ephemeral: true,
            voiceAgent: voiceAgent
        )
    }

    func fetchMessages(sessionId: UUID, accessToken: String) async throws -> [ChatMessageDTO] {
        let url = baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
            .appendingPathComponent(sessionId.uuidString)
            .appendingPathComponent("messages")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        let decoded = try jsonDecoder.decode(MessagesResponseBody.self, from: data)
        return decoded.messages
    }

    func fetchSessions(accessToken: String) async throws -> [ChatSessionDTO] {
        let url = baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        let decoded = try jsonDecoder.decode(SessionsResponseBody.self, from: data)
        return decoded.sessions
    }

    func createEmptySession(accessToken: String) async throws -> ChatSessionDTO {
        let url = baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
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

        return try jsonDecoder.decode(ChatSessionDTO.self, from: data)
    }

    func renameSession(sessionId: UUID, title: String?, accessToken: String) async throws {
        func makeRequest(at base: URL) throws -> URLRequest {
            let url = base
                .appendingPathComponent("chat")
                .appendingPathComponent("sessions")
                .appendingPathComponent(sessionId.uuidString)
            var req = URLRequest(url: url)
            req.httpMethod = "PATCH"
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            req.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
            struct Body: Codable { let title: String? }
            req.httpBody = try jsonEncoder.encode(Body(title: (title?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == true) ? nil : title))
            return req
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
    }

    func deleteSession(sessionId: UUID, accessToken: String) async throws {
        func makeRequest(at base: URL) -> URLRequest {
            let url = base
                .appendingPathComponent("chat")
                .appendingPathComponent("sessions")
                .appendingPathComponent(sessionId.uuidString)
            var req = URLRequest(url: url)
            req.httpMethod = "DELETE"
            req.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
            return req
        }

        var request = makeRequest(at: baseURL)
        var (data, response) = try await urlSession.data(for: request)
        var http = response as? HTTPURLResponse
        if let h = http, h.statusCode == 404 {
            var postRequest = makeRequest(at: baseURL)
            postRequest.httpMethod = "POST"
            (data, response) = try await urlSession.data(for: postRequest)
            http = response as? HTTPURLResponse
        }
        if let h = http, h.statusCode == 404 {
            request = makeRequest(at: baseURL.appendingPathComponent("api"))
            (data, response) = try await urlSession.data(for: request)
            http = response as? HTTPURLResponse
        }
        if let h = http, h.statusCode == 404 {
            var postRequest = makeRequest(at: baseURL.appendingPathComponent("api"))
            postRequest.httpMethod = "POST"
            (data, response) = try await urlSession.data(for: postRequest)
            http = response as? HTTPURLResponse
        }
        guard let final = http else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(final.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: final.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
    }

    /// Best-effort deletion of all messages in a session after a given message.
    /// Silently ignores 404 (backend may not support this endpoint yet).
    func deleteMessagesAfter(messageId: UUID, sessionId: UUID, accessToken: String, includeAnchor: Bool = false) async {
        var components = URLComponents(url: baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
            .appendingPathComponent(sessionId.uuidString)
            .appendingPathComponent("messages")
            .appendingPathComponent("after")
            .appendingPathComponent(messageId.uuidString), resolvingAgainstBaseURL: false)!
        if includeAnchor {
            components.queryItems = [URLQueryItem(name: "include_anchor", value: "true")]
        }
        var request = URLRequest(url: components.url!)
        request.httpMethod = "DELETE"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        do {
            let (_, response) = try await urlSession.data(for: request)
            if let http = response as? HTTPURLResponse, http.statusCode == 404 {
                // Try POST variant
                request.httpMethod = "POST"
                _ = try? await urlSession.data(for: request)
            }
        } catch {
            // Best-effort — don't propagate errors
        }
    }

    // Chat attachments
    func uploadChatAttachment(fileData: Data, filename: String, contentType: String, accessToken: String) async throws -> (path: String, url: String?) {
        let url = baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("attachments")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        var body = Data()
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"\(filename)\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: \(contentType)\r\n\r\n".data(using: .utf8)!)
        body.append(fileData)
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)
        request.httpBody = body

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        struct UploadRes: Codable { let path: String?; let url: String? }
        let decoded = try jsonDecoder.decode(UploadRes.self, from: data)
        return (decoded.path ?? "", decoded.url)
    }
}

private struct ChatRequestBody: Codable {
    let message: String
    let session_id: UUID?
    let message_id: UUID?
    let chat_history: [BackendService.ChatHistoryMessage]?
    let previous_response_id: String?
    let attachments: [BackendService.ChatAttachment]?
    let friend_user_id: UUID?
    let ephemeral: Bool?
    let voice_agent: String?
    let ghost_name: String?
}

private struct MessagesResponseBody: Codable {
    let messages: [BackendService.ChatMessageDTO]
}

private struct SessionsResponseBody: Codable {
    let sessions: [BackendService.ChatSessionDTO]
}


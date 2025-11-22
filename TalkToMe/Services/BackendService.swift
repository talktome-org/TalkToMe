import Foundation

struct BackendService {

    static let shared = BackendService()

    private let urlSession: URLSession = .shared
    private let jsonEncoder = JSONEncoder()
    private let jsonDecoder = JSONDecoder()
    let baseURL: URL

    private init() {
        guard let backendURLString = BackendService.getSecretsPlistValue(for: "BACKEND_BASE_URL") as? String,
              let url = URL(string: backendURLString) else {
            fatalError("Missing or invalid BACKEND_BASE_URL in Secrets.plist")
        }
        self.baseURL = url
        print("🌐 BackendService: Initialized with base URL: \(url)")
    }

    enum StreamEvent: Equatable {
        case session(UUID)
        case token(String)
        case partnerMessage(String)
        case toolStart(String)
        case toolArgs(String)
        case toolDone
        case responseId(String)
        case done
        case error(String)
    }

    func streamChatMessage(_ message: String, sessionId: UUID?, chatHistory: [ChatHistoryMessage]?, accessToken: String, focusSnippet: String? = nil, previousResponseId: String? = nil) -> AsyncStream<StreamEvent> {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
            .appendingPathComponent("message")
            .appendingPathComponent("stream"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        let payload = ChatRequestBody(message: message, session_id: sessionId, chat_history: chatHistory, previous_response_id: previousResponseId)
        request.httpBody = try? jsonEncoder.encode(payload)

        return SSEService.shared.stream(request: request)
    }

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

    func registerPushToken(token: String, platform: String, bundleId: String, accessToken: String) async throws {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("notifications")
            .appendingPathComponent("register"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        struct Body: Codable { let token: String; let platform: String; let bundle_id: String }
        request.httpBody = try jsonEncoder.encode(Body(token: token, platform: platform, bundle_id: bundleId))
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
    }

    func unregisterPushToken(token: String, accessToken: String) async throws {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("notifications")
            .appendingPathComponent("unregister"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        struct Body: Codable { let token: String }
        request.httpBody = try jsonEncoder.encode(Body(token: token))
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
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

    func fetchMessages(sessionId: UUID, accessToken: String) async throws -> [ChatMessageDTO] {
        let url = baseURL
            .appendingPathComponent("chat")
            .appendingPathComponent("sessions")
            .appendingPathComponent(sessionId.uuidString)
            .appendingPathComponent("messages")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

    static func getSecretsPlistValue(for key: String) -> Any? {
        if let path = Bundle.main.path(forResource: "Secrets", ofType: "plist"),
           let plist = NSDictionary(contentsOfFile: path),
           let value = plist[key] {
            return value
        }
        return nil
    }

    private func decodeSimpleDetail(from data: Data) -> String? {
        struct SimpleDetail: Decodable { let detail: String? }
        return (try? jsonDecoder.decode(SimpleDetail.self, from: data))?.detail
    }
}


struct ChatHistoryMessage: Codable {
    let role: String
    let content: String
}

private struct ChatRequestBody: Codable {
    let message: String
    let session_id: UUID?
    let chat_history: [ChatHistoryMessage]?
    let previous_response_id: String?
}

private struct ChatResponseBody: Codable {
    let response: String
    let success: Bool
    let session_id: UUID?
}

struct ChatMessageDTO: Codable {
    let id: UUID
    let user_id: UUID
    let session_id: UUID
    let role: String
    let content: String
}

private struct MessagesResponseBody: Codable {
    let messages: [ChatMessageDTO]
}

struct ChatSessionDTO: Codable {
    let id: UUID
    let title: String?
    let last_message_at: String?
    let last_message_content: String?
}

extension BackendService {
    struct DailyCheckinsStatus: Codable { let enabled: Bool; let hour: Int?; let minute: Int? }

    func getDailyCheckins(accessToken: String) async throws -> DailyCheckinsStatus {
        let url = baseURL
            .appendingPathComponent("notifications")
            .appendingPathComponent("daily-checkins")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        return try jsonDecoder.decode(DailyCheckinsStatus.self, from: data)
    }

    func setDailyCheckins(enabled: Bool, hour: Int? = nil, minute: Int? = nil, timezone: String? = nil, accessToken: String) async throws {
        let url = baseURL
            .appendingPathComponent("notifications")
            .appendingPathComponent("daily-checkins")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        struct Body: Codable { let enabled: Bool; let hour: Int?; let minute: Int?; let timezone: String? }
        request.httpBody = try jsonEncoder.encode(Body(enabled: enabled, hour: hour, minute: minute, timezone: timezone))
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
    }
    func uploadAvatar(imageData: Data, contentType: String, accessToken: String) async throws -> (path: String, url: String?) {
        let url = baseURL
            .appendingPathComponent("profile")
            .appendingPathComponent("avatar")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        var body = Data()
        let filename = "avatar"
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"\(filename)\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: \(contentType)\r\n\r\n".data(using: .utf8)!)
        body.append(imageData)
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

    struct PairedAvatars: Codable { struct Entry: Codable { let url: String?; let source: String } ; let me: Entry; let partner: Entry }
    func fetchPairedAvatars(accessToken: String) async throws -> PairedAvatars {
        let url = baseURL
            .appendingPathComponent("profile")
            .appendingPathComponent("avatars")
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
        return try jsonDecoder.decode(PairedAvatars.self, from: data)
    }

    struct ProfileInfo: Codable {
        let full_name: String
        let bio: String
    }

    struct ProfileUpdateResponse: Codable {
        let success: Bool
        let message: String
    }

    func fetchProfileInfo(accessToken: String) async throws -> ProfileInfo {
        func makeRequest(at base: URL) -> URLRequest {
            let url = base
                .appendingPathComponent("profile")
                .appendingPathComponent("info")
            var request = URLRequest(url: url)
            request.httpMethod = "GET"
            request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
            return request
        }

        var request = makeRequest(at: baseURL)
        var (data, response) = try await urlSession.data(for: request)
        var http = response as? HTTPURLResponse
        if let h = http, h.statusCode == 404 { // try /api fallback
            request = makeRequest(at: baseURL.appendingPathComponent("api"))
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
        return try jsonDecoder.decode(ProfileInfo.self, from: data)
    }

    func updateProfile(accessToken: String, fullName: String?, bio: String?, partnerDisplayName: String? = nil) async throws -> ProfileUpdateResponse {
        func makeRequest(at base: URL, method: String) -> URLRequest {
            let url = base
                .appendingPathComponent("profile")
                .appendingPathComponent("update")
            var request = URLRequest(url: url)
            request.httpMethod = method
            request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
            request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

            var formDataComponents: [String] = []
            if let fullName = fullName {
                let encoded = fullName.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? fullName
                formDataComponents.append("full_name=\(encoded)")
            }
            if let bio = bio, !bio.isEmpty {
                let encoded = bio.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? bio
                formDataComponents.append("bio=\(encoded)")
            }
            if let partner = partnerDisplayName, !partner.isEmpty {
                let encoded = partner.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? partner
                formDataComponents.append("partner_display_name=\(encoded)")
            }
            let formDataString = formDataComponents.joined(separator: "&")
            request.httpBody = formDataString.data(using: .utf8)
            return request
        }

        let attempts: [(URL, String)] = [
            (baseURL, "PUT"),
            (baseURL.appendingPathComponent("api"), "PUT"),
            (baseURL, "POST"),
            (baseURL.appendingPathComponent("api"), "POST")
        ]

        for (base, method) in attempts {
            let request = makeRequest(at: base, method: method)
            do {
                let (data, response) = try await urlSession.data(for: request)
                if let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) {
                    return try jsonDecoder.decode(ProfileUpdateResponse.self, from: data)
                }
            } catch {
                continue
            }
        }

        throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Profile update failed on all attempts"])
    }

    struct PartnerInfo: Codable {
        let linked: Bool
        let partner: Partner?
    }

    struct Partner: Codable {
        let name: String
        let avatar_url: String?
    }

    struct InviteInfo: Codable {
        let inviter_name: String
    }

    func fetchPartnerInfo(accessToken: String) async throws -> PartnerInfo {
        let url = baseURL
            .appendingPathComponent("profile")
            .appendingPathComponent("partner-info")
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
        return try jsonDecoder.decode(PartnerInfo.self, from: data)
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

    // MARK: - Onboarding

    struct OnboardingInfo: Codable {
        let full_name: String
        let partner_display_name: String?
        let onboarding_step: String
        let linked: Bool
    }

    func fetchOnboarding(accessToken: String) async throws -> OnboardingInfo {
        let url = baseURL
            .appendingPathComponent("profile")
            .appendingPathComponent("onboarding")
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        return try jsonDecoder.decode(OnboardingInfo.self, from: data)
    }

    struct UpdateOnboardingRequest: Codable {
        let partner_display_name: String?
        let onboarding_step: String?
    }

    struct SimpleSuccess: Codable { let success: Bool }

    func updateOnboarding(accessToken: String, update: UpdateOnboardingRequest) async throws -> Bool {
        func makeRequest(at base: URL) throws -> URLRequest {
            let url = base
                .appendingPathComponent("profile")
                .appendingPathComponent("onboarding")
            var request = URLRequest(url: url)
            request.httpMethod = "PATCH"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
            request.httpBody = try jsonEncoder.encode(update)
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
        return (try? jsonDecoder.decode(SimpleSuccess.self, from: data).success) ?? true
    }
}

private struct SessionsResponseBody: Codable {
    let sessions: [ChatSessionDTO]
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

extension BackendService {
    func createLinkInvite(accessToken: String) async throws -> URL {
        let url = baseURL
            .appendingPathComponent("link")
            .appendingPathComponent("send-invite")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await urlSession.data(for: request)
        } catch {
            throw error
        }

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
            // Parse ISO8601 or RFC3339 from backend
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            linkedDate = formatter.date(from: iso) ?? ISO8601DateFormatter().date(from: iso)
        }
        return (decoded.linked, decoded.relationship_id, linkedDate)
    }

}

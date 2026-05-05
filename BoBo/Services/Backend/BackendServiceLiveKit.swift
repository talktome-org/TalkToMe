import Foundation

extension BackendService {
    struct LiveKitTokenRequest: Codable {
        let voice_agent: String
        let ghost_name: String?
        let custom_prompt: String?
    }

    struct LiveKitTokenResponse: Codable {
        let url: String
        let token: String
        let room: String
    }

    func fetchLiveKitToken(
        voiceAgent: String,
        ghostName: String?,
        customPrompt: String?,
        accessToken: String
    ) async throws -> LiveKitTokenResponse {
        let url = baseURL
            .appendingPathComponent("livekit")
            .appendingPathComponent("token")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds
        request.httpBody = try jsonEncoder.encode(
            LiveKitTokenRequest(
                voice_agent: voiceAgent,
                ghost_name: ghostName,
                custom_prompt: customPrompt
            )
        )

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response from server"])
        }
        guard (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }

        return try jsonDecoder.decode(LiveKitTokenResponse.self, from: data)
    }
}

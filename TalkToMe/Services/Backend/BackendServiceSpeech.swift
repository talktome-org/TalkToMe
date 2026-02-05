import Foundation

extension BackendService {
    // MARK: - App Voices (curated TalkToMe voices)

    struct AppVoiceDTO: Codable, Equatable, Identifiable {
        var id: String { voice_id }
        let voice_id: String
        let name: String
        let description: String
    }

    struct AppVoicesResponseBody: Codable {
        let voices: [AppVoiceDTO]
        let default_voice_id: String
    }

    func fetchAppVoices(accessToken: String) async throws -> AppVoicesResponseBody {
        let url = baseURL
            .appendingPathComponent("speech")
            .appendingPathComponent("app-voices")
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

        return try jsonDecoder.decode(AppVoicesResponseBody.self, from: data)
    }

    // MARK: - ElevenLabs Full Voice List (legacy)

    struct ElevenVoiceDTO: Codable, Equatable, Identifiable {
        var id: String { voice_id }
        let voice_id: String
        let name: String
        let preview_url: String?
        let category: String?
        let labels: [String: String]?
    }

    struct ElevenVoicesResponseBody: Codable {
        let voices: [ElevenVoiceDTO]
    }

    func fetchElevenLabsVoices(accessToken: String) async throws -> [ElevenVoiceDTO] {
        let url = baseURL
            .appendingPathComponent("speech")
            .appendingPathComponent("voices")
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

        let decoded = try jsonDecoder.decode(ElevenVoicesResponseBody.self, from: data)
        return decoded.voices
    }

    func elevenLabsTTS(
        text: String,
        voiceId: String,
        accessToken: String
    ) async throws -> Data {
        let url = baseURL
            .appendingPathComponent("speech")
            .appendingPathComponent("tts")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = 60

        struct Body: Codable {
            let text: String
            let voice_id: String
            let model_id: String
            let output_format: String
        }
        request.httpBody = try jsonEncoder.encode(
            Body(
                text: text,
                voice_id: voiceId,
                model_id: "eleven_multilingual_v2",
                output_format: "mp3_44100_128"
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
        return data
    }
}


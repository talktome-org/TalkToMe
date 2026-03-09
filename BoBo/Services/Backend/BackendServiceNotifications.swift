import Foundation

extension BackendService {
    func registerPushToken(token: String, platform: String, bundleId: String, accessToken: String) async throws {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("notifications")
            .appendingPathComponent("register"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        struct Body: Codable { let token: String; let platform: String; let bundle_id: String; let timezone: String }
        let tz = TimeZone.current.identifier
        request.httpBody = try jsonEncoder.encode(Body(token: token, platform: platform, bundle_id: bundleId, timezone: tz))
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
}


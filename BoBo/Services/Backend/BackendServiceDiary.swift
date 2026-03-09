import Foundation

extension BackendService {
    struct DiarySettingsResponse: Codable {
        let name: String
        let description: String
        let header_color_hex: String
    }

    struct DiaryEntriesResponse: Codable {
        let entries: [DiaryEntryDTO]
    }

    struct DiaryEntryDTO: Codable {
        let id: UUID
        let user_id: UUID
        let date: String
        let title: String
        let body_blocks: [[String: String]]
        let created_at: String?
        let timezone_abbreviation: String
    }

    struct DiarySaveEntryRequest: Codable {
        let id: UUID
        let date: String
        let title: String
        let body_blocks: [[String: String]]
        let timezone_abbreviation: String
    }

    struct DiarySaveEntryResponse: Codable {
        let id: String
        let success: Bool
    }

    func fetchDiarySettings(accessToken: String) async throws -> (name: String, description: String, headerColorHex: String) {
        var request = URLRequest(url: baseURL.appendingPathComponent("diary").appendingPathComponent("settings"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
        let decoded = try jsonDecoder.decode(DiarySettingsResponse.self, from: data)
        return (decoded.name, decoded.description, decoded.header_color_hex)
    }

    func upsertDiarySettings(name: String, description: String, headerColorHex: String, accessToken: String) async throws {
        var request = URLRequest(url: baseURL.appendingPathComponent("diary").appendingPathComponent("settings"))
        request.httpMethod = "PUT"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds
        request.httpBody = try jsonEncoder.encode([
            "name": name,
            "description": description,
            "header_color_hex": headerColorHex,
        ] as [String: String])

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
    }

    func fetchDiaryEntries(accessToken: String) async throws -> [DiaryEntryDTO] {
        var request = URLRequest(url: baseURL.appendingPathComponent("diary").appendingPathComponent("entries"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
        let decoded = try jsonDecoder.decode(DiaryEntriesResponse.self, from: data)
        return decoded.entries
    }

    func fetchDiaryEntry(entryId: UUID, accessToken: String) async throws -> DiaryEntryDTO? {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("diary")
            .appendingPathComponent("entries")
            .appendingPathComponent(entryId.uuidString))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "Backend", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid response"])
        }
        if http.statusCode == 404 { return nil }
        guard (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: msg])
        }
        return try jsonDecoder.decode(DiaryEntryDTO.self, from: data)
    }

    func saveDiaryEntry(
        id: UUID,
        date: String,
        title: String,
        bodyBlocks: [[String: String]],
        timezoneAbbreviation: String,
        accessToken: String
    ) async throws -> UUID {
        var request = URLRequest(url: baseURL.appendingPathComponent("diary").appendingPathComponent("entries"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds
        let payload: [String: Any] = [
            "id": id.uuidString,
            "date": date,
            "title": title,
            "body_blocks": bodyBlocks,
            "timezone_abbreviation": timezoneAbbreviation,
        ]
        request.httpBody = try JSONSerialization.data(withJSONObject: payload)

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
        let decoded = try jsonDecoder.decode(DiarySaveEntryResponse.self, from: data)
        guard let uuid = UUID(uuidString: decoded.id) else {
            throw NSError(domain: "Backend", code: -2, userInfo: [NSLocalizedDescriptionKey: "Invalid response id"])
        }
        return uuid
    }

    func uploadDiaryPhoto(fileData: Data, filename: String, contentType: String, accessToken: String) async throws -> (path: String, url: String?) {
        let url = baseURL
            .appendingPathComponent("diary")
            .appendingPathComponent("attachments")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        let boundary = "Boundary-\(UUID().uuidString)"
        request.setValue("multipart/form-data; boundary=\(boundary)", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = 60

        var body = Data()
        body.append("--\(boundary)\r\n".data(using: .utf8)!)
        body.append("Content-Disposition: form-data; name=\"file\"; filename=\"\(filename)\"\r\n".data(using: .utf8)!)
        body.append("Content-Type: \(contentType)\r\n\r\n".data(using: .utf8)!)
        body.append(fileData)
        body.append("\r\n--\(boundary)--\r\n".data(using: .utf8)!)
        request.httpBody = body

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
        struct UploadRes: Codable { let path: String?; let url: String? }
        let decoded = try jsonDecoder.decode(UploadRes.self, from: data)
        return (decoded.path ?? "", decoded.url)
    }

    func deleteDiaryEntry(entryId: UUID, accessToken: String) async throws {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("diary")
            .appendingPathComponent("entries")
            .appendingPathComponent(entryId.uuidString))
        request.httpMethod = "DELETE"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let msg = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: msg])
        }
    }
}

import Foundation

extension BackendService {
    struct MyCodeDTO: Codable {
        let code: String
        let expires_at: String
    }

    struct AddByCodeRequestBody: Codable { let code: String }
    struct AddByCodeResponseBody: Codable { let success: Bool; let friend_user_id: UUID }
    struct FriendsListResponseBody: Codable { let friends: [FriendSummary] }

    func fetchMyFriendCode(accessToken: String) async throws -> MyCodeDTO {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("friends")
            .appendingPathComponent("my-code"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        return try jsonDecoder.decode(MyCodeDTO.self, from: data)
    }

    func addFriendByCode(code: String, accessToken: String) async throws -> UUID {
        var request = URLRequest(url: baseURL
            .appendingPathComponent("friends")
            .appendingPathComponent("add-by-code"))
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds
        request.httpBody = try jsonEncoder.encode(AddByCodeRequestBody(code: code))

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        let decoded = try jsonDecoder.decode(AddByCodeResponseBody.self, from: data)
        guard decoded.success else {
            throw NSError(domain: "Backend", code: -3, userInfo: [NSLocalizedDescriptionKey: "Failed to add friend"])
        }
        return decoded.friend_user_id
    }

    func fetchFriends(accessToken: String) async throws -> [FriendSummary] {
        var request = URLRequest(url: baseURL.appendingPathComponent("friends"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(accessToken)", forHTTPHeaderField: "Authorization")
        request.timeoutInterval = BackendService.coreRequestTimeoutSeconds

        let (data, response) = try await urlSession.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            let serverMessage = decodeSimpleDetail(from: data) ?? String(data: data, encoding: .utf8) ?? "Unknown server error"
            throw NSError(domain: "Backend", code: (response as? HTTPURLResponse)?.statusCode ?? -1, userInfo: [NSLocalizedDescriptionKey: serverMessage])
        }
        let decoded = try jsonDecoder.decode(FriendsListResponseBody.self, from: data)
        return decoded.friends
    }
}


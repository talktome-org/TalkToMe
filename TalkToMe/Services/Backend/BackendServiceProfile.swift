import Foundation

extension BackendService {
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

    struct PairedAvatars: Codable {
        struct Entry: Codable { let url: String?; let source: String }
        let me: Entry
        let partner: Entry
    }

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
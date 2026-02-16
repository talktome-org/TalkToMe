import Foundation

struct FriendSummary: Codable, Hashable, Identifiable {
    let id: UUID
    let fullName: String
    let authProvider: String
    let avatarURL: String?
    let voiceName: String?
    let lastSeenAt: String?
    let isOnline: Bool

    enum CodingKeys: String, CodingKey {
        case id = "user_id"
        case fullName = "full_name"
        case authProvider = "auth_provider"
        case avatarURL = "avatar_url"
        case voiceName = "voice_name"
        case lastSeenAt = "last_seen_at"
        case isOnline = "is_online"
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(UUID.self, forKey: .id)
        fullName = try c.decode(String.self, forKey: .fullName)
        authProvider = try c.decode(String.self, forKey: .authProvider)
        avatarURL = try c.decodeIfPresent(String.self, forKey: .avatarURL)
        voiceName = try c.decodeIfPresent(String.self, forKey: .voiceName)
        lastSeenAt = try c.decodeIfPresent(String.self, forKey: .lastSeenAt)
        isOnline = (try? c.decode(Bool.self, forKey: .isOnline)) ?? false
    }

    init(id: UUID, fullName: String, authProvider: String, avatarURL: String?, voiceName: String?, lastSeenAt: String?, isOnline: Bool = false) {
        self.id = id
        self.fullName = fullName
        self.authProvider = authProvider
        self.avatarURL = avatarURL
        self.voiceName = voiceName
        self.lastSeenAt = lastSeenAt
        self.isOnline = isOnline
    }

    var presenceText: String? {
        if isOnline { return "Online" }
        guard let date = parsedLastSeenAt else { return nil }
        let elapsed = Date().timeIntervalSince(date)
        if elapsed < 60 {
            return "Just now"
        }
        if elapsed < 60 * 60 {
            let minutes = max(1, Int(elapsed / 60))
            return "\(minutes)m ago"
        }
        if elapsed < 24 * 60 * 60 {
            let hours = Int(elapsed / 3600)
            return "\(hours)h ago"
        }
        if elapsed < 48 * 60 * 60 {
            return "Yesterday"
        }
        let days = Int(elapsed / 86400)
        return "\(days)d ago"
    }

    private var parsedLastSeenAt: Date? {
        guard let lastSeenAt, !lastSeenAt.isEmpty else { return nil }
        let f1 = ISO8601DateFormatter()
        f1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let d = f1.date(from: lastSeenAt) { return d }
        let f2 = ISO8601DateFormatter()
        f2.formatOptions = [.withInternetDateTime]
        return f2.date(from: lastSeenAt)
    }
}

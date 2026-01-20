import Foundation

struct FriendSummary: Codable, Hashable, Identifiable {
    let id: UUID
    let fullName: String
    let authProvider: String
    let avatarURL: String?

    enum CodingKeys: String, CodingKey {
        case id = "user_id"
        case fullName = "full_name"
        case authProvider = "auth_provider"
        case avatarURL = "avatar_url"
    }
}


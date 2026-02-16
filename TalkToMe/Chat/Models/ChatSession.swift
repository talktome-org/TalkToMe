import Foundation

struct ChatSession: Identifiable, Hashable, Equatable, Codable {
    static let defaultTitle = "New Chat"
    let id: UUID
    var title: String
    var lastUsedISO8601: String?
    var lastMessageContent: String?
    var unreadCount: Int

    enum CodingKeys: String, CodingKey {
        case id
        case title
        case lastUsedISO8601
        case lastMessageContent
        case unreadCount
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decode(UUID.self, forKey: .id)
        let rawTitle = try container.decodeIfPresent(String.self, forKey: .title)
        let trimmed = rawTitle?.trimmingCharacters(in: .whitespacesAndNewlines)
        self.title = (trimmed?.isEmpty == false) ? trimmed! : Self.defaultTitle
        self.lastUsedISO8601 = try container.decodeIfPresent(String.self, forKey: .lastUsedISO8601)
        self.lastMessageContent = try container.decodeIfPresent(String.self, forKey: .lastMessageContent)
        self.unreadCount = try container.decodeIfPresent(Int.self, forKey: .unreadCount) ?? 0
    }

    static func == (lhs: ChatSession, rhs: ChatSession) -> Bool {
        return lhs.id == rhs.id
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
}

extension ChatSession {
    init(id: UUID, title: String?, lastUsedISO8601: String?, lastMessageContent: String? = nil, unreadCount: Int = 0) {
        self.id = id
        let trimmedTitle = title?.trimmingCharacters(in: .whitespacesAndNewlines)
        self.title = (trimmedTitle?.isEmpty == false) ? trimmedTitle! : Self.defaultTitle
        self.lastUsedISO8601 = lastUsedISO8601
        self.lastMessageContent = lastMessageContent
        self.unreadCount = unreadCount
    }
}

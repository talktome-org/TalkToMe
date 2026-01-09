import Foundation

struct ChatSessionsCacheStore {
    private let fileManager: FileManager
    private let cacheDirectory: URL
    private let userId: String?

    init(
        userId: String?,
        fileManager: FileManager = .default,
        cacheDirectory: URL? = nil
    ) {
        self.userId = userId
        self.fileManager = fileManager
        self.cacheDirectory = cacheDirectory ?? fileManager.urls(for: .cachesDirectory, in: .userDomainMask).first!
    }

    private var sessionsURL: URL {
        if let userId = userId {
            return cacheDirectory.appendingPathComponent("chat_sessions_cache_\(userId).json")
        }
        return cacheDirectory.appendingPathComponent("chat_sessions_cache.json")
    }

    private var unreadURL: URL {
        if let userId = userId {
            return cacheDirectory.appendingPathComponent("chat_unread_cache_\(userId).json")
        }
        return cacheDirectory.appendingPathComponent("chat_unread_cache.json")
    }

    func loadSessions() throws -> [ChatSession]? {
        guard fileManager.fileExists(atPath: sessionsURL.path) else { return nil }
        let data = try Data(contentsOf: sessionsURL)
        return try JSONDecoder().decode([ChatSession].self, from: data)
    }

    func saveSessions(_ sessions: [ChatSession]) throws {
        let data = try JSONEncoder().encode(sessions)
        try data.write(to: sessionsURL, options: .atomic)
    }

    private struct UnreadCache: Codable { let unread: [UUID] }

    func loadUnread() throws -> Set<UUID>? {
        guard fileManager.fileExists(atPath: unreadURL.path) else { return nil }
        let data = try Data(contentsOf: unreadURL)
        let decoded = try JSONDecoder().decode(UnreadCache.self, from: data)
        return Set(decoded.unread)
    }

    func saveUnread(_ unread: Set<UUID>) throws {
        let body = UnreadCache(unread: Array(unread))
        let data = try JSONEncoder().encode(body)
        try data.write(to: unreadURL, options: .atomic)
    }
}



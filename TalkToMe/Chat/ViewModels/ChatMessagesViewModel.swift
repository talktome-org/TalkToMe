import Foundation

@MainActor
final class ChatMessagesViewModel: ObservableObject {

    struct MessagesCacheEntry {
        let messages: [ChatMessage]
        let lastLoaded: Date
    }

    @Published var messages: [ChatMessage] = []
    @Published var isLoadingHistory: Bool = false
    @Published var sessionId: UUID?

    private let cacheFreshnessSeconds: TimeInterval = 300
    private static var sharedMessagesCache: [UUID: MessagesCacheEntry] = [:]

    private func debugLog(_ message: @autoclosure () -> String) {
#if DEBUG
        print(message())
#endif
    }

    init(sessionId: UUID? = nil) {
        self.sessionId = sessionId
        if let sid = sessionId {
            if let entry = Self.sharedMessagesCache[sid], !entry.messages.isEmpty {
                self.messages = entry.messages
                self.isLoadingHistory = false
            } else {
                self.messages = []
                self.isLoadingHistory = true
            }
        }
    }

    func updateCacheForCurrentSession(currentMessages: [ChatMessage]) {
        guard let sid = self.sessionId else { return }
        Self.sharedMessagesCache[sid] = MessagesCacheEntry(messages: currentMessages, lastLoaded: Date())
    }

    func getCachedMessages(for sessionId: UUID) -> [ChatMessage]? {
        return Self.sharedMessagesCache[sessionId]?.messages
    }

    func setCachedMessages(_ messages: [ChatMessage], for sessionId: UUID) {
        Self.sharedMessagesCache[sessionId] = MessagesCacheEntry(messages: messages, lastLoaded: Date())
    }

    func isCacheFresh(for sessionId: UUID) -> Bool {
        guard let entry = Self.sharedMessagesCache[sessionId] else { return false }
        return Date().timeIntervalSince(entry.lastLoaded) < cacheFreshnessSeconds
    }

    static func preCachePartnerMessage(sessionId: UUID, text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        var messages = Self.sharedMessagesCache[sessionId]?.messages ?? []
        let partnerMessage = ChatMessage.partnerReceived(trimmed)
        let exists = messages.contains { msg in
            msg.segments.contains { seg in
                if case .partnerReceived(let t) = seg { return t == trimmed } else { return false }
            }
        }
        if !exists {
            messages.append(partnerMessage)
        }
        Self.sharedMessagesCache[sessionId] = MessagesCacheEntry(
            messages: messages,
            lastLoaded: Date()
        )
    }

    func presentSession(_ id: UUID) async {
        self.sessionId = id
        if let entry = Self.sharedMessagesCache[id], !entry.messages.isEmpty {
            self.messages = entry.messages
            self.isLoadingHistory = false
        } else {
            self.messages = []
            self.isLoadingHistory = true
        }

        if isCacheFresh(for: id) {
            self.isLoadingHistory = false
            return
        }

        await loadHistory(force: true)
    }

    func loadHistory(force: Bool = false) async {
        do {
            guard let sid = sessionId else { self.messages = []; self.isLoadingHistory = false; return }

            if !force, let entry = Self.sharedMessagesCache[sid] {
                let age = Date().timeIntervalSince(entry.lastLoaded)
                if age < cacheFreshnessSeconds {
                    self.messages = entry.messages
                    self.isLoadingHistory = false
                    return
                }
            }

            if self.messages.isEmpty { self.isLoadingHistory = true }

            guard let accessToken = await AuthService.shared.getAccessToken() else {
                debugLog("[ChatMessagesVM] ACCESS_TOKEN: <nil>")
                self.isLoadingHistory = false
                return
            }
            let dtos = try await BackendService.shared.fetchMessages(sessionId: sid, accessToken: accessToken)
            guard let userId = AuthService.shared.currentUser?.id else { self.isLoadingHistory = false; return }
            var mapped = dtos.map { ChatMessage(dto: $0, currentUserId: userId) }

            if let optimistic = self.messages.last {
                let optimisticPartnerReceivedText: String? = optimistic.segments.compactMap { seg in
                    if case .partnerReceived(let t) = seg {
                        let trimmed = t.trimmingCharacters(in: .whitespacesAndNewlines)
                        return trimmed.isEmpty ? nil : t
                    }
                    return nil
                }.first

                if let optimisticText = optimisticPartnerReceivedText {
                    let existsInMapped = mapped.contains { msg in
                        msg.segments.contains { seg in
                            if case .partnerReceived(let t) = seg { return t == optimisticText }
                            return false
                        }
                    }
                    if !existsInMapped {
                        mapped.append(optimistic)
                    }
                }
            }
            self.messages = mapped
            Self.sharedMessagesCache[sid] = MessagesCacheEntry(messages: mapped, lastLoaded: Date())

        } catch {
            debugLog("[ChatMessagesVM] Failed to load history: \(error)")
        }
        self.isLoadingHistory = false
    }
}



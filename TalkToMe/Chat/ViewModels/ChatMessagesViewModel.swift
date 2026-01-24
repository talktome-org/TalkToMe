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

    static func clearAllCachedMessages() {
        sharedMessagesCache.removeAll()
    }

    private func debugLog(_ message: @autoclosure () -> String) {
#if DEBUG
        print(message())
#endif
    }

    private func sameMessageIds(_ a: [ChatMessage], _ b: [ChatMessage]) -> Bool {
        if a.count != b.count { return false }
        if a.isEmpty { return true }
        for i in 0..<a.count {
            if a[i].id != b[i].id { return false }
        }
        return true
    }

    private func applyMessages(_ newMessages: [ChatMessage], reason: String) {
        if sameMessageIds(self.messages, newMessages) {
            debugLog("[ChatMessagesVM] applyMessages skip (same ids) reason=\(reason) count=\(newMessages.count) sid=\(sessionId?.uuidString ?? "nil")")
            return
        }
        debugLog("[ChatMessagesVM] applyMessages reason=\(reason) \(self.messages.count)->\(newMessages.count) sid=\(sessionId?.uuidString ?? "nil")")
        self.messages = newMessages
    }

    private func resolvedCurrentUserId() -> UUID? {
        if let uid = AuthService.shared.currentUser?.id { return uid }
        if let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId),
           let uid = UUID(uuidString: raw) {
            return uid
        }
        return nil
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

    private func setCachedMessagesFromLocal(_ messages: [ChatMessage], for sessionId: UUID) {
        let existingLastLoaded = Self.sharedMessagesCache[sessionId]?.lastLoaded ?? Date.distantPast
        Self.sharedMessagesCache[sessionId] = MessagesCacheEntry(messages: messages, lastLoaded: existingLastLoaded)
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
        // IMPORTANT: This is an optimistic in-memory hint; do NOT mark as a "fresh" network load.
        let existingLastLoaded = Self.sharedMessagesCache[sessionId]?.lastLoaded ?? Date.distantPast
        Self.sharedMessagesCache[sessionId] = MessagesCacheEntry(messages: messages, lastLoaded: existingLastLoaded)
    }

    func presentSession(_ id: UUID) async {
        self.sessionId = id
        // Prefer the durable GRDB cache over the in-memory cache to avoid "1 message then full history" flashes.
        // (In-memory cache can be partial/optimistic.)
        self.messages = []
        self.isLoadingHistory = true

        // Load cached messages from GRDB immediately (best-effort).
        if let currentUserId = resolvedCurrentUserId() {
            let local = await ChatStore.shared.loadMessages(sessionId: id, currentUserId: currentUserId)
            if !local.isEmpty {
                applyMessages(local, reason: "presentSession.grdb")
                // IMPORTANT: Do not mark as "fresh" just because we loaded from disk.
                // We still want to refresh from the server (when online) to avoid being stuck with partial history.
                setCachedMessagesFromLocal(local, for: id)
                self.isLoadingHistory = false
            }
        }

        // If GRDB had nothing, fall back to in-memory cache (best-effort).
        if self.messages.isEmpty,
           let entry = Self.sharedMessagesCache[id],
           !entry.messages.isEmpty {
            applyMessages(entry.messages, reason: "presentSession.memoryCacheFallback")
            self.isLoadingHistory = false
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

            if self.messages.isEmpty { self.isLoadingHistory = true }

            // Always try GRDB first so offline navigation shows cached messages immediately.
            if let userId = resolvedCurrentUserId() {
                let local = await ChatStore.shared.loadMessages(sessionId: sid, currentUserId: userId)
                if !local.isEmpty, (force || self.messages.isEmpty) {
                    applyMessages(local, reason: "loadHistory.grdb")
                    // Don't treat local disk reads as a "fresh" cache for server fetch purposes.
                    setCachedMessagesFromLocal(local, for: sid)
                    self.isLoadingHistory = false
                }
            }

            // If we're offline, stop here (local cache is the best we can do).
            if NetworkMonitor.shared.isOnline == false {
                self.isLoadingHistory = false
                return
            }

            // If we recently loaded (either from GRDB or from the network), don't refetch.
            if !force, let entry = Self.sharedMessagesCache[sid] {
                let age = Date().timeIntervalSince(entry.lastLoaded)
                if age < cacheFreshnessSeconds {
                    self.messages = entry.messages
                    self.isLoadingHistory = false
                    return
                }
            }

            guard let accessToken = await AuthService.shared.getAccessToken() else {
                debugLog("[ChatMessagesVM] ACCESS_TOKEN: <nil>")
                self.isLoadingHistory = false
                return
            }
            let dtos = try await BackendService.shared.fetchMessages(sessionId: sid, accessToken: accessToken)
            guard let userId = resolvedCurrentUserId() else { self.isLoadingHistory = false; return }
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
            applyMessages(mapped, reason: "loadHistory.network")
            Self.sharedMessagesCache[sid] = MessagesCacheEntry(messages: mapped, lastLoaded: Date())
            Task.detached {
                await ChatStore.shared.upsertMessages(dtos)
            }

        } catch {
            debugLog("[ChatMessagesVM] Failed to load history: \(error)")
        }
        self.isLoadingHistory = false
    }
}



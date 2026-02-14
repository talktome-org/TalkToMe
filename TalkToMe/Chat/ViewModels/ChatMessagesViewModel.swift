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

    static var sharedMessagesCache: [UUID: MessagesCacheEntry] = [:]
    private let cacheFreshnessSeconds: TimeInterval = 300

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


    func presentSession(_ id: UUID) async {
        self.sessionId = id
        self.messages = []
        self.isLoadingHistory = true

        if let currentUserId = resolvedCurrentUserId() {
            let local = await ChatStore.shared.loadMessages(sessionId: id, currentUserId: currentUserId)
            if !local.isEmpty {
                applyMessages(local)
                setCachedMessagesFromLocal(local, for: id)
                self.isLoadingHistory = false
            }
        }

        if self.messages.isEmpty, let entry = Self.sharedMessagesCache[id], !entry.messages.isEmpty {
            applyMessages(entry.messages)
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

            if let userId = resolvedCurrentUserId() {
                let local = await ChatStore.shared.loadMessages(sessionId: sid, currentUserId: userId)
                if !local.isEmpty, (force || self.messages.isEmpty) {
                    applyMessages(local)
                    setCachedMessagesFromLocal(local, for: sid)
                    self.isLoadingHistory = false
                }
            }

            if NetworkMonitor.shared.isOnline == false {
                self.isLoadingHistory = false
                return
            }

            if !force, let entry = Self.sharedMessagesCache[sid] {
                let age = Date().timeIntervalSince(entry.lastLoaded)
                if age < cacheFreshnessSeconds {
                    self.messages = entry.messages
                    self.isLoadingHistory = false
                    return
                }
            }

            guard let accessToken = await AuthService.shared.getAccessToken() else {
                self.isLoadingHistory = false
                return
            }
            let dtos = try await BackendService.shared.fetchMessages(sessionId: sid, accessToken: accessToken)
            await ChatStore.shared.reconcileMessagesWithServer(dtos, sessionId: sid)
            guard let userId = resolvedCurrentUserId() else { self.isLoadingHistory = false; return }
            var mapped = dtos.map { ChatMessage(dto: $0, currentUserId: userId) }

            // Merge local-only fields (thinking_summary) that aren't in server DTOs
            let localMetadata = await ChatStore.shared.loadLocalMetadata(sessionId: sid)
            for i in mapped.indices {
                if let meta = localMetadata[mapped[i].id.uuidString] {
                    mapped[i].thinkingSummary = meta.thinkingSummary
                }
            }

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
            applyMessages(mapped)
            Self.sharedMessagesCache[sid] = MessagesCacheEntry(messages: mapped, lastLoaded: Date())

        } catch { }
        self.isLoadingHistory = false
    }


    private func sameMessageIds(_ a: [ChatMessage], _ b: [ChatMessage]) -> Bool {
        if a.count != b.count { return false }
        if a.isEmpty { return true }
        for i in 0..<a.count {
            if a[i].id != b[i].id { return false }
        }
        return true
    }

    private func applyMessages(_ newMessages: [ChatMessage]) {
        guard !sameMessageIds(self.messages, newMessages) else { return }
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

    private func setCachedMessagesFromLocal(_ messages: [ChatMessage], for sessionId: UUID) {
        let existingLastLoaded = Self.sharedMessagesCache[sessionId]?.lastLoaded ?? Date.distantPast
        Self.sharedMessagesCache[sessionId] = MessagesCacheEntry(messages: messages, lastLoaded: existingLastLoaded)
    }
}

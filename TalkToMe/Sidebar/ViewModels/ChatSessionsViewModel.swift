import Foundation
import SwiftUI

@MainActor
final class ChatSessionsViewModel: ObservableObject {

    @Published var sessions: [ChatSession] = []
    @Published var isLoadingSessions: Bool = false
    @Published var sessionsLoadError: String? = nil
    @Published var activeSessionId: UUID? = nil
    @Published var chatViewKey: UUID = UUID()
    @Published var myAvatarURL: String? = {
        let cached = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return (cached?.isEmpty ?? true) ? nil : cached
    }()
    @Published var isBootstrapping: Bool = false
    @Published var lastSessionsSyncSucceeded: Bool? = nil
    @Published var lastSessionsSyncAt: Date? = nil
    @Published var shouldStartInVoiceMode: Bool = false

    private var notificationTokens: [NSObjectProtocol] = []
    private var observedUserId: String? = nil
    private var generationToken: UUID = UUID()
    private var bootstrapTask: Task<Void, Never>? = nil


    func startNewChat() {
        activeSessionId = nil
        chatViewKey = UUID()
    }

    func openSession(_ id: UUID) {
        activeSessionId = id
        chatViewKey = UUID()
        // Clear local unread count immediately for responsive UI
        if let idx = sessions.firstIndex(where: { $0.id == id }), sessions[idx].unreadCount > 0 {
            sessions[idx].unreadCount = 0
        }
        // Tell the server
        Task {
            guard let token = await AuthService.shared.getAccessToken() else { return }
            try? await BackendService.shared.markSessionRead(sessionId: id, accessToken: token)
        }
    }

    func resetForLogout() {
        bootstrapTask?.cancel()
        bootstrapTask = nil
        generationToken = UUID()

        stopObserving()
        sessions = []
        activeSessionId = nil
        chatViewKey = UUID()
        sessionsLoadError = nil
        isLoadingSessions = false
        myAvatarURL = nil
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.myAvatarURL)
        ChatMessagesViewModel.sharedMessagesCache.removeAll()
        observedUserId = nil
    }

    /// Resets volatile in-memory state when the authenticated user changes (account switch).
    /// This avoids briefly showing the previous user's sessions/messages while the app reboots data for the new account.
    func resetForAccountSwitch() {
        bootstrapTask?.cancel()
        bootstrapTask = nil
        generationToken = UUID()

        stopObserving()
        sessions = []
        activeSessionId = nil
        chatViewKey = UUID()
        sessionsLoadError = nil
        isLoadingSessions = false
        isBootstrapping = false
        lastSessionsSyncSucceeded = nil
        lastSessionsSyncAt = nil
        myAvatarURL = nil
        UserDefaults.standard.removeObject(forKey: PreferenceKeys.myAvatarURL)
        ChatMessagesViewModel.sharedMessagesCache.removeAll()
        observedUserId = nil
    }

    func stopObserving() {
        if notificationTokens.isEmpty { return }
        let nc = NotificationCenter.default
        for t in notificationTokens {
            nc.removeObserver(t)
        }
        notificationTokens.removeAll()
    }

    func preloadCachedSessionsIfNeeded() async {
        if !sessions.isEmpty { return }
        let tokenAtStart = generationToken
        let userAtStart = effectiveUserIdKey()
        let local = await ChatStore.shared.loadSessions()
        if tokenAtStart != generationToken { return }
        if userAtStart != effectiveUserIdKey() { return }
        guard !local.isEmpty else { return }
        sessions = local
    }

    func startObserving() {
        let currentKey = effectiveUserIdKey()

        // If we were already observing, only keep it if it's still the same account.
        // Otherwise, tear down and rebuild observers for the new user.
        if !notificationTokens.isEmpty {
            if observedUserId == currentKey { return }
            resetForAccountSwitch()
        }

        if observedUserId != nil, observedUserId != currentKey {
            resetForAccountSwitch()
        }
        observedUserId = currentKey

        hydrateMyAvatarURLFromAuthIfPossible()

        // Warm from local cache immediately so the sidebar avatar can show on first paint.
        if let cached = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL),
           !cached.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            myAvatarURL = cached
        }

        let nc = NotificationCenter.default
        notificationTokens.append(
            nc.addObserver(forName: .chatSessionsNeedRefresh, object: nil, queue: .main) { [weak self] _ in
                Task { @MainActor in
                    await self?.refreshSessions()
                }
            }
        )
        notificationTokens.append(
            nc.addObserver(forName: .chatSessionCreated, object: nil, queue: .main) { [weak self] note in
                MainActor.assumeIsolated {
                    self?.handleChatSessionCreated(note)
                }
            }
        )
        notificationTokens.append(
            nc.addObserver(forName: .chatSessionRekeyed, object: nil, queue: .main) { [weak self] note in
                MainActor.assumeIsolated {
                    self?.handleChatSessionRekeyed(note)
                }
            }
        )
        notificationTokens.append(
            nc.addObserver(forName: .chatMessageSent, object: nil, queue: .main) { [weak self] note in
                MainActor.assumeIsolated {
                    self?.handleChatMessageSent(note)
                }
            }
        )
        notificationTokens.append(
            nc.addObserver(forName: .partnerMessageReceived, object: nil, queue: .main) { [weak self] _ in
                Task { @MainActor in
                    await self?.refreshSessions()
                }
            }
        )
        notificationTokens.append(
            nc.addObserver(forName: .partnerMessageOpen, object: nil, queue: .main) { [weak self] note in
                MainActor.assumeIsolated {
                    guard let sid = note.userInfo?["sessionId"] as? UUID else { return }
                    self?.openSession(sid)
                    NotificationCenter.default.post(name: .openChatSession, object: nil, userInfo: ["sessionId": sid])
                }
            }
        )

        bootstrapTask?.cancel()
        let tokenAtStart = generationToken
        let userAtStart = observedUserId
        bootstrapTask = Task { [weak self] in
            guard let self else { return }
            if Task.isCancelled { return }
            await MainActor.run { self.isBootstrapping = true }
            await withTaskGroup(of: Void.self) { group in
                group.addTask { [weak self] in
                    await self?.refreshSessions()
                }
                group.addTask { [weak self] in
                    await self?.refreshMyAvatarURL()
                }
            }
            if Task.isCancelled { return }
            let stillSame =
                (tokenAtStart == self.generationToken) &&
                (userAtStart == self.observedUserId) &&
                (userAtStart == self.effectiveUserIdKey())
            if stillSame {
                await MainActor.run { self.isBootstrapping = false }
            }
        }
    }

    func refreshSessions() async {
        if isLoadingSessions { return }
        await loadSessions()
    }

    func refreshMyAvatarURL() async {
        hydrateMyAvatarURLFromAuthIfPossible()

        guard let accessToken = await AuthService.shared.getAccessToken() else { return }
        do {
            let oldURL = myAvatarURL?.trimmingCharacters(in: .whitespacesAndNewlines)
            let res = try await BackendService.shared.fetchPairedAvatars(accessToken: accessToken)
            let raw = res.me.url?.trimmingCharacters(in: .whitespacesAndNewlines)
            guard let url = raw, !url.isEmpty else { return }

            if url.hasPrefix("http://") || url.hasPrefix("https://") {
                UserDefaults.standard.set(url, forKey: PreferenceKeys.myAvatarURL)
            }

            // If the signed URL changed but the underlying image is the same,
            // carry the cached image over to the new URL so the avatar doesn't flash.
            if let oldURL, oldURL != url,
               let existingImage = AvatarCacheManager.shared.getImageIfCached(urlString: oldURL) {
                await AvatarCacheManager.shared.cacheImageImmediately(urlString: url, image: existingImage)
            }

            myAvatarURL = url
            // Only post avatarChanged when the user actually uploaded a new avatar,
            // not when the signed URL simply rotated.
        } catch { }
    }

    func loadSessions() async {
        do {
            let tokenAtStart = generationToken
            let userAtStart = observedUserId
            sessionsLoadError = nil
            isLoadingSessions = true
            lastSessionsSyncAt = Date()

            let local = await ChatStore.shared.loadSessions()
            if tokenAtStart != generationToken { return }
            if userAtStart != observedUserId { return }
            if sessions.isEmpty, !local.isEmpty {
                sessions = local
            }

            guard let accessToken = await AuthService.shared.getAccessToken() else {
                isLoadingSessions = false
                lastSessionsSyncSucceeded = false
                return
            }

            let dtos = try await BackendService.shared.fetchSessions(accessToken: accessToken)
            let mapped = dtos.map { dto in
                ChatSession(
                    id: dto.id,
                    title: dto.title ?? ChatSession.defaultTitle,
                    lastUsedISO8601: dto.last_message_at,
                    lastMessageContent: dto.last_message_content,
                    unreadCount: dto.unread_count ?? 0
                )
            }
            if tokenAtStart != generationToken { return }
            if userAtStart != observedUserId { return }
            if userAtStart != effectiveUserIdKey() { return }

            sessions = mapped
            isLoadingSessions = false
            lastSessionsSyncSucceeded = true
            await ChatStore.shared.reconcileSessionsWithServer(mapped)
        } catch is CancellationError {
            isLoadingSessions = false
        } catch {
            let ns = error as NSError
            if ns.domain == NSURLErrorDomain, ns.code == NSURLErrorCancelled {
                isLoadingSessions = false
                return
            }
            sessionsLoadError = error.localizedDescription
            isLoadingSessions = false
            lastSessionsSyncAt = Date()
            lastSessionsSyncSucceeded = false
        }
    }

    func renameSession(_ id: UUID, to title: String) async {
        do {
            guard let accessToken = await AuthService.shared.getAccessToken() else { return }
            try await BackendService.shared.renameSession(sessionId: id, title: title, accessToken: accessToken)
            await refreshSessions()
        } catch {
            sessionsLoadError = error.localizedDescription
        }
    }

    func deleteSession(_ id: UUID) async {
        do {
            guard let accessToken = await AuthService.shared.getAccessToken() else { return }
            try await BackendService.shared.deleteSession(sessionId: id, accessToken: accessToken)
            await refreshSessions()
        } catch {
            sessionsLoadError = error.localizedDescription
        }
    }

    func ensureProfilePictureCached() async {
        hydrateMyAvatarURLFromAuthIfPossible()
        if let cached = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL),
           (myAvatarURL?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ?? true) {
            myAvatarURL = cached
        }

        if let url = myAvatarURL?.trimmingCharacters(in: .whitespacesAndNewlines),
           !url.isEmpty,
           (url.hasPrefix("http://") || url.hasPrefix("https://")) {
            _ = await AvatarCacheManager.shared.getCachedImage(urlString: url)
        }
    }

    func formatLastUsed(_ iso: String?) -> String {
        guard let raw = iso?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else { return "" }

        let iso1 = ISO8601DateFormatter()
        iso1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let iso2 = ISO8601DateFormatter()
        iso2.formatOptions = [.withInternetDateTime]

        let parsed = iso1.date(from: raw) ?? iso2.date(from: raw)
        guard let date = parsed else { return "" }

        let interval = Date().timeIntervalSince(date)
        let minutes = Int(interval / 60)
        if minutes < 1 { return "now" }
        if minutes < 60 { return "\(minutes)m" }
        let hours = minutes / 60
        if hours < 24 { return "\(hours)h" }
        let days = hours / 24
        if days == 1 { return "Yesterday" }
        if days < 7 { return "\(days)d" }

        let out = DateFormatter()
        out.locale = Locale.current
        out.dateFormat = "MMM d"
        return out.string(from: date)
    }

    private func effectiveUserIdKey() -> String? {
        let raw: String? =
            AuthService.shared.currentUser?.id.uuidString
            ?? UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)
        let trimmed = raw?.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func extractAvatarURLFromAuth() -> String? {
        guard let user = AuthService.shared.currentUser else { return nil }

        let candidateKeys = ["avatar_url", "picture", "photo_url", "profile_image_url"]
        for key in candidateKeys {
            if let v = user.userMetadata[key]?.stringValue?.trimmingCharacters(in: .whitespacesAndNewlines),
               !v.isEmpty {
                return v
            }
        }
        return nil
    }

    private func hydrateMyAvatarURLFromAuthIfPossible() {
        guard (myAvatarURL?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ?? true) else { return }
        guard let url = Self.extractAvatarURLFromAuth(),
              !url.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }

        myAvatarURL = url
        if url.hasPrefix("http://") || url.hasPrefix("https://") {
            UserDefaults.standard.set(url, forKey: PreferenceKeys.myAvatarURL)
        }
    }

    private func handleChatSessionCreated(_ note: Notification) {
        if let sid = note.userInfo?["sessionId"] as? UUID {
            if !self.sessions.contains(where: { $0.id == sid }) {
                let rawTitle = (note.userInfo?["title"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
                let session = ChatSession(
                    id: sid,
                    title: rawTitle,
                    lastUsedISO8601: note.userInfo?["lastUsedISO8601"] as? String,
                    lastMessageContent: note.userInfo?["lastMessageContent"] as? String
                )
                self.sessions.insert(session, at: 0)
            }
        }
    }

    private func handleChatSessionRekeyed(_ note: Notification) {
        guard
            let oldId = note.userInfo?["oldSessionId"] as? UUID,
            let newId = note.userInfo?["newSessionId"] as? UUID
        else { return }

        let oldSession = self.sessions.first(where: { $0.id == oldId })
        let serverSession = self.sessions.first(where: { $0.id == newId })

        self.sessions.removeAll(where: { $0.id == newId })

        if let idx = self.sessions.firstIndex(where: { $0.id == oldId }) {
            let base = oldSession ?? self.sessions[idx]

            let mergedTitle: String = {
                if let serverSession, serverSession.title != ChatSession.defaultTitle {
                    return serverSession.title
                }
                return base.title
            }()

            let mergedLastUsed = serverSession?.lastUsedISO8601 ?? base.lastUsedISO8601
            let mergedLastMessage = serverSession?.lastMessageContent ?? base.lastMessageContent

            self.sessions[idx] = ChatSession(
                id: newId,
                title: mergedTitle,
                lastUsedISO8601: mergedLastUsed,
                lastMessageContent: mergedLastMessage
            )
        } else if let serverSession {
            if !self.sessions.contains(where: { $0.id == newId }) {
                self.sessions.insert(serverSession, at: 0)
            }
        }

        if self.activeSessionId == oldId {
            self.activeSessionId = newId
            self.chatViewKey = UUID()
        }
    }

    private func handleChatMessageSent(_ note: Notification) {
        if let sid = note.userInfo?["sessionId"] as? UUID,
           let messageContent = note.userInfo?["messageContent"] as? String,
           let idx = self.sessions.firstIndex(where: { $0.id == sid }) {
            var item = self.sessions.remove(at: idx)
            item.lastMessageContent = messageContent
            self.sessions.insert(item, at: 0)
        }
    }
}
import SwiftUI

@MainActor
class ChatSessionsViewModel: ObservableObject {
    @Published var sessions: [ChatSession] = []
    @Published var isLoadingSessions: Bool = false
    @Published var sessionsLoadError: String? = nil
    @Published var pendingRequests: [BackendService.PartnerPendingRequest] = []
    @Published var activeSessionId: UUID? = nil {
        didSet {
            if let id = activeSessionId {
                if unreadPartnerSessionIds.remove(id) != nil {
                    print("[SessionsVM] Cleared unread on active change for session=\(id)")
                }
            }
        }
    }
    @Published var chatViewKey: UUID = UUID()
    @Published var myAvatarURL: String? = nil
    @Published var partnerAvatarURL: String? = nil
    @Published var partnerInfo: BackendService.PartnerInfo? = nil
    @Published var isBootstrapping: Bool = false
    @Published var isBootstrapComplete: Bool = false
    @Published var unreadPartnerSessionIds: Set<UUID> = []
    @Published var lastSessionsSyncSucceeded: Bool? = nil
    @Published var lastSessionsSyncAt: Date? = nil
    @Published var lastPendingRequestsSyncSucceeded: Bool? = nil
    @Published var lastPendingRequestsSyncAt: Date? = nil

    var suppressUnreadSessionIds: Set<UUID> = []
    var handlingPartnerRequestIds: Set<UUID> = []
    private var hasStartedObserving: Bool = false
    private let linkStatusPoller = PartnerLinkStatusPoller()
    private let eventRouter = ChatSessionsEventRouter()
    let avatarCacheManager = AvatarCacheManager.shared
    private weak var navigationViewModel: SidebarNavigationViewModel?
    private weak var linkViewModel: LinkViewModel?
    weak var chatViewModel: ChatViewModel?
    private var currentUserId: String?
    private var pendingAcceptancePreviewBySession: [UUID: String] = [:]

    func findNavigationViewModel() -> SidebarNavigationViewModel? {
        return navigationViewModel
    }

    func setNavigationViewModel(_ navVM: SidebarNavigationViewModel) {
        self.navigationViewModel = navVM
    }

    @MainActor
    private func findLinkViewModel() -> LinkViewModel? {
        return linkViewModel
    }

    func setLinkViewModel(_ linkVM: LinkViewModel) {
        self.linkViewModel = linkVM
    }

    @MainActor
    func storePendingAcceptance(sessionId: UUID, text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        pendingAcceptancePreviewBySession[sessionId] = trimmed
    }

    @MainActor
    func getPendingAcceptancePreview(for sessionId: UUID) -> String? {
        return pendingAcceptancePreviewBySession[sessionId]
    }

    @MainActor
    func consumePendingAcceptancePreview(for sessionId: UUID) -> String? {
        let val = pendingAcceptancePreviewBySession.removeValue(forKey: sessionId)
        return val
    }

    init() {
        if let storedMyAvatar = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL),
           !storedMyAvatar.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            self.myAvatarURL = storedMyAvatar
            Task { @MainActor in
                _ = avatarCacheManager.getImageIfCached(urlString: storedMyAvatar)
            }
        }
    }

    func preloadCachedSessionsIfNeeded() async {
        if !self.sessions.isEmpty { return }
        let local = await ChatStore.shared.loadSessions()
        guard !local.isEmpty else { return }
        await MainActor.run {
            self.sessions = local
        }
    }

    deinit {
        // `deinit` is nonisolated; schedule MainActor cleanup for actor-isolated helpers.
        Task { @MainActor [eventRouter, linkStatusPoller] in
            eventRouter.stop()
            linkStatusPoller.stop()
        }
    }

    func startNewChat() {
        activeSessionId = nil
        chatViewKey = UUID()
    }

    func resetForLogout() {
        hasStartedObserving = false
        isBootstrapComplete = false
        isBootstrapping = false
        linkStatusPoller.stop()
        eventRouter.stop()
        sessions = []
        pendingRequests = []
        activeSessionId = nil
        myAvatarURL = nil
        partnerAvatarURL = nil
        partnerInfo = nil
        unreadPartnerSessionIds.removeAll()
        suppressUnreadSessionIds.removeAll()
        handlingPartnerRequestIds.removeAll()
    }

    @MainActor
    func openSession(_ id: UUID) {
        let wasUnread = unreadPartnerSessionIds.contains(id)
        activeSessionId = id

        if wasUnread, let preview = sessions.first(where: { $0.id == id })?.lastMessageContent {
            let trimmed = preview.trimmingCharacters(in: .whitespacesAndNewlines)
            if !trimmed.isEmpty {
                ChatMessagesViewModel.preCachePartnerMessage(sessionId: id, text: trimmed)
            }
        }

        chatViewKey = UUID()

        if unreadPartnerSessionIds.remove(id) != nil {
            print("[SessionsVM] openSession cleared unread for session=\(id)")
        }
    }

    func openPendingRequest(_ request: BackendService.PartnerPendingRequest) {
        if let sid = request.recipient_session_id {
            activeSessionId = sid
            chatViewKey = UUID()
            Task { @MainActor in
                ChatMessagesViewModel.preCachePartnerMessage(sessionId: sid, text: request.content)
            }
            Task { @MainActor [weak self] in
                await self?.chatViewModel?.loadHistory(force: true)
            }
        }
        Task { await acceptPendingRequest(request) }
    }

    func formatLastUsed(_ iso: String?) -> String {
        guard let raw = iso?.trimmingCharacters(in: .whitespacesAndNewlines), !raw.isEmpty else { return "" }

        let iso1 = ISO8601DateFormatter()
        iso1.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let iso2 = ISO8601DateFormatter()
        iso2.formatOptions = [.withInternetDateTime]

        let parsed = iso1.date(from: raw) ?? iso2.date(from: raw)
        guard let date = parsed else { return "" }

        let out = DateFormatter()
        out.locale = Locale.current
        out.dateFormat = "dd.MM.yyyy"
        return out.string(from: date)
    }

    func loadSessions(ensurePartnerInfo: Bool = true) async {
        print("🔄 Loading sessions from backend...")
        do {
            await MainActor.run {
                self.sessionsLoadError = nil
                self.isLoadingSessions = true
            }

            // Load cached sessions first (best-effort) so sidebar is instant.
            let local = await ChatStore.shared.loadSessions()
            if !local.isEmpty {
                await MainActor.run {
                    if self.sessions.isEmpty {
                        self.sessions = local
                    }
                }
            }

            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            self.currentUserId = session.user.id.uuidString
            let dtos = try await BackendService.shared.fetchSessions(accessToken: accessToken)
            var mapped = dtos.map { dto in
                return ChatSession(
                    id: dto.id,
                    title: dto.title,
                    lastUsedISO8601: dto.last_message_at,
                    lastMessageContent: dto.last_message_content
                )
            }

            if !self.pendingRequests.isEmpty {
                let hiddenIds = Set(self.pendingRequests.compactMap { $0.recipient_session_id })
                if !hiddenIds.isEmpty {
                    mapped.removeAll { hiddenIds.contains($0.id) }
                }
            }

            if ensurePartnerInfo, self.partnerInfo == nil {
                await loadPartnerInfo()
            }

            if self.partnerInfo?.linked == true {
                let previousSessions = self.sessions

                for session in mapped {
                    if let lastMessage = session.lastMessageContent, !lastMessage.isEmpty {
                        let previousSession = previousSessions.first { $0.id == session.id }
                        let isNewOrChanged = previousSession == nil || previousSession?.lastMessageContent != lastMessage
                        if isNewOrChanged &&
                           session.id != self.activeSessionId &&
                           !self.suppressUnreadSessionIds.contains(session.id) &&
                           !self.unreadPartnerSessionIds.contains(session.id) {
                            self.unreadPartnerSessionIds.insert(session.id)
                            print("[SessionsVM] ✅ Detected new/changed message in session \(session.id) - was: \(previousSession?.lastMessageContent ?? "nil"), now: \(lastMessage)")
                        }
                    }
                }
            }

            let finalMapped = mapped
            await MainActor.run {
                self.sessions = finalMapped
                self.isLoadingSessions = false
                self.lastSessionsSyncSucceeded = true
                self.lastSessionsSyncAt = Date()
                print("📱 Updated local sessions list with \(finalMapped.count) sessions")
            }
            Task.detached {
                await ChatStore.shared.reconcileSessionsWithServer(finalMapped)
            }
        } catch {
            if let nsError = error as NSError?, nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled {
                print("⏭️ Load sessions cancelled (expected during rapid refresh) — ignoring")
                await MainActor.run { self.isLoadingSessions = false }
                return
            }
            print("❌ Failed to load sessions: \(error)")
            await MainActor.run {
                self.isLoadingSessions = false
                self.lastSessionsSyncSucceeded = false
                self.lastSessionsSyncAt = Date()
                // If we already have cached sessions, stay silent (don't show an error banner).
                self.sessionsLoadError = self.sessions.isEmpty
                    ? "Couldn’t load conversations. Check your connection and pull to refresh."
                    : nil
            }
        }
    }

    func refreshSessions() async {
        await loadSessions()
    }

    func renameSession(_ id: UUID, to newTitle: String?) async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            try await BackendService.shared.renameSession(sessionId: id, title: newTitle, accessToken: accessToken)
            let persistedTitle: String = {
                let trimmed = newTitle?.trimmingCharacters(in: .whitespacesAndNewlines)
                return (trimmed?.isEmpty == false) ? trimmed! : ChatSession.defaultTitle
            }()
            await MainActor.run {
                if let idx = self.sessions.firstIndex(where: { $0.id == id }) {
                    var updated = self.sessions[idx]
                    updated.title = persistedTitle
                    self.sessions[idx] = updated
                }
            }
            Task.detached {
                await ChatStore.shared.upsertSessions([
                    ChatSession(
                        id: id,
                        title: persistedTitle,
                        lastUsedISO8601: nil,
                        lastMessageContent: nil
                    )
                ])
            }
        } catch {
            print("Failed to rename session: \(error)")
        }
    }

    func deleteSession(_ id: UUID) async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            try await BackendService.shared.deleteSession(sessionId: id, accessToken: accessToken)
            Task.detached {
                await ChatStore.shared.deleteSessionLocal(sessionId: id)
            }
            await MainActor.run {
                self.sessions.removeAll { $0.id == id }
                if self.activeSessionId == id { self.activeSessionId = nil }
                NotificationCenter.default.post(name: .relationshipTotalsChanged, object: nil)
            }
        } catch {
            print("Failed to delete session: \(error)")
        }
    }

    func loadPendingRequests() async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            let response = try await BackendService.shared.getPartnerPendingRequests(accessToken: accessToken)
            await MainActor.run {
                self.pendingRequests = response.requests
                self.lastPendingRequestsSyncSucceeded = true
                self.lastPendingRequestsSyncAt = Date()
            }
        } catch {
            print("Failed to load pending requests: \(error)")
            await MainActor.run {
                self.lastPendingRequestsSyncSucceeded = false
                self.lastPendingRequestsSyncAt = Date()
            }
        }
    }

    func startObserving() {
        if hasStartedObserving {
            print("[SessionsVM] startObserving called but already observing")
            return
        }
        print("[SessionsVM] Starting observation...")
        hasStartedObserving = true
        activeSessionId = nil
        chatViewKey = UUID()

        Task {
            if let session = try? await AuthService.shared.client.auth.session {
                self.currentUserId = session.user.id.uuidString
            }
            await bootstrapInitialData()
            print("[SessionsVM] Initial data loaded. PartnerLinked=\(self.partnerInfo?.linked ?? false)")
        }
        eventRouter.start(self)

        maybeStartLinkStatusPolling()
    }

    func bootstrapInitialData() async {
        if isBootstrapComplete { return }
        await MainActor.run { self.isBootstrapping = true }

        // Phase 1 (fast): get core data in place so the UI can update quickly.
        await AppSyncGate.shared.setSyncing(true)
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.loadSessions(ensurePartnerInfo: false) }
            group.addTask { await self.loadPendingRequests() }
            group.addTask { await self.loadPartnerInfo(prefetchAvatars: false) }
        }
        await AppSyncGate.shared.setSyncing(false)

        await MainActor.run {
            self.isBootstrapping = false
            self.isBootstrapComplete = true
        }

        Task { @MainActor [weak self] in
            guard let self else { return }
            await self.fetchAndCacheProfileName()
            await self.loadPairedAvatars()
            await self.preloadAvatars()
            await self.ensureProfilePictureCached()
            if let cachedPartnerURL = UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL),
               !cachedPartnerURL.isEmpty,
               (self.partnerAvatarURL == nil || self.partnerAvatarURL?.isEmpty == true) {
                await self.avatarCacheManager.preloadAvatars(urls: [cachedPartnerURL])
            }
        }
    }

    private func fetchAndCacheProfileName() async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let token = session.accessToken
            let profile = try await BackendService.shared.fetchProfileInfo(accessToken: token)
            await MainActor.run {
                UserDefaults.standard.set(profile.full_name, forKey: "talktome_profile_full_name")
                NotificationCenter.default.post(name: .profileChanged, object: nil)
            }
        } catch {
            print("Failed to fetch profile name during bootstrap: \(error)")
        }
    }

    func loadPartnerInfo(prefetchAvatars: Bool = true) async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            let res = try await BackendService.shared.fetchPartnerInfo(accessToken: accessToken)
            let wasLinked = self.partnerInfo?.linked ?? false
            await MainActor.run {
                self.partnerInfo = res
                UserDefaults.standard.set(res.linked, forKey: PreferenceKeys.partnerConnected)
                if res.linked, let partner = res.partner {
                    UserDefaults.standard.set(partner.name, forKey: PreferenceKeys.partnerName)
                    if let avatar = partner.avatar_url {
                        UserDefaults.standard.set(avatar, forKey: PreferenceKeys.partnerAvatarURL)
                    }
                } else {
                    UserDefaults.standard.removeObject(forKey: PreferenceKeys.partnerName)
                    UserDefaults.standard.removeObject(forKey: PreferenceKeys.partnerAvatarURL)
                }
                if res.linked, let linkVM = self.findLinkViewModel() {
                    Task {
                        try? await linkVM.refreshStatus()
                    }
                }
                if (!res.linked) && wasLinked, let linkVM = self.findLinkViewModel() {
                    Task {
                        try? await linkVM.refreshStatus()
                        await linkVM.ensureInviteReady()
                    }
                }

            }
            if prefetchAvatars, res.linked, let url = res.partner?.avatar_url, !url.isEmpty {
                await avatarCacheManager.preloadAvatars(urls: [url])
            }
            if res.linked {
                maybeStartUnlinkStatusPolling()
            } else {
                maybeStartLinkStatusPolling()
            }
        } catch {
            print("Failed to load partner info: \(error)")
        }
    }

    private func acceptPendingRequest(_ request: BackendService.PartnerPendingRequest) async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken

            if let currentUserId = AuthService.shared.currentUser?.id,
               request.sender_user_id != currentUserId {
                let partnerSessionId = try await BackendService.shared.acceptPartnerRequest(requestId: request.id, accessToken: accessToken)

                await MainActor.run {
                    self.pendingRequests.removeAll { $0.id == request.id }
                    ChatMessagesViewModel.preCachePartnerMessage(sessionId: partnerSessionId, text: request.content)
                    self.activeSessionId = partnerSessionId
                    self.chatViewKey = UUID()
                    NotificationCenter.default.post(name: .relationshipTotalsChanged, object: nil)
                }

                Task.detached { [weak self] in
                    try? await Task.sleep(nanoseconds: 300_000_000)
                    await self?.loadSessions()
                    await self?.loadPendingRequests()
                }
            }
        } catch {
            print("Failed to accept pending request: \(error)")
        }
    }
}

extension ChatSessionsViewModel {
    private func maybeStartLinkStatusPolling() {
        linkStatusPoller.startLinkPolling(isLinked: { [weak self] in
            self?.partnerInfo?.linked == true
        }) { [weak self] in
            guard let self = self else { return }
            await self.loadPartnerInfo()
        }
    }

    private func maybeStartUnlinkStatusPolling() {
        linkStatusPoller.startUnlinkPolling(isLinked: { [weak self] in
            self?.partnerInfo?.linked == true
        }) { [weak self] in
            guard let self = self else { return }
            await self.loadPartnerInfo()
        }
    }
}
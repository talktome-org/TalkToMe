import SwiftUI

class ChatSessionsViewModel: ObservableObject {

    @Published var sessions: [ChatSession] = []
    @Published var isLoadingSessions: Bool = false
    @Published var pendingRequests: [BackendService.PartnerPendingRequest] = []
    @Published var activeSessionId: UUID? = nil {
        didSet {
            if let id = activeSessionId {
                if unreadPartnerSessionIds.remove(id) != nil {
                    print("[SessionsVM] Cleared unread on active change for session=\(id)")
                    saveCachedUnread()
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
    @Published private(set) var unreadPartnerSessionIds: Set<UUID> = []

    private var suppressUnreadSessionIds: Set<UUID> = []
    private var observers: [NSObjectProtocol] = []
    private var handlingPartnerRequestIds: Set<UUID> = []
    private var hasStartedObserving: Bool = false
    private var linkStatusPollingTask: Task<Void, Never>? = nil
    private var unlinkStatusPollingTask: Task<Void, Never>? = nil
    private let avatarCacheManager = AvatarCacheManager.shared
    private weak var navigationViewModel: SidebarNavigationViewModel?
    private weak var linkViewModel: LinkViewModel?
    weak var chatViewModel: ChatViewModel?
    private var currentUserId: String?
    private var pendingAcceptancePreviewBySession: [UUID: String] = [:]

    @MainActor
    private func findNavigationViewModel() -> SidebarNavigationViewModel? {
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
        loadCachedSessions()
        loadCachedUnread()
        // Warm my avatar URL from persisted cache to avoid flicker on cold start
        if let storedMyAvatar = UserDefaults.standard.string(forKey: PreferenceKeys.myAvatarURL),
           !storedMyAvatar.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            self.myAvatarURL = storedMyAvatar
            // Warm memory/disk cache without network if possible on MainActor
            Task { @MainActor in
                _ = avatarCacheManager.getImageIfCached(urlString: storedMyAvatar)
            }
        }
    }

    deinit {
        for ob in observers { NotificationCenter.default.removeObserver(ob) }
    }

    func startNewChat() {
        activeSessionId = nil
        chatViewKey = UUID()
    }

    func resetForLogout() {
        hasStartedObserving = false
        isBootstrapComplete = false
        isBootstrapping = false
        linkStatusPollingTask?.cancel()
        unlinkStatusPollingTask?.cancel()
        linkStatusPollingTask = nil
        unlinkStatusPollingTask = nil
        sessions = []
        pendingRequests = []
        activeSessionId = nil
        myAvatarURL = nil
        partnerAvatarURL = nil
        partnerInfo = nil
        unreadPartnerSessionIds.removeAll()
        suppressUnreadSessionIds.removeAll()
        for observer in observers {
            NotificationCenter.default.removeObserver(observer)
        }
        observers.removeAll()
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
            saveCachedUnread()
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

    func loadSessions() async {
        print("🔄 Loading sessions from backend...")
        do {
            await MainActor.run { self.isLoadingSessions = self.sessions.isEmpty }
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

            // UI tweak: hide pre-created partner-request sessions until the request is accepted
            if !self.pendingRequests.isEmpty {
                let hiddenIds = Set(self.pendingRequests.compactMap { $0.recipient_session_id })
                if !hiddenIds.isEmpty {
                    mapped.removeAll { hiddenIds.contains($0.id) }
                }
            }

            if self.partnerInfo == nil {
                await loadPartnerInfo()
            }

            if self.partnerInfo?.linked == true {
                var previousSessions: [ChatSession] = []
                do {
                    let url = self.cacheURL
                    if FileManager.default.fileExists(atPath: url.path) {
                        let data = try Data(contentsOf: url)
                        previousSessions = try JSONDecoder().decode([ChatSession].self, from: data)
                    }
                } catch {
                    previousSessions = self.sessions
                }

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
                if !self.unreadPartnerSessionIds.isEmpty {
                    self.saveCachedUnread()
                }
            }

            let finalMapped = mapped
            await MainActor.run {
                self.sessions = finalMapped
                self.isLoadingSessions = false
                print("📱 Updated local sessions list with \(finalMapped.count) sessions")
                self.saveCachedSessions()
            }
        } catch {
            if let nsError = error as NSError?, nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled {
                print("⏭️ Load sessions cancelled (expected during rapid refresh) — ignoring")
                await MainActor.run { self.isLoadingSessions = false }
                return
            }
            print("❌ Failed to load sessions: \(error)")
            await MainActor.run { self.isLoadingSessions = false }
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
            await MainActor.run {
                if let idx = self.sessions.firstIndex(where: { $0.id == id }) {
                    var updated = self.sessions[idx]
                    let trimmed = newTitle?.trimmingCharacters(in: .whitespacesAndNewlines)
                    updated.title = (trimmed?.isEmpty == false) ? trimmed! : ChatSession.defaultTitle
                    self.sessions[idx] = updated
                    self.saveCachedSessions()
                }
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
            await MainActor.run {
                self.sessions.removeAll { $0.id == id }
                if self.activeSessionId == id { self.activeSessionId = nil }
                self.saveCachedSessions()
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
            }
        } catch {
            print("Failed to load pending requests: \(error)")
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
            await loadSessions()
            await loadPendingRequests()
            await loadPairedAvatars()
            await loadPartnerInfo()
            await preloadAvatars()
            print("[SessionsVM] Initial data loaded. PartnerLinked=\(self.partnerInfo?.linked ?? false)")
        }

        let created = NotificationCenter.default.addObserver(forName: .chatSessionCreated, object: nil, queue: .main) { [weak self] note in
            guard let self = self else { return }
            if let sid = note.userInfo?["sessionId"] as? UUID {
                if !self.sessions.contains(where: { $0.id == sid }) {
                    let rawTitle = (note.userInfo?["title"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
                    let title = rawTitle
                    let session = ChatSession(
                        id: sid,
                        title: title,
                        lastUsedISO8601: note.userInfo?["lastUsedISO8601"] as? String,
                        lastMessageContent: note.userInfo?["lastMessageContent"] as? String
                    )
                    self.sessions.insert(session, at: 0)
                }
            }
        }
        observers.append(created)

        let sent = NotificationCenter.default.addObserver(forName: .chatMessageSent, object: nil, queue: .main) { [weak self] note in
            guard let self = self else { return }
            if let sid = note.userInfo?["sessionId"] as? UUID,
               let messageContent = note.userInfo?["messageContent"] as? String,
               let idx = self.sessions.firstIndex(where: { $0.id == sid }) {
                var item = self.sessions.remove(at: idx)
                item.lastMessageContent = messageContent
                self.sessions.insert(item, at: 0)
                self.suppressUnreadSessionIds.insert(sid)
                DispatchQueue.main.asyncAfter(deadline: .now() + 30) { [weak self] in
                    self?.suppressUnreadSessionIds.remove(sid)
                }
                print("[SessionsVM] chatMessageSent by self; suppress unread for session=\(sid)")
            }
        }
        observers.append(sent)

        let needRefresh = NotificationCenter.default.addObserver(forName: .chatSessionsNeedRefresh, object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            Task {
                await self.refreshSessions()
                await self.loadPartnerInfo()
                await self.loadPairedAvatars()
                await self.preloadAvatars()
            }
        }
        observers.append(needRefresh)

        let willEnterForeground = NotificationCenter.default.addObserver(
            forName: UIApplication.willEnterForegroundNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self = self else { return }
            Task {
                await self.loadPartnerInfo()
                await self.loadPairedAvatars()
                await self.preloadAvatars()
            }
        }
        observers.append(willEnterForeground)

        let avatarChanged = NotificationCenter.default.addObserver(forName: .avatarChanged, object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            Task {
                await self.loadPairedAvatars()
                await self.preloadAvatars()
            }
        }
        observers.append(avatarChanged)

        let partnerReceived = NotificationCenter.default.addObserver(forName: .partnerMessageReceived, object: nil, queue: .main) { [weak self] note in
            guard let self = self else { return }
            guard let sid = note.userInfo?["sessionId"] as? UUID else {
                print("[SessionsVM] partnerMessageReceived but no sessionId in notification")
                return
            }

            let sessionExists = self.sessions.contains(where: { $0.id == sid })
            if !sessionExists {
                print("[SessionsVM] ⚠️ Session \(sid) not in local list - likely for other account on same device")
                return
            }

            // No longer refreshing LinkViewModel here to avoid clobbering prepared invite state

            if self.activeSessionId != sid && self.partnerInfo?.linked == true {
                self.unreadPartnerSessionIds.insert(sid)
                print("[SessionsVM] ✅ Marked session \(sid) as unread, total unread: \(self.unreadPartnerSessionIds.count)")
                self.saveCachedUnread()
                self.objectWillChange.send()
            } else {
                print("[SessionsVM] ❌ Not marking unread: isActive=\(self.activeSessionId == sid), linked=\(self.partnerInfo?.linked ?? false)")
            }

            if let idx = self.sessions.firstIndex(where: { $0.id == sid }) {
                var item = self.sessions.remove(at: idx)
                if let preview = note.userInfo?["messagePreview"] as? String {
                    item.lastMessageContent = preview
                }
                self.sessions.insert(item, at: 0)
                print("[SessionsVM] partnerMessageReceived → lifted session; wasIdx=\(idx)")
            }
        }
        observers.append(partnerReceived)

        let pushTapped = NotificationCenter.default.addObserver(forName: .partnerRequestOpen, object: nil, queue: .main) { [weak self] note in
            guard let self = self else { return }
            guard let requestId = note.userInfo?["requestId"] as? UUID else { return }
            guard AuthService.shared.isAuthenticated else { return }
            if self.handlingPartnerRequestIds.contains(requestId) { return }
            self.handlingPartnerRequestIds.insert(requestId)
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                do {
                    if self.pendingRequests.isEmpty { await self.loadPendingRequests() }
                    let req = self.pendingRequests.first(where: { $0.id == requestId })
                    let messageContent = req?.content ?? ""

                    let session = try await AuthService.shared.client.auth.session
                    let accessToken = session.accessToken
                    // If we already have a recipient session id from the pending request, open instantly
                    if let sid = req?.recipient_session_id {
                        self.activeSessionId = sid
                        self.chatViewKey = UUID()
                        if !messageContent.isEmpty { ChatMessagesViewModel.preCachePartnerMessage(sessionId: sid, text: messageContent) }
                        await self.chatViewModel?.loadHistory(force: true)
                    }
                    let partnerSessionId = try await BackendService.shared.acceptPartnerRequest(requestId: requestId, accessToken: accessToken)
                    await self.loadSessions()
                    if !messageContent.isEmpty { ChatMessagesViewModel.preCachePartnerMessage(sessionId: partnerSessionId, text: messageContent) }
                    self.activeSessionId = partnerSessionId
                    self.chatViewKey = UUID()
                    if let navVM = self.findNavigationViewModel() {
                        navVM.closeSidebar()
                    }
                    await self.loadPendingRequests()
                } catch {
                    print("Failed to accept partner request: \(error)")
                    await self.loadSessions()
                    if let existingSession = self.sessions.first {
                        self.activeSessionId = existingSession.id
                        self.chatViewKey = UUID()
                        if let navVM = self.findNavigationViewModel() {
                            navVM.closeSidebar()
                        }
                    }
                }
                DispatchQueue.main.asyncAfter(deadline: .now() + 10) { [weak self] in
                    self?.handlingPartnerRequestIds.remove(requestId)
                }
            }
        }
        observers.append(pushTapped)

        let partnerMessageTapped = NotificationCenter.default.addObserver(forName: .partnerMessageOpen, object: nil, queue: .main) { [weak self] note in
            guard let self = self else { return }
            guard let sessionId = note.userInfo?["sessionId"] as? UUID else { return }
            guard AuthService.shared.isAuthenticated else { return }
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                if self.partnerInfo?.linked == true && sessionId != self.activeSessionId {
                    self.unreadPartnerSessionIds.insert(sessionId)
                }
                await self.loadSessions()
                self.activeSessionId = sessionId
                self.chatViewKey = UUID()
                if let navVM = self.findNavigationViewModel() {
                    navVM.closeSidebar()
                }
            }
        }
        observers.append(partnerMessageTapped)

        maybeStartLinkStatusPolling()
    }

    func bootstrapInitialData() async {
        if isBootstrapComplete { return }
        await MainActor.run { self.isBootstrapping = true }

        await withTaskGroup(of: Void.self) { group in
            group.addTask { await self.loadSessions() }
            group.addTask { await self.loadPendingRequests() }
            group.addTask { await self.loadPartnerInfo() }
            group.addTask { await self.fetchAndCacheProfileName() }
        }

        await loadPairedAvatars()
        await preloadAvatars()
        await ensureProfilePictureCached()
        if let cachedPartnerURL = UserDefaults.standard.string(forKey: PreferenceKeys.partnerAvatarURL),
           !cachedPartnerURL.isEmpty,
           (partnerAvatarURL == nil || partnerAvatarURL?.isEmpty == true) {
            await avatarCacheManager.preloadAvatars(urls: [cachedPartnerURL])
        }

        await MainActor.run {
            self.isBootstrapping = false
            self.isBootstrapComplete = true
        }
    }

    private func fetchAndCacheProfileName() async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let token = session.accessToken
            let profile = try await BackendService.shared.fetchProfileInfo(accessToken: token)
            await MainActor.run {
                UserDefaults.standard.set(profile.full_name, forKey: "therai_profile_full_name")
                NotificationCenter.default.post(name: .profileChanged, object: nil)
            }
        } catch {
            print("Failed to fetch profile name during bootstrap: \(error)")
        }
    }

    func loadPairedAvatars() async {
        do {
            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
            let res = try await BackendService.shared.fetchPairedAvatars(accessToken: accessToken)
            await MainActor.run {
                self.myAvatarURL = res.me.url
                self.partnerAvatarURL = res.partner.url
                // Persist my avatar URL for future launches
                if let myURL = res.me.url, !myURL.isEmpty {
                    UserDefaults.standard.set(myURL, forKey: PreferenceKeys.myAvatarURL)
                } else {
                    UserDefaults.standard.removeObject(forKey: PreferenceKeys.myAvatarURL)
                }
            }
        } catch {
            print("Failed to load avatars: \(error)")
        }
    }

    func preloadAvatars() async {
        var avatarURLs: [String] = []
        if let myAvatar = myAvatarURL, !myAvatar.isEmpty {
            avatarURLs.append(myAvatar)
        }
        if let partnerAvatar = partnerAvatarURL, !partnerAvatar.isEmpty {
            avatarURLs.append(partnerAvatar)
        }
        if !avatarURLs.isEmpty {
            await avatarCacheManager.preloadAvatars(urls: avatarURLs)
        }
    }

    func ensureProfilePictureCached() async {
        if let myAvatar = myAvatarURL, !myAvatar.isEmpty {
            _ = await avatarCacheManager.getCachedImage(urlString: myAvatar)
        }
    }

    func getCachedAvatar(urlString: String?) async -> UIImage? {
        guard let urlString = urlString, !urlString.isEmpty else { return nil }
        return await avatarCacheManager.getCachedImage(urlString: urlString)
    }

    func loadPartnerInfo() async {
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
            if res.linked, let url = res.partner?.avatar_url, !url.isEmpty {
                await avatarCacheManager.preloadAvatars(urls: [url])
            }
            if res.linked {
                // Stop link-acceptance polling; start unlink polling
                linkStatusPollingTask?.cancel()
                linkStatusPollingTask = nil
                maybeStartUnlinkStatusPolling()
            } else {
                // Stop unlink polling; ensure link polling continues
                unlinkStatusPollingTask?.cancel()
                unlinkStatusPollingTask = nil
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
    private var cacheURL: URL {
        let dir = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        if let userId = currentUserId {
            return dir.appendingPathComponent("chat_sessions_cache_\(userId).json")
        }
        return dir.appendingPathComponent("chat_sessions_cache.json")
    }
    private var unreadCacheURL: URL {
        let dir = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first!
        if let userId = currentUserId {
            return dir.appendingPathComponent("chat_unread_cache_\(userId).json")
        }
        return dir.appendingPathComponent("chat_unread_cache.json")
    }

    private func loadCachedSessions() {
        do {
            let url = cacheURL
            guard FileManager.default.fileExists(atPath: url.path) else { return }
            let data = try Data(contentsOf: url)
            let decoded = try JSONDecoder().decode([ChatSession].self, from: data)
            self.sessions = decoded
        } catch {
            print("⚠️ Failed to load cached sessions: \(error)")
        }
    }

    private func saveCachedSessions() {
        do {
            let data = try JSONEncoder().encode(self.sessions)
            try data.write(to: cacheURL, options: .atomic)
        } catch {
            print("⚠️ Failed to save cached sessions: \(error)")
        }
    }

    private struct UnreadCache: Codable { let unread: [UUID] }

    private func loadCachedUnread() {
        do {
            let url = unreadCacheURL
            guard FileManager.default.fileExists(atPath: url.path) else { return }
            let data = try Data(contentsOf: url)
            let decoded = try JSONDecoder().decode(UnreadCache.self, from: data)
            self.unreadPartnerSessionIds = Set(decoded.unread)
            print("[SessionsVM] Loaded unread cache; count=\(decoded.unread.count)")
        } catch {
            print("⚠️ Failed to load unread cache: \(error)")
        }
    }

    private func saveCachedUnread() {
        do {
            let body = UnreadCache(unread: Array(self.unreadPartnerSessionIds))
            let data = try JSONEncoder().encode(body)
            try data.write(to: unreadCacheURL, options: .atomic)
            print("[SessionsVM] Saved unread cache; count=\(self.unreadPartnerSessionIds.count)")
        } catch {
            print("⚠️ Failed to save unread cache: \(error)")
        }
    }

    private func maybeStartLinkStatusPolling() {
        // Start a lightweight poll while not linked to detect acceptance quickly
        if linkStatusPollingTask != nil { return }
        linkStatusPollingTask = Task { [weak self] in
            guard let self = self else { return }
            var attempts = 0
            while !Task.isCancelled {
                if self.partnerInfo?.linked == true { break }
                attempts += 1
                do { await self.loadPartnerInfo() }
                // Poll every 5 seconds for up to ~2 minutes
                if attempts >= 24 { break }
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }

    private func maybeStartUnlinkStatusPolling() {
        // Start a lightweight poll while linked to detect unlink quickly
        if unlinkStatusPollingTask != nil { return }
        unlinkStatusPollingTask = Task { [weak self] in
            guard let self = self else { return }
            var attempts = 0
            while !Task.isCancelled {
                if self.partnerInfo?.linked != true { break }
                attempts += 1
                do { await self.loadPartnerInfo() }
                // Poll every 5 seconds for up to ~2 minutes
                if attempts >= 24 { break }
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }
}
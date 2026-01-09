import Foundation

extension ChatSessionsViewModel {
    func handleChatSessionCreated(_ note: Notification) async {
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

    func handleChatMessageSent(_ note: Notification) async {
        if let sid = note.userInfo?["sessionId"] as? UUID,
           let messageContent = note.userInfo?["messageContent"] as? String,
           let idx = self.sessions.firstIndex(where: { $0.id == sid }) {
            var item = self.sessions.remove(at: idx)
            item.lastMessageContent = messageContent
            self.sessions.insert(item, at: 0)
            self.suppressUnreadSessionIds.insert(sid)

            Task { @MainActor [weak self] in
                try? await Task.sleep(nanoseconds: 30_000_000_000)
                self?.suppressUnreadSessionIds.remove(sid)
            }

            print("[SessionsVM] chatMessageSent by self; suppress unread for session=\(sid)")
        }
    }

    func handleChatSessionsNeedRefresh() async {
        await self.refreshSessions()
        await self.loadPartnerInfo()
        await self.loadPairedAvatars()
        await self.preloadAvatars()
    }

    func handleWillEnterForeground() async {
        await self.loadPartnerInfo()
        await self.loadPairedAvatars()
        await self.preloadAvatars()
    }

    func handleAvatarChanged() async {
        await self.loadPairedAvatars()
        await self.preloadAvatars()
    }

    func handlePartnerMessageReceived(_ note: Notification) async {
        guard let sid = note.userInfo?["sessionId"] as? UUID else {
            print("[SessionsVM] partnerMessageReceived but no sessionId in notification")
            return
        }

        let sessionExists = self.sessions.contains(where: { $0.id == sid })
        if !sessionExists {
            print("[SessionsVM] ⚠️ Session \(sid) not in local list - likely for other account on same device")
            return
        }

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

    func handlePartnerRequestOpen(_ note: Notification) async {
        guard let requestId = note.userInfo?["requestId"] as? UUID else { return }
        guard AuthService.shared.isAuthenticated else { return }
        if self.handlingPartnerRequestIds.contains(requestId) { return }
        self.handlingPartnerRequestIds.insert(requestId)

        do {
            if self.pendingRequests.isEmpty { await self.loadPendingRequests() }
            let req = self.pendingRequests.first(where: { $0.id == requestId })
            let messageContent = req?.content ?? ""

            let session = try await AuthService.shared.client.auth.session
            let accessToken = session.accessToken
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

        Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: 10_000_000_000)
            self?.handlingPartnerRequestIds.remove(requestId)
        }
    }

    func handlePartnerMessageOpen(_ note: Notification) async {
        guard let sessionId = note.userInfo?["sessionId"] as? UUID else { return }
        guard AuthService.shared.isAuthenticated else { return }
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



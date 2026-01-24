import Foundation
import SwiftUI
import UIKit
import Combine

@MainActor
class ChatViewModel: ObservableObject {

    private static func beginBackgroundTask(name: String, onExpire: (() -> Void)? = nil) -> UIBackgroundTaskIdentifier {
        var identifier: UIBackgroundTaskIdentifier = .invalid
        let work = {
            identifier = UIApplication.shared.beginBackgroundTask(withName: name) {
                onExpire?()
                UIApplication.shared.endBackgroundTask(identifier)
            }
        }
        if Thread.isMainThread {
            work()
        } else {
            DispatchQueue.main.sync { work() }
        }
        return identifier
    }

    private static func endBackgroundTask(_ identifier: UIBackgroundTaskIdentifier?) {
        guard let id = identifier, id != .invalid else { return }
        DispatchQueue.main.async {
            UIApplication.shared.endBackgroundTask(id)
        }
    }

    @Published var messages: [ChatMessage] = []
    @Published var inputText: String = ""
    @Published var focusSnippet: String? = nil
    @Published var pendingAttachments: [PendingAttachment] = []
    @Published var focusTopMessageId: UUID? = nil
    @Published var assistantScrollTargetId: UUID? = nil
    @Published var streamingScrollToken: Int = 0
    @Published var selectedFriendUserId: UUID? = nil
    @Published var sessionId: UUID? {
        didSet {
#if DEBUG
            let oldStr = oldValue?.uuidString ?? "nil"
            let newStr = sessionId?.uuidString ?? "nil"
            if oldStr != newStr {
                debugLog("[ChatVM] sessionId changed \(oldStr) -> \(newStr)")
            }
            if sessionId == nil {
                let stack = Thread.callStackSymbols.prefix(12).joined(separator: "\n")
                debugLog("[ChatVM] sessionId set to nil (will clear UI state). Call stack:\n\(stack)")
            }
#endif
            if sessionId == nil {
                messages = []
                selectedFriendUserId = nil
            }
        }
    }
    @Published var isLoading: Bool = false
    @Published var isLoadingHistory: Bool = false
    @Published var isAssistantTyping: Bool = false
    @Published var initialJumpToken: Int = 0

    private let backend = BackendService.shared
    private let authService = AuthService.shared
    private let chatMessagesVM: ChatMessagesViewModel
    let partnerDrafts = PartnerDraftsViewModel()
    private var currentStreamTask: Task<Void, Never>?
    private var currentStreamToken: UUID?
    private var typingDelayTask: Task<Void, Never>?
    private var receivedAnyAssistantOutput: Bool = false
    private var currentAssistantMessageId: UUID?
    private var isStreaming: Bool = false
    private var responseIdBySession: [UUID: String] = [:]
    private var assistantMessageIdBySession: [UUID: UUID] = [:]
    private var currentStreamingSessionId: UUID?
    private var observers: [NSObjectProtocol] = []
    private var refreshTimer: Timer?
    private var cancellables: Set<AnyCancellable> = []

    /// True when this chat session is connected to a friend thread (sender picked a friend,
    /// or recipient has received partner messages in this session).
    var isConnectedToFriendInThisChat: Bool {
        // Sender-side: once user picked a friend, we persist selection in the ChatViewModel.
        if selectedFriendUserId != nil { return true }

        // Recipient-side: once a message has been delivered into this session, it's already a friend thread.
        return messages.contains(where: { msg in
            msg.segments.contains(where: { seg in
                if case .partnerReceived(_) = seg { return true }
                return false
            })
        })
    }

    private static func friendKey(for sessionId: UUID) -> String {
        "session_friend_user_id_\(sessionId.uuidString)"
    }

    private func loadPersistedFriendUserId(for sessionId: UUID) -> UUID? {
        guard let raw = UserDefaults.standard.string(forKey: Self.friendKey(for: sessionId)),
              let id = UUID(uuidString: raw)
        else { return nil }
        return id
    }

    private func persistFriendUserId(_ friendUserId: UUID, for sessionId: UUID) {
        UserDefaults.standard.set(friendUserId.uuidString, forKey: Self.friendKey(for: sessionId))
    }

    private func movePersistedFriendUserId(from oldId: UUID, to newId: UUID) {
        let oldKey = Self.friendKey(for: oldId)
        let newKey = Self.friendKey(for: newId)
        if let raw = UserDefaults.standard.string(forKey: oldKey) {
            UserDefaults.standard.set(raw, forKey: newKey)
            UserDefaults.standard.removeObject(forKey: oldKey)
        }
    }

    private func cancelCurrentStream() {
        currentStreamTask?.cancel()
        currentStreamTask = nil
        currentStreamToken = nil
    }

    private nonisolated func debugLog(_ message: @autoclosure () -> String) {
#if DEBUG
        print(message())
#endif
    }

    init(sessionId: UUID? = nil) {
        self.sessionId = sessionId
        if let sid = sessionId,
           let raw = UserDefaults.standard.string(forKey: Self.friendKey(for: sid)),
           let fid = UUID(uuidString: raw) {
            self.selectedFriendUserId = fid
        }
        self.chatMessagesVM = ChatMessagesViewModel(sessionId: sessionId)
        self.messages = chatMessagesVM.messages
        self.isLoadingHistory = chatMessagesVM.isLoadingHistory

        chatMessagesVM.$messages
            .receive(on: DispatchQueue.main)
            .sink { [weak self] newMessages in
                self?.messages = newMessages
            }
            .store(in: &cancellables)

        chatMessagesVM.$isLoadingHistory
            .receive(on: DispatchQueue.main)
            .sink { [weak self] loading in
                self?.isLoadingHistory = loading
            }
            .store(in: &cancellables)

        partnerDrafts.objectWillChange
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.objectWillChange.send()
            }
            .store(in: &cancellables)

        Task { [weak self] in
            guard let self = self else { return }
            await self.loadHistory()
            if !self.messages.isEmpty { self.initialJumpToken &+= 1 }
        }

        let sessionRekeyed = NotificationCenter.default.addObserver(
            forName: .chatSessionRekeyed,
            object: nil,
            queue: .main
        ) { [weak self] note in
            Task { @MainActor in
                guard let self else { return }
                guard
                    let oldId = note.userInfo?["oldSessionId"] as? UUID,
                    let newId = note.userInfo?["newSessionId"] as? UUID
                else { return }
                if self.sessionId == oldId {
                    self.movePersistedFriendUserId(from: oldId, to: newId)
                    self.partnerDrafts.rekeySentDrafts(oldSessionId: oldId, newSessionId: newId)
                    self.sessionId = newId
                    self.selectedFriendUserId = self.loadPersistedFriendUserId(for: newId)
                    await self.presentSession(newId)
                }
            }
        }
        observers.append(sessionRekeyed)

        // Partner-draft sending is handled by ChatView (it must show the friend picker if not connected).
    }

    deinit {
        for ob in observers { NotificationCenter.default.removeObserver(ob) }
        refreshTimer?.invalidate()
    }

    func ensureSessionId() async -> UUID? {
        if let sid = sessionId { return sid }
        do {
            guard let accessToken = await authService.getAccessToken() else { return nil }
            let dto = try await backend.createEmptySession(accessToken: accessToken)
            await MainActor.run {
                self.sessionId = dto.id
                let currentTime = ISO8601DateFormatter().string(from: Date())
                NotificationCenter.default.post(name: .chatSessionCreated, object: nil, userInfo: [
                    "sessionId": dto.id,
                    "title": ChatSession.defaultTitle,
                    "lastUsedISO8601": currentTime,
                    "lastMessageContent": ""
                ])
            }
            return dto.id
        } catch {
            self.debugLog("[ChatVM] ensureSessionId failed: \(error)")
            return nil
        }
    }

    func loadHistory(force: Bool = false) async {
        await chatMessagesVM.loadHistory(force: force)
        self.messages = chatMessagesVM.messages
        self.isLoadingHistory = chatMessagesVM.isLoadingHistory
    }

    func sendPartnerDraftToSelectedFriend(_ text: String) async {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard let friendId = selectedFriendUserId else { return }

        do {
            let sid = await ensureSessionId()
            guard let sid else { return }
            persistFriendUserId(friendId, for: sid)

            guard let accessToken = await authService.getAccessToken() else { return }

            _ = try await backend.sendPartnerMessage(
                message: trimmed,
                sessionId: sid,
                friendUserId: friendId,
                accessToken: accessToken
            )

            partnerDrafts.markPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
        } catch {
            self.debugLog("[ChatVM] sendPartnerDraftToSelectedFriend failed: \(error)")
        }
    }

    func sendPartnerDraftViaSession(_ text: String) async {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        do {
            let sid = await ensureSessionId()
            guard let sid else { return }
            guard let accessToken = await authService.getAccessToken() else { return }
            _ = try await backend.sendPartnerMessage(
                message: trimmed,
                sessionId: sid,
                friendUserId: nil,
                accessToken: accessToken
            )
            partnerDrafts.markPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
        } catch {
            self.debugLog("[ChatVM] sendPartnerDraftViaSession failed: \(error)")
        }
    }

    func presentSession(_ id: UUID) async {
        await MainActor.run {
            self.sessionId = id
            self.selectedFriendUserId = self.loadPersistedFriendUserId(for: id)
            if let placeholderId = self.assistantMessageIdBySession[id] {
                self.currentAssistantMessageId = placeholderId
            } else {
                self.currentAssistantMessageId = nil
            }
            self.isLoading = (self.currentStreamingSessionId == id)
            self.isAssistantTyping = false
        }

        await chatMessagesVM.presentSession(id)
        await MainActor.run {
            self.messages = chatMessagesVM.messages
            self.isLoadingHistory = chatMessagesVM.isLoadingHistory
            if !self.messages.isEmpty { self.initialJumpToken &+= 1 }
        }
    }

    func sendMessage() {
        let trimmedMessage = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
        let attachmentsToSend = pendingAttachments
        guard !(trimmedMessage.isEmpty && attachmentsToSend.isEmpty) else { return }
        guard !isStreaming else { return }

        // IMPORTANT: For image-only (and attachment-only) sends, we want to send ONLY the attachment.
        // The backend already stores attachments as "segments" when attachments are present, so sending
        // an empty message avoids injecting a fake text segment like "User sent a photo."
        let messageToSend = trimmedMessage
        let previewToSend: String = {
            if !trimmedMessage.isEmpty { return trimmedMessage }
            if attachmentsToSend.contains(where: { $0.isImage }) { return "Sent a photo." }
            return "Sent an attachment."
        }()

        // Ensure we always have a local session ID so we can persist the user message even if the request fails.
        var createdLocalSessionId: UUID? = nil
        if self.sessionId == nil {
            let localId = UUID()
            createdLocalSessionId = localId
            self.sessionId = localId
            self.chatMessagesVM.sessionId = localId
            let currentTime = ISO8601DateFormatter().string(from: Date())
            NotificationCenter.default.post(name: .chatSessionCreated, object: nil, userInfo: [
                "sessionId": localId,
                "title": ChatSession.defaultTitle,
                "lastUsedISO8601": currentTime,
                "lastMessageContent": ""
            ])

            self.debugLog("[ChatVM] Created local-only session id=\(localId.uuidString) (will rekey once server assigns id)")

            // Persist the local-only session so it survives app restarts even if the network / backend call fails.
            Task.detached {
                await ChatStore.shared.upsertSessions([
                    ChatSession(id: localId, title: ChatSession.defaultTitle, lastUsedISO8601: currentTime, lastMessageContent: "")
                ])
            }
        }

        guard let sid = self.sessionId else { return }
        if let friendId = selectedFriendUserId {
            persistFriendUserId(friendId, for: sid)
        }
        let friendToUse = selectedFriendUserId

        let userId: UUID? = {
            if let uid = AuthService.shared.currentUser?.id { return uid }
            if let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId),
               let uid = UUID(uuidString: raw) {
                return uid
            }
            return nil
        }()
        guard let userId else { return }

        // Persist attachments to disk now so they survive restarts even if streaming fails.
        var outboxAttachments: [OutboxAttachment] = []
        var segs: [[String: Any]] = []
        if !trimmedMessage.isEmpty {
            segs.append(["type": "text", "content": trimmedMessage])
        }

        let fm = FileManager.default
        let baseDir: URL = {
            let appSupport = (try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)) ?? fm.temporaryDirectory
            let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")
            let cleaned = raw.unicodeScalars.map { allowed.contains($0) ? Character($0) : "-" }
            let key = raw.isEmpty ? "unauthenticated" : String(cleaned)
            let dir = appSupport.appendingPathComponent("TalkToMe/OutboxAttachments/\(key)", isDirectory: true)
            try? fm.createDirectory(at: dir, withIntermediateDirectories: true)
            return dir
        }()

        if !attachmentsToSend.isEmpty {
            for att in attachmentsToSend {
                switch att.kind {
                case .image(let data, let ct):
                    let ext: String = {
                        if ct == "image/png" { return "png" }
                        if ct == "image/webp" { return "webp" }
                        return "jpg"
                    }()
                    let filename = "image.\(ext)"
                    let fileURL = baseDir.appendingPathComponent(UUID().uuidString).appendingPathExtension(ext)
                    try? data.write(to: fileURL, options: [.atomic])
                    outboxAttachments.append(
                        OutboxAttachment(type: "image", filename: filename, contentType: ct, localPath: fileURL.path)
                    )
                    segs.append(["type": "image", "url": fileURL.absoluteString, "filename": filename, "content_type": ct])
                case .file(let data, let filename, let ct):
                    let fileURL = baseDir.appendingPathComponent(UUID().uuidString + "_" + filename)
                    try? data.write(to: fileURL, options: [.atomic])
                    outboxAttachments.append(
                        OutboxAttachment(type: "file", filename: filename, contentType: ct, localPath: fileURL.path)
                    )
                    segs.append(["type": "file", "url": fileURL.absoluteString, "filename": filename, "content_type": ct])
                }
            }
        }

        let persistedContent: String = {
            guard !attachmentsToSend.isEmpty else { return messageToSend }
            let obj: [String: Any] = ["_talktome": ["type": "segments", "segments": segs]]
            let data = (try? JSONSerialization.data(withJSONObject: obj)) ?? Data()
            return String(data: data, encoding: .utf8) ?? messageToSend
        }()

        let dto = BackendService.ChatMessageDTO(
            id: UUID(),
            user_id: userId,
            session_id: sid,
            role: "user",
            content: persistedContent,
            created_at: ISO8601DateFormatter().string(from: Date())
        )
        let clientMessageId = dto.id

        // Render + persist immediately so the user's message survives failures (no credits, no internet, etc).
        let localMessage = ChatMessage(dto: dto, currentUserId: userId)
        self.messages.append(localMessage)
        self.updateCacheForCurrentSession()
        Task.detached {
            await ChatStore.shared.upsertMessages([dto])
        }

        NotificationCenter.default.post(name: .chatMessageSent, object: nil, userInfo: [
            "sessionId": sid,
            "messageContent": previewToSend
        ])
        NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)

        // Keep current UI behavior (optimistic local message).
        inputText = ""
        pendingAttachments = []

        // If we're offline, stop here and rely on the outbox to send later.
        if NetworkMonitor.shared.isOnline == false {
            Task.detached {
                await ChatOutboxProcessor.shared.enqueueChatMessage(
                    sessionId: sid,
                    serverSessionId: nil,
                    friendUserId: friendToUse,
                    messageId: clientMessageId,
                    message: messageToSend,
                    attachments: outboxAttachments
                )
            }
            isLoading = false
            isAssistantTyping = false
            isStreaming = false
            currentStreamingSessionId = nil
            return
        }

        isLoading = true
        isAssistantTyping = false
        receivedAnyAssistantOutput = false
        typingDelayTask?.cancel()
        typingDelayTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 500_000_000)
            await MainActor.run {
                guard let self = self else { return }
                if self.isLoading && !self.receivedAnyAssistantOutput {
                    self.isAssistantTyping = true
                }
            }
        }

        cancelCurrentStream()

        let placeholderMessage = ChatMessage.text("", isFromUser: false)
        messages.append(placeholderMessage)
        currentAssistantMessageId = placeholderMessage.id
        assistantScrollTargetId = placeholderMessage.id
        streamingScrollToken = 0

        if let sid = self.sessionId {
            assistantMessageIdBySession[sid] = placeholderMessage.id
        }
		updateCacheForCurrentSession()

        // Snapshot stream context immediately so it can't accidentally switch sessions mid-send.
        // (This prevents cross-chat contamination if the user navigates while we await network work.)
        let initialMessagesForStream = self.messages
        let initialAssistantPlaceholderId = self.currentAssistantMessageId
        let chatHistoryForStream: [BackendService.ChatHistoryMessage] = initialMessagesForStream.dropLast(2).map { message in
            let plain = message.segments.compactMap { seg -> String? in
                if case .text(let t) = seg { return t }
                return nil
            }.joined()
            return BackendService.ChatHistoryMessage(
                role: message.isFromUser ? "user" : "assistant",
                content: plain
            )
        }

        Task { [weak self] in
            guard let self = self else { return }
            guard let accessToken = await authService.getAccessToken() else {
                self.debugLog("[ChatVM] ACCESS_TOKEN: <nil>")
                return
            }

            // IMPORTANT: Pin all stream/session identifiers to the session the user actually sent from.
            // `self.sessionId` can change while we await (e.g. user switches chats), and we must never
            // accidentally send a message into a different session.
            let localSessionIdForSend = sid
            let createdLocalSessionIdForSend = createdLocalSessionId
            let requestSessionIdForStream: UUID? = (createdLocalSessionIdForSend != nil) ? nil : localSessionIdForSend

            await MainActor.run { self.isStreaming = true }
            await MainActor.run { self.currentStreamingSessionId = localSessionIdForSend }
            let bgName = "chat_stream_" + localSessionIdForSend.uuidString
            let bgTask: UIBackgroundTaskIdentifier? = Self.beginBackgroundTask(name: bgName) { [weak self] in
                guard let self = self else { return }
                Task { @MainActor in
                    self.cancelCurrentStream()
                    self.isStreaming = false
                    self.isAssistantTyping = false
                    self.currentStreamingSessionId = nil
                    self.currentAssistantMessageId = nil
                    self.updateCacheForCurrentSession()
                }
            }
            self.debugLog("[ChatVM] stream starting (manager); sessionId=\(localSessionIdForSend.uuidString) messagesCount=\(self.messages.count)")

            var uploaded: [BackendService.ChatAttachment] = []
            if !attachmentsToSend.isEmpty {
                for att in attachmentsToSend {
                    let data: Data
                    let filename: String
                    let contentType: String
                    let type: String
                    switch att.kind {
                    case .image(let d, let ct):
                        data = d
                        contentType = ct
                        let ext: String = {
                            if ct == "image/png" { return "png" }
                            if ct == "image/webp" { return "webp" }
                            return "jpg"
                        }()
                        filename = "image.\(ext)"
                        type = "image"
                    case .file(let d, let name, let ct):
                        data = d
                        contentType = ct
                        filename = name
                        type = "file"
                    }

                    do {
                        let res = try await self.backend.uploadChatAttachment(
                            fileData: data,
                            filename: filename,
                            contentType: contentType,
                            accessToken: accessToken
                        )
                        uploaded.append(
                            BackendService.ChatAttachment(
                                type: type,
                                path: res.path,
                                filename: filename,
                                contentType: contentType
                            )
                        )
                    } catch {
                        self.debugLog("[ChatVM] Attachment upload failed: \(error)")
                        await MainActor.run {
                            if !self.messages.isEmpty {
                                self.messages[self.messages.count - 1] = ChatMessage.text(
                                    "Error: Failed to upload attachment.",
                                    isFromUser: false
                                )
                            }
                            self.isLoading = false
                            self.isAssistantTyping = false
                            self.isStreaming = false
                            self.currentStreamingSessionId = nil
                            self.updateCacheForCurrentSession()
                        }
                        Self.endBackgroundTask(bgTask)
                        return
                    }
                }
            }

            let chatHistory = chatHistoryForStream

            var accumulated = ""
            var currentSegments: [MessageSegment] = []
            var sawToolStart = false
            var sawPartnerMessage = false
            var eventCounter = 0
            // Track which session incoming SSE events belong to.
            // Start with the session-at-send id; backend will emit a `session` event that may rekey it.
            var streamSessionId: UUID? = requestSessionIdForStream ?? localSessionIdForSend
            let prevId: String? = {
                return self.responseIdBySession[localSessionIdForSend]
            }()

            let streamToken = UUID()
            let onEvent: (BackendService.StreamEvent) -> Void = { [weak self] event in
                    guard let self = self else { return }
                    eventCounter += 1
                    switch event {
                    case .responseId(let rid):
                        Task { @MainActor in
                            let targetSid = streamSessionId ?? self.sessionId
                            if let sid = targetSid {
                                self.responseIdBySession[sid] = rid
                            }
                        }
                    case .toolStart:
                        sawToolStart = true
                        Task { @MainActor in
                            let targetSid = streamSessionId ?? self.sessionId
                            guard let sid = targetSid else { return }
                            if sid == self.sessionId {
                                var newMessages = self.messages
                                let idx: Int = {
                                    if let id = self.currentAssistantMessageId,
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.currentAssistantMessageId = placeholder.id
                                    if let sid = self.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: last.segments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: true
                                )
                                newMessages[idx] = updated
                                self.messages = newMessages
                                if let id = self.currentAssistantMessageId { self.assistantScrollTargetId = id }
                                self.streamingScrollToken &+= 1
                            } else {
                                var newMessages = self.chatMessagesVM.getCachedMessages(for: sid) ?? []
                                let idx: Int = {
                                    if let id = self.assistantMessageIdBySession[sid],
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.assistantMessageIdBySession[sid] = placeholder.id
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: last.segments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: true
                                )
                                newMessages[idx] = updated
                                self.chatMessagesVM.setCachedMessages(newMessages, for: sid)
                            }
                            self.debugLog("[ChatVM] toolStart received; showing loader (manager)")
                        }
                    case .toolArgs:
                        if !sawToolStart {
                            self.debugLog("[ChatVM] toolArgs before toolStart; loader may be delayed")
                        }
                    case .toolDone:
                        Task { @MainActor in
                            let targetSid = streamSessionId ?? self.sessionId
                            guard let sid = targetSid else { return }
                            if sid == self.sessionId {
                                var newMessages = self.messages
                                let idx: Int = {
                                    if let id = self.currentAssistantMessageId,
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.currentAssistantMessageId = placeholder.id
                                    if let sid = self.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: last.segments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: false
                                )
                                newMessages[idx] = updated
                                self.messages = newMessages
                                if let id = self.currentAssistantMessageId { self.assistantScrollTargetId = id }
                                self.streamingScrollToken &+= 1
                            } else {
                                var newMessages = self.chatMessagesVM.getCachedMessages(for: sid) ?? []
                                let idx: Int = {
                                    if let id = self.assistantMessageIdBySession[sid],
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.assistantMessageIdBySession[sid] = placeholder.id
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: last.segments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: false
                                )
                                newMessages[idx] = updated
                                self.chatMessagesVM.setCachedMessages(newMessages, for: sid)
                            }
                            self.debugLog("[ChatVM] toolDone received; hiding loader (manager)")
                        }
                    case .session(let sid):
                        streamSessionId = sid
                        self.debugLog("[ChatVM] SSE session event sid=\(sid.uuidString) currentSessionId=\(self.sessionId?.uuidString ?? "nil") createdLocal=\(createdLocalSessionIdForSend?.uuidString ?? "nil")")
                        Task { @MainActor in
                            // If we created a local-only session for persistence, rekey it to the server session id.
                            if let local = createdLocalSessionIdForSend, self.sessionId == local, sid != local {
                                await ChatStore.shared.rekeySession(oldId: local, newId: sid)
                                NotificationCenter.default.post(name: .chatSessionRekeyed, object: nil, userInfo: [
                                    "oldSessionId": local,
                                    "newSessionId": sid
                                ])
                                NotificationCenter.default.post(name: .chatSessionCreated, object: nil, userInfo: [
                                    "sessionId": sid,
                                    "title": ChatSession.defaultTitle,
                                    "lastUsedISO8601": ISO8601DateFormatter().string(from: Date()),
                                    "lastMessageContent": ""
                                ])
                                self.sessionId = sid
                                self.chatMessagesVM.sessionId = sid
                            } else if self.sessionId == nil {
                                self.sessionId = sid
                                self.chatMessagesVM.sessionId = sid
                            }
                            self.chatMessagesVM.setCachedMessages(initialMessagesForStream, for: sid)
                            if let placeholderId = initialAssistantPlaceholderId {
                                self.assistantMessageIdBySession[sid] = placeholderId
                            }
                            self.currentStreamingSessionId = sid
                        }
                    case .token(let token):
                        Task { @MainActor in
                            let targetSid = streamSessionId ?? self.sessionId
                            guard let sid = targetSid else { return }
                            if !self.receivedAnyAssistantOutput {
                                self.receivedAnyAssistantOutput = true
                                self.typingDelayTask?.cancel()
                                self.isAssistantTyping = false
                            }

                            accumulated += token
                            if !currentSegments.isEmpty, case .text(let existingText) = currentSegments[currentSegments.count - 1] {
                                currentSegments[currentSegments.count - 1] = .text(existingText + token)
                            } else {
                                if currentSegments.isEmpty {
                                    currentSegments = [.text(token)]
                                } else {
                                    currentSegments.append(.text(token))
                                }
                            }
                            if sid == self.sessionId {
                                var newMessages = self.messages
                                let idx: Int = {
                                    if let id = self.currentAssistantMessageId,
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.currentAssistantMessageId = placeholder.id
                                    if let sid = self.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: currentSegments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: last.isToolLoading
                                )
                                newMessages[idx] = updated
                                self.messages = newMessages
                                if let id = self.currentAssistantMessageId { self.assistantScrollTargetId = id }
                                self.streamingScrollToken &+= 1
                                self.debugLog("[ChatVM] token update length=\(accumulated.count)")
                            } else {
                                var newMessages = self.chatMessagesVM.getCachedMessages(for: sid) ?? []
                                let idx: Int = {
                                    if let id = self.assistantMessageIdBySession[sid],
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.assistantMessageIdBySession[sid] = placeholder.id
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: currentSegments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: last.isToolLoading
                                )
                                newMessages[idx] = updated
                                self.chatMessagesVM.setCachedMessages(newMessages, for: sid)
                            }
                        }
                    case .partnerMessage(let text):
                        Task { @MainActor in
                            let targetSid = streamSessionId ?? self.sessionId
                            guard let sid = targetSid else { return }
                            if !self.receivedAnyAssistantOutput {
                                self.receivedAnyAssistantOutput = true
                                self.typingDelayTask?.cancel()
                                self.isAssistantTyping = false
                            }
                            sawPartnerMessage = true
                            currentSegments.append(.partnerMessage(text))
                            self.debugLog("[ChatVM] partner_message received len=\(text.count)")
                            if sid == self.sessionId {
                                var newMessages = self.messages
                                let idx: Int = {
                                    if let id = self.currentAssistantMessageId,
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.currentAssistantMessageId = placeholder.id
                                    if let sid = self.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: currentSegments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: false
                                )
                                newMessages[idx] = updated
                                self.messages = newMessages
                                if let id = self.currentAssistantMessageId { self.assistantScrollTargetId = id }
                                self.streamingScrollToken &+= 1
                                self.debugLog("[ChatVM] appended draft as segment; total segments=\(currentSegments.count)")
                            } else {
                                var newMessages = self.chatMessagesVM.getCachedMessages(for: sid) ?? []
                                let idx: Int = {
                                    if let id = self.assistantMessageIdBySession[sid],
                                       let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                    let placeholder = ChatMessage.text("", isFromUser: false)
                                    newMessages.append(placeholder)
                                    self.assistantMessageIdBySession[sid] = placeholder.id
                                    return newMessages.count - 1
                                }()
                                let last = newMessages[idx]
                                let updated = ChatMessage(
                                    id: last.id,
                                    segments: currentSegments,
                                    isFromUser: false,
                                    timestamp: last.timestamp,
                                    isToolLoading: false
                                )
                                newMessages[idx] = updated
                                self.chatMessagesVM.setCachedMessages(newMessages, for: sid)
                            }
                        }
                    case .done:
                        self.debugLog("[ChatVM] stream done (manager); sawToolStart=\(sawToolStart) sawPartnerMessage=\(sawPartnerMessage) events=\(eventCounter)")

                        let targetSid = streamSessionId ?? self.sessionId
                        if let sid = targetSid, sid != self.sessionId {
                            Task { @MainActor in
                                if let arr = self.chatMessagesVM.getCachedMessages(for: sid) {
                                    self.chatMessagesVM.setCachedMessages(arr, for: sid)
                                }
                                self.assistantMessageIdBySession[sid] = nil
                                if self.currentStreamingSessionId == sid { self.currentStreamingSessionId = nil }
                            }
                        } else {
                            Task { @MainActor in self.isLoading = false; self.isAssistantTyping = false; self.isStreaming = false }
                            Task { @MainActor in self.currentAssistantMessageId = nil }
                            Task { @MainActor in self.currentStreamingSessionId = nil }
                            Task { @MainActor in self.focusTopMessageId = nil }
                            if let sid = self.sessionId {
                                Task { @MainActor in
                                    self.chatMessagesVM.setCachedMessages(self.messages, for: sid)
                                }
                            }
                            Self.endBackgroundTask(bgTask)

                            // Refresh sidebar sessions after the stream completes.
                            // The backend may update `last_message_*` and/or generate a chat title
                            // after it has received enough context, so a refresh at send-time can be too early.
                            if NetworkMonitor.shared.isOnline {
                                // Persist the canonical server history to GRDB after streaming completes.
                                // This avoids the "opens with 1 message, then loads full history" effect on next launch.
                                if let sid = (streamSessionId ?? self.sessionId) {
                                    let tokenForSync = accessToken
                                    Task.detached {
                                        try? await Task.sleep(nanoseconds: 900_000_000) // allow server background persistence to commit
                                        do {
                                            let dtos = try await BackendService.shared.fetchMessages(sessionId: sid, accessToken: tokenForSync)
                                            await ChatStore.shared.upsertMessages(dtos)
#if DEBUG
                                            print("[ChatVM] post-stream sync persisted \(dtos.count) messages to GRDB for session \(sid.uuidString)")
#endif
                                        } catch {
#if DEBUG
                                            print("[ChatVM] post-stream sync failed for session \(sid.uuidString): \(error)")
#endif
                                        }
                                    }
                                }
                                Task.detached {
                                    try? await Task.sleep(nanoseconds: 900_000_000) // small delay for server-side updates
                                    NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)
                                }
                            }
                        }
                    case .error(let message):
                        self.debugLog("[ChatVM] stream error (manager): \(message)")
                        Task { @MainActor in

                            let targetSid = streamSessionId ?? self.sessionId
                            if let sid = targetSid, sid != self.sessionId {
                                var newMessages = self.chatMessagesVM.getCachedMessages(for: sid) ?? []
                                if !newMessages.isEmpty {
                                    newMessages[newMessages.count - 1] = ChatMessage.text("Error: \(message)", isFromUser: false)
                                } else {
                                    newMessages.append(ChatMessage.text("Error: \(message)", isFromUser: false))
                                }
                                self.chatMessagesVM.setCachedMessages(newMessages, for: sid)
                                self.assistantMessageIdBySession[sid] = nil
                            } else {
                                if !self.messages.isEmpty {
                                    self.messages[self.messages.count - 1] = ChatMessage.text("Error: \(message)", isFromUser: false)
                                }
                                self.isLoading = false
                                self.isAssistantTyping = false
                                self.isStreaming = false
                                self.currentStreamingSessionId = nil
                                self.updateCacheForCurrentSession()
                            }
                            Self.endBackgroundTask(bgTask)
                        }
                        // Queue for retry so the message can be sent later (e.g. no credits / transient network).
                        let outboxSessionId: UUID = streamSessionId ?? localSessionIdForSend
                        let outboxServerSessionId: UUID? = {
                            guard let s = streamSessionId else { return nil }
                            if let created = createdLocalSessionIdForSend, s == created { return nil }
                            return s
                        }()
                        Task.detached {
                            await ChatOutboxProcessor.shared.enqueueChatMessage(
                                sessionId: outboxSessionId,
                                serverSessionId: outboxServerSessionId,
                                friendUserId: friendToUse,
                                messageId: clientMessageId,
                                message: messageToSend,
                                attachments: outboxAttachments
                            )
                        }
                    }
            }

            let onFinish: () -> Void = { [weak self] in
                    Task { @MainActor in
                        self?.isLoading = false
                        self?.isAssistantTyping = false
                        self?.isStreaming = false
                        self?.currentAssistantMessageId = nil
                        self?.updateCacheForCurrentSession()
                    }
            }

            let focusSnippetForStream = self.focusSnippet

            let task = Task.detached { [streamToken] in
                let stream = BackendService.shared.streamChatMessage(
                    messageToSend,
                    sessionId: requestSessionIdForStream,
                    chatHistory: Array(chatHistory),
                    attachments: uploaded.isEmpty ? nil : uploaded,
                    accessToken: accessToken,
                    focusSnippet: focusSnippetForStream,
                    previousResponseId: prevId,
                    friendUserId: friendToUse,
                    messageId: clientMessageId
                )
                for await event in stream {
                    onEvent(event)
                    if case .done = event { break }
                    if case .error(_) = event { break }
                }
                onFinish()
                await MainActor.run { [weak self] in
                    guard let self else { return }
                    if self.currentStreamToken == streamToken {
                        self.currentStreamTask = nil
                        self.currentStreamToken = nil
                    }
                }
            }

            await MainActor.run {
                self.currentStreamTask = task
                self.currentStreamToken = streamToken
            }
        }
    }

    func stopGeneration() {
        cancelCurrentStream()
        isLoading = false
        isStreaming = false
    }

    private func updateCacheForCurrentSession() {
        chatMessagesVM.updateCacheForCurrentSession(currentMessages: self.messages)
    }
}


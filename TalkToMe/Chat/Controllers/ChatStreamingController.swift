import Foundation
import UIKit

@MainActor
protocol ChatStreamingDelegate: AnyObject {
    var inputText: String { get set }
    var replyQuoteText: String? { get set }
    var messages: [ChatMessage] { get set }
    var pendingAttachments: [PendingAttachment] { get set }
    var sessionId: UUID? { get set }
    var selectedFriendUserId: UUID? { get }
    var isLoading: Bool { get set }
    var isAssistantTyping: Bool { get set }
    var thinkingText: String { get set }
    var thinkingTextDone: Bool { get set }
    var pendingOutgoingUserMessageId: UUID? { get set }
    var assistantScrollTargetId: UUID? { get set }
    var streamingScrollToken: Int { get set }

    func getAccessToken() async -> String?
    func persistFriendUserId(_ friendUserId: UUID, for sessionId: UUID)
    func getCachedMessages(for sessionId: UUID) -> [ChatMessage]?
    func setCachedMessages(_ messages: [ChatMessage], for sessionId: UUID)
    func setChatMessagesVMSessionId(_ id: UUID)
    func streamingWillStart()
    func streamingDidReceiveToken(_ token: String)
    func streamingDidFinish()
}

@MainActor
final class ChatStreamingController {
    private var currentStreamTask: Task<Void, Never>?
    private var currentStreamToken: UUID?
    private var typingDelayTask: Task<Void, Never>?
    private var receivedAnyAssistantOutput: Bool = false
    private var currentAssistantMessageId: UUID?
    private(set) var isStreaming: Bool = false
    private var responseIdBySession: [UUID: String] = [:]
    private var assistantMessageIdBySession: [UUID: UUID] = [:]
    private var currentStreamingSessionId: UUID?
    private var pendingRegenerationCount: Int = 0
    private var ghostNameBySession: [UUID: String] = [:]

    private weak var delegate: ChatStreamingDelegate?
    private var onCacheUpdate: (() -> Void)?
    private let backend = BackendService.shared

    /// When set, backend chat streaming will include `voice_agent` so the server can
    /// use the matching voice-agent system prompt (and Gemini voice flow where applicable).
    var activeVoiceAgentName: String? = nil

    init() {}

    func configure(delegate: ChatStreamingDelegate, onCacheUpdate: @escaping () -> Void) {
        self.delegate = delegate
        self.onCacheUpdate = onCacheUpdate
    }

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

    private func cancelCurrentStream() {
        currentStreamTask?.cancel()
        currentStreamTask = nil
        currentStreamToken = nil
    }

    /// Prefer the actively speaking voice agent; otherwise use the currently selected buddy name.
    /// This lets partner draft segments carry stable `ghost_name` metadata in normal (non-speak) chat, too.
    private func resolvedGhostNameForRequest() -> String? {
        let active = (activeVoiceAgentName ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !active.isEmpty { return active }

        let selected = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return selected.isEmpty ? nil : selected
    }

    func getAssistantMessageId(for sessionId: UUID) -> UUID? {
        assistantMessageIdBySession[sessionId]
    }

    func isCurrentlyStreaming(sessionId: UUID) -> Bool {
        currentStreamingSessionId == sessionId
    }

    func clearResponseId(for sessionId: UUID) {
        responseIdBySession.removeValue(forKey: sessionId)
    }

    func handleSessionPresented(_ id: UUID) {
        if let placeholderId = assistantMessageIdBySession[id] {
            currentAssistantMessageId = placeholderId
        } else {
            currentAssistantMessageId = nil
        }
    }

    func sendMessage(overrideText: String? = nil, isRegeneration: Bool = false, reuseMessageId: UUID? = nil) {
        guard let delegate else {
            print("[VoiceAgent] sendMessage — no delegate, returning")
            return
        }

        let trimmedMessage = (overrideText ?? delegate.inputText).trimmingCharacters(in: .whitespacesAndNewlines)
        let attachmentsToSend = delegate.pendingAttachments
        print("[VoiceAgent] sendMessage called — voiceAgent=\(activeVoiceAgentName ?? "nil") msgLen=\(trimmedMessage.count) isStreaming=\(isStreaming) isRegen=\(isRegeneration) override=\(overrideText != nil)")
        guard !(trimmedMessage.isEmpty && attachmentsToSend.isEmpty) else {
            print("[VoiceAgent] sendMessage — empty message & no attachments, returning (voiceAgent=\(activeVoiceAgentName ?? "nil"))")
            return
        }
        guard !isStreaming else {
            print("[VoiceAgent] sendMessage — BLOCKED: already streaming (voiceAgent=\(activeVoiceAgentName ?? "nil"))")
            return
        }

        let quoteText = delegate.replyQuoteText?.trimmingCharacters(in: .whitespacesAndNewlines)
        let hasQuote = !(quoteText ?? "").isEmpty
        let messageToSend: String = {
            if hasQuote, let qt = quoteText {
                return "> \(qt)\n\n\(trimmedMessage)"
            }
            return trimmedMessage
        }()
        let previewToSend: String = {
            if !trimmedMessage.isEmpty { return trimmedMessage }
            if attachmentsToSend.contains(where: { $0.isImage }) { return "Sent a photo." }
            return "Sent an attachment."
        }()

        var createdLocalSessionId: UUID? = nil
        if delegate.sessionId == nil {
            let localId = UUID()
            createdLocalSessionId = localId
            delegate.sessionId = localId
            delegate.setChatMessagesVMSessionId(localId)
            let currentTime = ISO8601DateFormatter().string(from: Date())
            NotificationCenter.default.post(name: .chatSessionCreated, object: nil, userInfo: [
                "sessionId": localId,
                "title": ChatSession.defaultTitle,
                "lastUsedISO8601": currentTime,
                "lastMessageContent": ""
            ])

            Task.detached {
                await ChatStore.shared.upsertSessions([
                    ChatSession(id: localId, title: ChatSession.defaultTitle, lastUsedISO8601: currentTime, lastMessageContent: "")
                ])
            }
        }

        guard let sid = delegate.sessionId else { return }
        if let friendId = delegate.selectedFriendUserId {
            delegate.persistFriendUserId(friendId, for: sid)
        }
        let friendToUse = delegate.selectedFriendUserId

        let userId: UUID? = {
            if let uid = AuthService.shared.currentUser?.id { return uid }
            if let raw = UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId),
               let uid = UUID(uuidString: raw) {
                return uid
            }
            return nil
        }()
        guard let userId else { return }

        var outboxAttachments: [OutboxAttachment] = []
        var segs: [[String: Any]] = []
        if hasQuote, let qt = quoteText {
            segs.append(["type": "quoted_reply", "text": qt])
        }
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

        let clientMessageId: UUID
        let pendingUpsertDto: BackendService.ChatMessageDTO
        let pendingVoiceMetadata: (ghostName: String, messageId: UUID)?

        if !isRegeneration {
            let persistedContent: String = {
                guard !attachmentsToSend.isEmpty || hasQuote else { return trimmedMessage }
                let obj: [String: Any] = ["_talktome": ["type": "segments", "segments": segs]]
                let data = (try? JSONSerialization.data(withJSONObject: obj)) ?? Data()
                return String(data: data, encoding: .utf8) ?? trimmedMessage
            }()

            let dto = BackendService.ChatMessageDTO(
                id: UUID(),
                user_id: userId,
                session_id: sid,
                role: "user",
                content: persistedContent,
                created_at: ISO8601DateFormatter().string(from: Date())
            )
            clientMessageId = dto.id
            pendingUpsertDto = dto

            var localMessage = ChatMessage(dto: dto, currentUserId: userId)
            let voiceGhostName: String? = {
                let trimmed = (self.activeVoiceAgentName ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }()
            if let ghostName = voiceGhostName {
                localMessage.isFromVoiceMode = true
                localMessage.ghostName = ghostName
            }
            pendingVoiceMetadata = voiceGhostName.map { (ghostName: $0, messageId: dto.id) }
            delegate.messages.append(localMessage)
            delegate.pendingOutgoingUserMessageId = clientMessageId
            onCacheUpdate?()

            NotificationCenter.default.post(name: .chatMessageSent, object: nil, userInfo: [
                "sessionId": sid,
                "messageContent": previewToSend
            ])
            NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)

            delegate.inputText = ""
            delegate.replyQuoteText = nil
            delegate.pendingAttachments = []
        } else {
            // Regeneration: persist the user message to local DB (the old one
            // was deleted) but don't add it to the messages array — the original
            // row is still visible in the UI for a seamless experience.
            // Reuse the original message ID so the UI and DB stay in sync on reload.
            let dto = BackendService.ChatMessageDTO(
                id: reuseMessageId ?? UUID(),
                user_id: userId,
                session_id: sid,
                role: "user",
                content: messageToSend,
                created_at: ISO8601DateFormatter().string(from: Date())
            )
            clientMessageId = dto.id
            pendingUpsertDto = dto
            pendingVoiceMetadata = nil
        }

        if NetworkMonitor.shared.isOnline == false {
            Task.detached {
                await ChatStore.shared.upsertMessages([pendingUpsertDto], voiceMetadata: pendingVoiceMetadata)
                await ChatOutboxProcessor.shared.enqueueChatMessage(
                    sessionId: sid,
                    serverSessionId: nil,
                    friendUserId: friendToUse,
                    messageId: clientMessageId,
                    message: messageToSend,
                    attachments: outboxAttachments,
                    quotedReply: quoteText
                )
            }
            delegate.isLoading = false
            delegate.isAssistantTyping = false
            isStreaming = false
            currentStreamingSessionId = nil
            return
        }

        delegate.isLoading = true
        delegate.isAssistantTyping = false
        delegate.thinkingText = ""
        delegate.thinkingTextDone = false
        receivedAnyAssistantOutput = false
        typingDelayTask?.cancel()
        typingDelayTask = Task { [weak self, weak delegate] in
            try? await Task.sleep(nanoseconds: 500_000_000)
            await MainActor.run {
                guard let self = self, let delegate = delegate else { return }
                if delegate.isLoading && !self.receivedAnyAssistantOutput {
                    delegate.isAssistantTyping = true
                }
            }
        }

        cancelCurrentStream()

        // Create and register the stream token synchronously so the old
        // stream's onFinish handler sees a non-nil currentStreamToken that
        // differs from its captured value, preventing it from clobbering
        // the new placeholder's currentAssistantMessageId.
        let streamToken = UUID()
        currentStreamToken = streamToken

        let isVoiceModeMessage = activeVoiceAgentName != nil
        let placeholderMessage = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
        delegate.messages.append(placeholderMessage)
        currentAssistantMessageId = placeholderMessage.id
        delegate.assistantScrollTargetId = placeholderMessage.id
        delegate.streamingScrollToken = 0

        if let sid = delegate.sessionId {
            assistantMessageIdBySession[sid] = placeholderMessage.id
        }
        onCacheUpdate?()

        let initialMessagesForStream = delegate.messages
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

        Task { [weak self, weak delegate] in
            guard let self = self, let delegate = delegate else { return }

            // Persist user message to DB before streaming starts so it's available
            // when the session rekey triggers a DB reload (prevents race condition).
            await ChatStore.shared.upsertMessages([pendingUpsertDto], voiceMetadata: pendingVoiceMetadata)

            guard let accessToken = await delegate.getAccessToken() else { return }

            let localSessionIdForSend = sid
            let createdLocalSessionIdForSend = createdLocalSessionId
            let requestSessionIdForStream: UUID? = (createdLocalSessionIdForSend != nil) ? nil : localSessionIdForSend

            await MainActor.run {
                self.isStreaming = true
                delegate.streamingWillStart()
            }
            await MainActor.run { self.currentStreamingSessionId = localSessionIdForSend }
            let bgName = "chat_stream_" + localSessionIdForSend.uuidString
            let bgTask: UIBackgroundTaskIdentifier? = Self.beginBackgroundTask(name: bgName) { [weak self, weak delegate] in
                guard let self = self else { return }
                Task { @MainActor in
                    self.cancelCurrentStream()
                    self.isStreaming = false
                    delegate?.isAssistantTyping = false
                    self.currentStreamingSessionId = nil
                    self.currentAssistantMessageId = nil
                    self.onCacheUpdate?()
                }
            }

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
                        await MainActor.run {
                            if !delegate.messages.isEmpty {
                                delegate.messages[delegate.messages.count - 1] = ChatMessage.text(
                                    "Error: Failed to upload attachment.",
                                    isFromUser: false,
                                    isFromVoiceMode: isVoiceModeMessage
                                )
                            }
                            delegate.isLoading = false
                            delegate.isAssistantTyping = false
                            self.isStreaming = false
                            self.currentStreamingSessionId = nil
                            self.onCacheUpdate?()
                        }
                        Self.endBackgroundTask(bgTask)
                        return
                    }
                }
            }

            let chatHistory = chatHistoryForStream

            var accumulated = ""
            var accumulatedThinking = ""
            var currentSegments: [MessageSegment] = []
            var streamSessionId: UUID? = requestSessionIdForStream ?? localSessionIdForSend
            let prevId: String? = {
                return self.responseIdBySession[localSessionIdForSend]
            }()
            let voiceAgentToSend: String? = {
                let trimmed = (self.activeVoiceAgentName ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }()
            let customInstructionsToSend: String? = {
                guard let agentName = voiceAgentToSend else { return nil }
                let key = agentName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
                guard !key.isEmpty else { return nil }
                let prompt = UserDefaults.standard.string(forKey: PreferenceKeys.buddyCustomPromptKey(key)) ?? ""
                let trimmed = prompt.trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }()

            let onEvent: (BackendService.StreamEvent) -> Void = { [weak self, weak delegate] event in
                guard let self = self, let delegate = delegate else { return }
                switch event {
                case .responseId(let rid):
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        if let sid = targetSid {
                            self.responseIdBySession[sid] = rid
                        }
                    }
                case .toolStart:
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        guard let sid = targetSid else { return }
                        if sid == delegate.sessionId {
                            var newMessages = delegate.messages
                            let idx: Int = {
                                if let id = self.currentAssistantMessageId,
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.currentAssistantMessageId = placeholder.id
                                if let sid = delegate.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            let updated = ChatMessage(
                                id: last.id,
                                segments: last.segments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: true,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            newMessages[idx] = updated
                            delegate.messages = newMessages
                            if let id = self.currentAssistantMessageId { delegate.assistantScrollTargetId = id }
                            delegate.streamingScrollToken &+= 1
                        } else {
                            var newMessages = delegate.getCachedMessages(for: sid) ?? []
                            let idx: Int = {
                                if let id = self.assistantMessageIdBySession[sid],
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
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
                                isToolLoading: true,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            newMessages[idx] = updated
                            delegate.setCachedMessages(newMessages, for: sid)
                        }
                    }
                case .toolArgs:
                    break
                case .toolDone:
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        guard let sid = targetSid else { return }
                        if sid == delegate.sessionId {
                            var newMessages = delegate.messages
                            let idx: Int = {
                                if let id = self.currentAssistantMessageId,
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.currentAssistantMessageId = placeholder.id
                                if let sid = delegate.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            let updated = ChatMessage(
                                id: last.id,
                                segments: last.segments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: false,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            newMessages[idx] = updated
                            delegate.messages = newMessages
                            if let id = self.currentAssistantMessageId { delegate.assistantScrollTargetId = id }
                            delegate.streamingScrollToken &+= 1
                        } else {
                            var newMessages = delegate.getCachedMessages(for: sid) ?? []
                            let idx: Int = {
                                if let id = self.assistantMessageIdBySession[sid],
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
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
                                isToolLoading: false,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            newMessages[idx] = updated
                            delegate.setCachedMessages(newMessages, for: sid)
                        }
                    }
                case .thinking(let text):
                    accumulatedThinking += text
                    Task { @MainActor in
                        if !self.receivedAnyAssistantOutput {
                            self.typingDelayTask?.cancel()
                            delegate.isAssistantTyping = true
                        }
                        delegate.thinkingText += text
                    }
                case .thinkingDone:
                    Task { @MainActor in
                        delegate.thinkingTextDone = true
                    }
                case .session(let sid):
                    streamSessionId = sid
                    Task { @MainActor in
                        let isRekey = createdLocalSessionIdForSend != nil
                            && delegate.sessionId == createdLocalSessionIdForSend
                            && sid != createdLocalSessionIdForSend

                        // Update session tracking state synchronously BEFORE any
                        // async work so that token/done handlers arriving during
                        // the DB rekey see streamSessionId == delegate.sessionId
                        // and update delegate.messages (not just the cache).
                        // Without this, the .done handler takes the wrong branch
                        // and never calls streamingDidFinish → TTS hangs.
                        if isRekey || delegate.sessionId == nil {
                            delegate.sessionId = sid
                            delegate.setChatMessagesVMSessionId(sid)
                        }
                        delegate.setCachedMessages(initialMessagesForStream, for: sid)
                        if let placeholderId = initialAssistantPlaceholderId {
                            self.assistantMessageIdBySession[sid] = placeholderId
                        }
                        self.currentStreamingSessionId = sid

                        if isRekey, let local = createdLocalSessionIdForSend {
                            await ChatStore.shared.rekeySession(oldId: local, newId: sid)
                            if let ghostName = self.ghostNameBySession[local] {
                                self.ghostNameBySession[sid] = ghostName
                            }
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
                        }
                    }
                case .token(let token):
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        guard let sid = targetSid else { return }
                        if !self.receivedAnyAssistantOutput {
                            self.receivedAnyAssistantOutput = true
                            self.typingDelayTask?.cancel()
                            delegate.isAssistantTyping = false
                        }
                        delegate.streamingDidReceiveToken(token)

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
                        if sid == delegate.sessionId {
                            var newMessages = delegate.messages
                            let idx: Int = {
                                if let id = self.currentAssistantMessageId,
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.currentAssistantMessageId = placeholder.id
                                if let sid = delegate.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            var updated = ChatMessage(
                                id: last.id,
                                segments: currentSegments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: last.isToolLoading,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            if isVoiceModeMessage {
                                updated.ghostName = self.ghostNameBySession[sid]
                            }
                            newMessages[idx] = updated
                            delegate.messages = newMessages
                            if let id = self.currentAssistantMessageId { delegate.assistantScrollTargetId = id }
                            delegate.streamingScrollToken &+= 1
                        } else {
                            var newMessages = delegate.getCachedMessages(for: sid) ?? []
                            let idx: Int = {
                                if let id = self.assistantMessageIdBySession[sid],
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.assistantMessageIdBySession[sid] = placeholder.id
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            var updated = ChatMessage(
                                id: last.id,
                                segments: currentSegments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: last.isToolLoading,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            if isVoiceModeMessage {
                                updated.ghostName = self.ghostNameBySession[sid]
                            }
                            newMessages[idx] = updated
                            delegate.setCachedMessages(newMessages, for: sid)
                        }
                    }
                case .partnerMessage(let text):
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        guard let sid = targetSid else { return }
                        if !self.receivedAnyAssistantOutput {
                            self.receivedAnyAssistantOutput = true
                            self.typingDelayTask?.cancel()
                            delegate.isAssistantTyping = false
                        }
                        let currentGhostName = self.ghostNameBySession[sid]
                        currentSegments.append(.partnerMessage(text: text, ghostName: currentGhostName))
                        if sid == delegate.sessionId {
                            var newMessages = delegate.messages
                            let idx: Int = {
                                if let id = self.currentAssistantMessageId,
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.currentAssistantMessageId = placeholder.id
                                if let sid = delegate.sessionId { self.assistantMessageIdBySession[sid] = placeholder.id }
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            var updated = ChatMessage(
                                id: last.id,
                                segments: currentSegments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: false,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            updated.ghostName = self.ghostNameBySession[sid]
                            newMessages[idx] = updated
                            delegate.messages = newMessages
                            if let id = self.currentAssistantMessageId { delegate.assistantScrollTargetId = id }
                            delegate.streamingScrollToken &+= 1
                        } else {
                            var newMessages = delegate.getCachedMessages(for: sid) ?? []
                            let idx: Int = {
                                if let id = self.assistantMessageIdBySession[sid],
                                   let i = newMessages.firstIndex(where: { $0.id == id }) { return i }
                                let placeholder = ChatMessage.text("", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                                newMessages.append(placeholder)
                                self.assistantMessageIdBySession[sid] = placeholder.id
                                return newMessages.count - 1
                            }()
                            let last = newMessages[idx]
                            var updated = ChatMessage(
                                id: last.id,
                                segments: currentSegments,
                                isFromUser: false,
                                timestamp: last.timestamp,
                                isToolLoading: false,
                                isFromVoiceMode: last.isFromVoiceMode
                            )
                            updated.ghostName = self.ghostNameBySession[sid]
                            newMessages[idx] = updated
                            delegate.setCachedMessages(newMessages, for: sid)
                        }
                    }
                case .done:
                    print("[VoiceAgent] SSE .done received — voiceAgent=\(voiceAgentToSend ?? "nil") accumulatedLen=\(accumulated.count) isVoiceMode=\(isVoiceModeMessage) currentActiveVoice=\(self.activeVoiceAgentName ?? "nil")")
                    let targetSid = streamSessionId ?? delegate.sessionId
                    if let sid = targetSid, sid != delegate.sessionId {
                        Task { @MainActor in
                            if let arr = delegate.getCachedMessages(for: sid) {
                                delegate.setCachedMessages(arr, for: sid)
                            }
                            self.assistantMessageIdBySession[sid] = nil
                            if self.currentStreamingSessionId == sid { self.currentStreamingSessionId = nil }
                        }
                        Self.endBackgroundTask(bgTask)
                        // Reconcile with server even when the user navigated away,
                        // so the AI response is persisted locally for next open.
                        if NetworkMonitor.shared.isOnline {
                            let tokenForSync = accessToken
                            Task.detached {
                                if let dtos = try? await BackendService.shared.fetchMessages(sessionId: sid, accessToken: tokenForSync) {
                                    await ChatStore.shared.reconcileMessagesWithServer(dtos, sessionId: sid)
                                }
                            }
                        }
                    } else {
                        let capturedRegenCount = self.pendingRegenerationCount
                        Task { @MainActor in
                            // Apply regeneration count to the completed message
                            if self.pendingRegenerationCount > 0,
                               let msgId = self.currentAssistantMessageId,
                               let idx = delegate.messages.firstIndex(where: { $0.id == msgId }) {
                                let count = self.pendingRegenerationCount
                                delegate.messages[idx].regenerationCount = count
                                self.pendingRegenerationCount = 0
                            }

                            // Save thinking summary to the completed message (in-memory for immediate display)
                            let thinkingSummary = accumulatedThinking.trimmingCharacters(in: .whitespacesAndNewlines)
                            if !thinkingSummary.isEmpty,
                               let msgId = self.currentAssistantMessageId,
                               let idx = delegate.messages.firstIndex(where: { $0.id == msgId }) {
                                delegate.messages[idx].thinkingSummary = thinkingSummary
                            }

                            // If the assistant message is empty (no segments), clear the
                            // stored response ID so the next request doesn't chain from a
                            // broken/empty response, which can cause repeated empty replies.
                            if let msgId = self.currentAssistantMessageId,
                               let idx = delegate.messages.firstIndex(where: { $0.id == msgId }) {
                                let msg = delegate.messages[idx]
                                let hasContent = msg.segments.contains(where: { seg in
                                    switch seg {
                                    case .text(let t): return !t.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                                    case .partnerMessage: return true
                                    default: return false
                                    }
                                })
                                if !hasContent, let sid = delegate.sessionId {
                                    self.responseIdBySession.removeValue(forKey: sid)
                                }
                            }

                            delegate.isLoading = false
                            delegate.isAssistantTyping = false
                            delegate.thinkingText = ""
                            delegate.thinkingTextDone = false
                            self.isStreaming = false
                            delegate.streamingDidFinish()
                            self.currentAssistantMessageId = nil
                            self.currentStreamingSessionId = nil
                            if let sid = delegate.sessionId {
                                delegate.setCachedMessages(delegate.messages, for: sid)
                            }
                        }
                        Self.endBackgroundTask(bgTask)

                        if NetworkMonitor.shared.isOnline {
                            if let sid = (streamSessionId ?? delegate.sessionId) {
                                let tokenForSync = accessToken
                                let capturedGhostName = self.ghostNameBySession[sid]
                                let capturedThinkingSummary = accumulatedThinking.trimmingCharacters(in: .whitespacesAndNewlines)
                                let skipReconciliation = isRegeneration
                                // Reconcile with server and persist local metadata
                                // immediately (no sleep).  The server has already saved
                                // the assistant message before sending the .done event,
                                // so the GET endpoint should return it right away.
                                // Previously this task slept 900ms, creating a window
                                // where app termination could lose voice metadata.
                                Task.detached {
                                    if let dtos = try? await BackendService.shared.fetchMessages(sessionId: sid, accessToken: tokenForSync) {
                                        if skipReconciliation {
                                            // After regeneration: upsert server messages so the new
                                            // assistant message is persisted to GRDB, but skip orphan
                                            // cleanup to avoid restoring messages pending server-side
                                            // deletion.
                                            await ChatStore.shared.upsertMessages(dtos)
                                        } else {
                                            await ChatStore.shared.reconcileMessagesWithServer(dtos, sessionId: sid)
                                        }
                                    }
                                    if let ghostName = capturedGhostName {
                                        await ChatStore.shared.setGhostNameForPartnerDrafts(sessionId: sid, ghostName: ghostName)
                                        if isVoiceModeMessage {
                                            await ChatStore.shared.setGhostNameForLastAssistant(sessionId: sid, ghostName: ghostName)
                                        }
                                    }
                                    if !capturedThinkingSummary.isEmpty {
                                        await ChatStore.shared.setThinkingSummaryForLastAssistant(sessionId: sid, summary: capturedThinkingSummary)
                                    }
                                    if capturedRegenCount > 0 {
                                        await ChatStore.shared.setRegenerationCountForLastAssistant(sessionId: sid, count: capturedRegenCount)
                                    }
                                    if isVoiceModeMessage {
                                        await ChatStore.shared.setVoiceModeForLastAssistant(sessionId: sid)
                                        if let ghostName = capturedGhostName {
                                            await ChatStore.shared.setVoiceMetadataForLastUserMessage(sessionId: sid, ghostName: ghostName)
                                        }
                                    }
                                    // Update the static cache with GRDB data (server-assigned UUIDs)
                                    // so returning to this chat doesn't cause an ID mismatch flash.
                                    let resolvedUserId: UUID? = await MainActor.run {
                                        AuthService.shared.currentUser?.id
                                            ?? UUID(uuidString: UserDefaults.standard.string(forKey: PreferenceKeys.currentUserId) ?? "")
                                    }
                                    if let userId = resolvedUserId {
                                        let refreshed = await ChatStore.shared.loadMessages(sessionId: sid, currentUserId: userId)
                                        if !refreshed.isEmpty {
                                            await MainActor.run {
                                                ChatMessagesViewModel.sharedMessagesCache[sid] = .init(messages: refreshed, lastLoaded: Date())
                                            }
                                        }
                                    }
                                }
                            }
                            Task.detached {
                                try? await Task.sleep(nanoseconds: 900_000_000)
                                NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)
                            }
                        }
                    }
                case .error(let message):
                    print("[VoiceAgent] SSE .error received: \(message) — voiceAgent=\(voiceAgentToSend ?? "nil")")
                    if message.contains("previous_response_not_found") || (message.contains("previous_response_id") && message.contains("not found")) {
                        Task { @MainActor in
                            let sid = streamSessionId ?? localSessionIdForSend
                            self.responseIdBySession.removeValue(forKey: sid)
                        }
                    }
                    Task { @MainActor in
                        let targetSid = streamSessionId ?? delegate.sessionId
                        if let sid = targetSid, sid != delegate.sessionId {
                            var newMessages = delegate.getCachedMessages(for: sid) ?? []
                            if !newMessages.isEmpty {
                                newMessages[newMessages.count - 1] = ChatMessage.text("Error: \(message)", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                            } else {
                                newMessages.append(ChatMessage.text("Error: \(message)", isFromUser: false, isFromVoiceMode: isVoiceModeMessage))
                            }
                            delegate.setCachedMessages(newMessages, for: sid)
                            self.assistantMessageIdBySession[sid] = nil
                        } else {
                            if !delegate.messages.isEmpty {
                                delegate.messages[delegate.messages.count - 1] = ChatMessage.text("Error: \(message)", isFromUser: false, isFromVoiceMode: isVoiceModeMessage)
                            }
                            delegate.isLoading = false
                            delegate.isAssistantTyping = false
                            delegate.thinkingText = ""
                            delegate.thinkingTextDone = false
                            self.isStreaming = false
                            self.currentStreamingSessionId = nil
                            delegate.streamingDidFinish()
                            self.onCacheUpdate?()
                        }
                        Self.endBackgroundTask(bgTask)
                    }
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
                            attachments: outboxAttachments,
                            quotedReply: quoteText
                        )
                    }
                }
            }

            let capturedStreamToken = streamToken
            let onFinish: () -> Void = { [weak self, weak delegate] in
                Task { @MainActor in
                    guard let self else { return }
                    // If a newer stream is already active, this is a stale
                    // onFinish from a cancelled stream — do nothing.
                    if self.currentStreamToken != nil && self.currentStreamToken != capturedStreamToken {
                        return
                    }
                    delegate?.isLoading = false
                    delegate?.isAssistantTyping = false
                    self.isStreaming = false
                    self.currentAssistantMessageId = nil
                    self.onCacheUpdate?()
                }
            }

            let ghostNameToSend: String? = self.resolvedGhostNameForRequest()
            let cleanedGhostName = ghostNameToSend?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            if !cleanedGhostName.isEmpty {
                self.ghostNameBySession[localSessionIdForSend] = cleanedGhostName
            }

            let deleteBeforeId: UUID? = isRegeneration ? reuseMessageId : nil
            if voiceAgentToSend != nil {
                print("[VoiceAgent] starting SSE stream — voiceAgent=\(voiceAgentToSend!) ghostName=\(ghostNameToSend ?? "nil") sessionId=\(requestSessionIdForStream?.uuidString ?? "nil") customInstructions=\(customInstructionsToSend != nil ? "yes" : "no")")
            }
            let quotedReplyToSend = quoteText
            let task = Task.detached { [streamToken] in
                let stream = BackendService.shared.streamChatMessage(
                    messageToSend,
                    sessionId: requestSessionIdForStream,
                    chatHistory: Array(chatHistory),
                    attachments: uploaded.isEmpty ? nil : uploaded,
                    accessToken: accessToken,
                    previousResponseId: prevId,
                    friendUserId: friendToUse,
                    messageId: clientMessageId,
                    voiceAgent: voiceAgentToSend,
                    ghostName: ghostNameToSend,
                    deleteBefore: deleteBeforeId,
                    customInstructions: customInstructionsToSend,
                    quotedReply: quotedReplyToSend
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
            }
        }
    }

    func stopGeneration() {
        cancelCurrentStream()
        delegate?.isLoading = false
        delegate?.isAssistantTyping = false
        delegate?.thinkingText = ""
        delegate?.thinkingTextDone = false
        isStreaming = false
        currentAssistantMessageId = nil
    }

    func regenerateResponse(forAssistantMessageId assistantMessageId: UUID) {
        guard let delegate else { return }
        guard !isStreaming else { return }

        // Find the assistant message
        guard let assistantIndex = delegate.messages.firstIndex(where: { $0.id == assistantMessageId }) else { return }

        // Track regeneration count from the original message
        let previousCount = delegate.messages[assistantIndex].regenerationCount

        // Find the preceding user message text and index
        var userText: String?
        var userIndex: Int?
        var userMessageId: UUID?
        for i in stride(from: assistantIndex - 1, through: 0, by: -1) {
            if delegate.messages[i].isFromUser {
                userIndex = i
                userMessageId = delegate.messages[i].id
                userText = delegate.messages[i].segments.compactMap { seg -> String? in
                    if case .text(let t) = seg { return t }
                    return nil
                }.joined()
                break
            }
        }

        guard let text = userText, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        guard let removeFrom = userIndex else { return }
        let anchorMessageId = userMessageId
        let sessionId = delegate.sessionId

        // Save current input state
        let savedInput = delegate.inputText
        let savedAttachments = delegate.pendingAttachments

        // Keep the user message in place; only remove the assistant message and everything after it
        let removeAfterUser = removeFrom + 1
        if removeAfterUser < delegate.messages.count {
            delegate.messages.removeSubrange(removeAfterUser...)
        }
        delegate.pendingAttachments = []
        onCacheUpdate?()

        // Store the regeneration count so we can apply it to the new message
        pendingRegenerationCount = previousCount + 1

        // Clear stale response ID for this session so it isn't sent with the retry
        if let sid = sessionId {
            responseIdBySession.removeValue(forKey: sid)
        }

        // Await deletes before re-sending to avoid a race where the delete
        // removes the newly created message.
        if let anchorId = anchorMessageId, let sid = sessionId {
            Task { [weak self, weak delegate] in
                // Delete the user message + everything after from DB (sendMessage will re-persist it)
                await ChatStore.shared.deleteMessagesAfter(messageId: anchorId, sessionId: sid, includeAnchor: true)

                if let accessToken = await AuthService.shared.getAccessToken() {
                    await BackendService.shared.deleteMessagesAfter(messageId: anchorId, sessionId: sid, accessToken: accessToken, includeAnchor: true)
                }

                await MainActor.run { [weak self, weak delegate] in
                    guard let self, let delegate else { return }
                    self.sendMessage(overrideText: text, isRegeneration: true, reuseMessageId: anchorId)
                    delegate.inputText = savedInput
                    delegate.pendingAttachments = savedAttachments
                }
            }
        } else {
            sendMessage(overrideText: text, isRegeneration: true, reuseMessageId: anchorMessageId)
            delegate.inputText = savedInput
            delegate.pendingAttachments = savedAttachments
        }
    }
}

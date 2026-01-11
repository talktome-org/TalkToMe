import Foundation
import SwiftUI
import UIKit
import Combine

@MainActor
class ChatViewModel: ObservableObject {

    @Published var messages: [ChatMessage] = []
    @Published var inputText: String = ""
    @Published var focusSnippet: String? = nil
    @Published var pendingAttachments: [PendingAttachment] = []
    @Published var focusTopMessageId: UUID? = nil
    @Published var assistantScrollTargetId: UUID? = nil
    @Published var streamingScrollToken: Int = 0
    @Published var sessionId: UUID? {
        didSet {
            if sessionId == nil {
                messages = []
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
    private var currentStreamHandleId: UUID?
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

    private nonisolated func debugLog(_ message: @autoclosure () -> String) {
#if DEBUG
        print(message())
#endif
    }

    init(sessionId: UUID? = nil) {
        self.sessionId = sessionId
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

        Task { [weak self] in
            guard let self = self else { return }
            await self.loadHistory()
            if !self.messages.isEmpty { self.initialJumpToken &+= 1 }
        }

        let partnerReceived = NotificationCenter.default.addObserver(
            forName: .partnerMessageReceived,
            object: nil,
            queue: .main
        ) { [weak self] note in
            Task { @MainActor in
                guard let self = self else { return }
                guard let notificationSessionId = note.userInfo?["sessionId"] as? UUID else { return }
                let currentSessionId = self.sessionId
                self.debugLog("[ChatVM] Received partnerMessageReceived for session \(notificationSessionId), current session: \(String(describing: currentSessionId))")
                if notificationSessionId == currentSessionId {
                    self.debugLog("[ChatVM] Refreshing messages for partner message in session \(notificationSessionId)")
                    await self.loadHistory(force: true)
                }
            }
        }
        observers.append(partnerReceived)
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

    func presentSession(_ id: UUID) async {
        await MainActor.run {
            self.sessionId = id
            self.startPartnerMessagePolling()
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

        var userSegments: [MessageSegment] = []
        if !trimmedMessage.isEmpty {
            userSegments.append(.text(trimmedMessage))
        }
        for att in attachmentsToSend {
            switch att.kind {
            case .image(let data, _):
                userSegments.append(.imageData(data))
            case .file(let data, let filename, _):
                userSegments.append(.fileData(name: filename, data: data))
            }
        }

        let userMessage = ChatMessage(segments: userSegments, isFromUser: true)
        messages.append(userMessage)

        let messageToSend: String = {
            if !trimmedMessage.isEmpty { return trimmedMessage }
            if attachmentsToSend.contains(where: { $0.isImage }) { return "User sent a photo." }
            return "User sent an attachment."
        }()
        inputText = ""
        pendingAttachments = []
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

        ChatStreamManager.shared.cancel(handleId: currentStreamHandleId)
        currentStreamHandleId = nil

        let placeholderMessage = ChatMessage.text("", isFromUser: false)
        messages.append(placeholderMessage)
        currentAssistantMessageId = placeholderMessage.id
        assistantScrollTargetId = placeholderMessage.id
        streamingScrollToken = 0

        if let sid = self.sessionId {
            assistantMessageIdBySession[sid] = placeholderMessage.id
        }
		updateCacheForCurrentSession()

        Task { [weak self] in
            guard let self = self else { return }
            guard let accessToken = await authService.getAccessToken() else {
                self.debugLog("[ChatVM] ACCESS_TOKEN: <nil>")
                return
            }

            // If we're sending attachments (or this is a brand new chat), we need a session ID before streaming.
            if self.sessionId == nil || !attachmentsToSend.isEmpty {
                _ = await self.ensureSessionId()
                if let sid = self.sessionId {
                    await MainActor.run {
                        self.assistantMessageIdBySession[sid] = placeholderMessage.id
                    }
                }
            }
            await MainActor.run { self.isStreaming = true }
            await MainActor.run { if let sid = self.sessionId { self.currentStreamingSessionId = sid } }
            let bgName = await MainActor.run { "chat_stream_" + (self.sessionId?.uuidString ?? "unknown") }
            let bgTask: UIBackgroundTaskIdentifier? = BackgroundTaskManager.shared.begin(name: bgName) { [weak self] in
                guard let self = self else { return }
                Task { @MainActor in
                    ChatStreamManager.shared.cancel(handleId: self.currentStreamHandleId)
                    self.isStreaming = false
                    self.isAssistantTyping = false
                    self.currentStreamingSessionId = nil
                    self.currentAssistantMessageId = nil
                    self.updateCacheForCurrentSession()
                }
            }
            self.debugLog("[ChatVM] stream starting (manager); sessionId=\(String(describing: self.sessionId)) messagesCount=\(self.messages.count)")

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
                        BackgroundTaskManager.shared.end(bgTask)
                        return
                    }
                }
            }

            let chatHistory = self.messages.dropLast(2).map { message in
                let plain = message.segments.compactMap { seg -> String? in
                    if case .text(let t) = seg { return t }
                    return nil
                }.joined()
                return ChatHistoryMessage(
                    role: message.isFromUser ? "user" : "assistant",
                    content: plain
                )
            }

            var accumulated = ""
            var currentSegments: [MessageSegment] = []
            var sawToolStart = false
            var sawPartnerMessage = false
            var eventCounter = 0
            var streamSessionId: UUID? = self.sessionId
            let (initialMessagesForStream, initialAssistantPlaceholderId): ([ChatMessage], UUID?) = await MainActor.run { (self.messages, self.currentAssistantMessageId) }

            let prevId: String? = {
                if let sid = self.sessionId { return self.responseIdBySession[sid] }
                return nil
            }()

            let handleId = ChatStreamManager.shared.startStream(
                params: ChatStreamManager.StartParams(
                    message: messageToSend,
                    sessionId: self.sessionId,
                    chatHistory: Array(chatHistory),
                    attachments: uploaded.isEmpty ? nil : uploaded,
                    accessToken: accessToken,
                    focusSnippet: self.focusSnippet,
                    previousResponseId: prevId
                ),
                onEvent: { [weak self] event in
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
                        Task { @MainActor in
                            if self.sessionId == nil { self.sessionId = sid }
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
                                NotificationCenter.default.post(name: .chatMessageSent, object: nil, userInfo: [
                                    "sessionId": sid,
                                    "messageContent": messageToSend
                                ])
                                NotificationCenter.default.post(name: .chatSessionsNeedRefresh, object: nil)
                            }
                            BackgroundTaskManager.shared.end(bgTask)
                        }
                    case .error(let message):
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
                            BackgroundTaskManager.shared.end(bgTask)
                        }
                    }
                },
                onFinish: { [weak self] in
                    Task { @MainActor in
                        self?.isLoading = false
                        self?.isAssistantTyping = false
                        self?.isStreaming = false
                        self?.currentAssistantMessageId = nil
                        self?.updateCacheForCurrentSession()
                    }
                }
            )

            await MainActor.run { self.currentStreamHandleId = handleId }
        }
    }

    func stopGeneration() {
        ChatStreamManager.shared.cancel(handleId: currentStreamHandleId)
        currentStreamHandleId = nil
        isLoading = false
        isStreaming = false
    }

    private func startPartnerMessagePolling() {
        refreshTimer?.invalidate()
        guard sessionId != nil else { return }

        refreshTimer = Timer.scheduledTimer(withTimeInterval: 3.0, repeats: true) { [weak self] _ in
            Task { @MainActor [weak self] in
                guard let self = self else { return }
                guard self.sessionId != nil else { return }
                guard !self.isStreaming else { return }

                self.debugLog("[ChatVM] Polling for new partner messages...")
                await self.loadHistory(force: true)
            }
        }
    }

    func sendToPartner(sessionsViewModel: ChatSessionsViewModel, customMessage: String? = nil) async {
        // Guard: must be linked to a partner
        let isLinked = (sessionsViewModel.partnerInfo?.linked == true) || UserDefaults.standard.bool(forKey: PreferenceKeys.partnerConnected) == true
        guard isLinked else {
            await MainActor.run {
                Haptics.notification(.error)
            }
            return
        }
        let resolved = await ensureSessionId()
        let sessionId = resolved ?? sessionsViewModel.activeSessionId
        guard let sid = sessionId else { return }
        do {
            guard let accessToken = await AuthService.shared.getAccessToken() else { return }
            let body = BackendService.PartnerRequestBody(message: customMessage ?? (self.inputText), session_id: sid)
            Task.detached {
                let stream = BackendService.shared.streamPartnerRequest(body, accessToken: accessToken)
                for await event in stream {
                    switch event {
                    case .toolStart(_): break
                    case .toolArgs(_): break
                    case .toolDone: break
                    case .token(_): break
                    case .done: return
                    case .error(let msg):
                        self.debugLog("[PartnerStream][iOS] error=\(msg)")
                        return
                    case .session(_): break
                    case .partnerMessage(_): break
                    case .responseId(_): break
                    }
                }
            }
        }
    }

    @MainActor
    func showPartnerAcceptanceInstant(sessionId targetSessionId: UUID, text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        let optimistic = ChatMessage(
            segments: [.partnerReceived(trimmed)],
            isFromUser: false,
            isToolLoading: false
        )

        if self.sessionId == targetSessionId {
            if let last = self.messages.last, last.partnerMessageContent == trimmed, last.isPartnerMessage {
                self.isLoadingHistory = false
                self.assistantScrollTargetId = last.id
                self.streamingScrollToken &+= 1
                updateCacheForCurrentSession()
                return
            }
            self.messages.append(optimistic)
            self.isLoadingHistory = false
            self.assistantScrollTargetId = optimistic.id
            self.streamingScrollToken &+= 1
            updateCacheForCurrentSession()
        } else {
            var entry = self.chatMessagesVM.getCachedMessages(for: targetSessionId) ?? []
            if let last = entry.last, last.partnerMessageContent == trimmed, last.isPartnerMessage {
                self.chatMessagesVM.setCachedMessages(entry, for: targetSessionId)
            } else {
                entry.append(optimistic)
                self.chatMessagesVM.setCachedMessages(entry, for: targetSessionId)
            }
        }
    }

    @MainActor
    func preloadPartnerMessageIntoCache(sessionId: UUID, text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        let partnerMessage = ChatMessage.partnerReceived(trimmed)
        var messages = self.chatMessagesVM.getCachedMessages(for: sessionId) ?? []

        let alreadyExists = messages.contains { msg in
            msg.partnerMessageContent == trimmed && msg.isPartnerMessage
        }

        if !alreadyExists {
            messages.append(partnerMessage)
            self.chatMessagesVM.setCachedMessages(messages, for: sessionId)
        }

        if self.sessionId == sessionId {
            self.messages = messages
            self.isLoadingHistory = false
        }
    }

    private func updateCacheForCurrentSession() {
        chatMessagesVM.updateCacheForCurrentSession(currentMessages: self.messages)
    }
}


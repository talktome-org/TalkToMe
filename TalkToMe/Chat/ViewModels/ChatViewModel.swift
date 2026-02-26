import Foundation
import SwiftUI
import UIKit
import Combine

@MainActor
class ChatViewModel: ObservableObject {
    @Published var messages: [ChatMessage] = []
    @Published var inputText: String = ""
    @Published var pendingAttachments: [PendingAttachment] = []
    @Published var pendingOutgoingUserMessageId: UUID? = nil
    @Published var assistantScrollTargetId: UUID? = nil
    @Published var streamingScrollToken: Int = 0
    @Published var selectedFriendUserId: UUID? = nil
    @Published var sessionId: UUID? {
        didSet {
            if sessionId == nil {
                messages = []
                selectedFriendUserId = nil
            }
        }
    }
    @Published var isLoading: Bool = false
    @Published var isLoadingHistory: Bool = false
    @Published var isAssistantTyping: Bool = false
    @Published var thinkingText: String = ""
    @Published var thinkingTextDone: Bool = false
    @Published var initialJumpToken: Int = 0
    @Published var regenerationCounts: [String: Int] = [:]
    @Published var isTTSSpeaking: Bool = false
    @Published var hasSpokenCurrentResponse: Bool = false
    private(set) var hasActiveStreamingCycle: Bool = false

    let voiceController = ChatVoiceModeController()
    let streamingController = ChatStreamingController()

    var isSpeakModeActive: Bool { voiceController.isSpeakModeActive }
    var isDictationRecording: Bool { voiceController.isDictationRecording }
    var dictationSTTService: DeepgramStreamingSTTService { voiceController.dictationSTTService }
    var speakSTTService: DeepgramStreamingSTTService { voiceController.speakSTTService }
    var elevenLabsStreamingTTS: ElevenLabsStreamingTTSService { voiceController.elevenLabsStreamingTTS }

    private var pendingInitialJump: Bool = false
    private var didReceiveFirstToken: Bool = false

    private let backend = BackendService.shared
    private let authService = AuthService.shared
    private let chatMessagesVM: ChatMessagesViewModel
    let partnerDrafts = PartnerDraftsViewModel()

    private var observers: [NSObjectProtocol] = []
    private var cancellables: Set<AnyCancellable> = []

    var isConnectedToFriendInThisChat: Bool {
        if selectedFriendUserId != nil { return true }
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

    func persistFriendUserId(_ friendUserId: UUID, for sessionId: UUID) {
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
                guard let self else { return }
                self.messages = newMessages
                // Auto-detect friend from incoming partner messages
                if self.selectedFriendUserId == nil, let sid = self.sessionId {
                    for msg in newMessages where msg.isFromPartnerUser {
                        if let senderId = msg.senderUserId {
                            self.selectedFriendUserId = senderId
                            self.persistFriendUserId(senderId, for: sid)
                            break
                        }
                    }
                }
                if self.pendingInitialJump, !newMessages.isEmpty {
                    self.pendingInitialJump = false
                    self.initialJumpToken &+= 1
                }
            }
            .store(in: &cancellables)

        chatMessagesVM.$isLoadingHistory
            .receive(on: DispatchQueue.main)
            .sink { [weak self] loading in
                self?.isLoadingHistory = loading
            }
            .store(in: &cancellables)


        voiceController.configure(
            delegate: self,
            streamingController: streamingController,
            isStreamingCheck: { [weak self] in
                self?.streamingController.isStreaming ?? false
            }
        )

        voiceController.objectWillChange
            .receive(on: DispatchQueue.main)
            .sink { [weak self] _ in
                self?.objectWillChange.send()
            }
            .store(in: &cancellables)

        voiceController.elevenLabsStreamingTTS.$isSpeaking
            .receive(on: DispatchQueue.main)
            .sink { [weak self] speaking in
                self?.isTTSSpeaking = speaking
                if speaking {
                    self?.hasSpokenCurrentResponse = true
                }
            }
            .store(in: &cancellables)

        streamingController.configure(delegate: self, onCacheUpdate: { [weak self] in
            guard let self else { return }
            self.chatMessagesVM.updateCacheForCurrentSession(currentMessages: self.messages)
        })

        pendingInitialJump = true
        Task { [weak self] in
            guard let self = self else { return }
            await self.loadHistory()
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
    }

    deinit {
        for ob in observers { NotificationCenter.default.removeObserver(ob) }
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

        let sid = await ensureSessionId()
        guard let sid else { return }
        persistFriendUserId(friendId, for: sid)

        // Optimistically persist sent state so it survives refreshes
        partnerDrafts.markPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
        objectWillChange.send()

        do {
            guard let accessToken = await authService.getAccessToken() else { return }
            _ = try await backend.sendPartnerMessage(
                message: trimmed,
                sessionId: sid,
                friendUserId: friendId,
                accessToken: accessToken
            )
        } catch {
            // Roll back on failure
            partnerDrafts.unmarkPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
            objectWillChange.send()
        }
    }

    func unsendPartnerDraft(_ text: String) async -> Bool {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return false }
        guard let sid = sessionId else { return false }
        guard let accessToken = await authService.getAccessToken() else { return false }

        // Optimistically persist unsent state so it survives refreshes
        partnerDrafts.unmarkPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
        objectWillChange.send()

        do {
            try await backend.unsendPartnerMessage(
                sessionId: sid,
                messageText: trimmed,
                accessToken: accessToken
            )

            // If no other sent drafts remain for this session, clear the friend linkage
            if !partnerDrafts.hasAnySentDraft(for: sid) {
                selectedFriendUserId = nil
                UserDefaults.standard.removeObject(forKey: Self.friendKey(for: sid))
            }

            return true
        } catch {
            // Roll back on failure — re-mark as sent
            partnerDrafts.markPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
            objectWillChange.send()
            return false
        }
    }

    func sendPartnerDraftViaSession(_ text: String) async {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        let sid = await ensureSessionId()
        guard let sid else { return }

        if let friendId = selectedFriendUserId {
            persistFriendUserId(friendId, for: sid)
        }

        // Optimistically persist sent state so it survives refreshes
        partnerDrafts.markPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
        objectWillChange.send()

        do {
            guard let accessToken = await authService.getAccessToken() else { return }
            _ = try await backend.sendPartnerMessage(
                message: trimmed,
                sessionId: sid,
                friendUserId: selectedFriendUserId,
                accessToken: accessToken
            )
        } catch {
            // Roll back on failure
            partnerDrafts.unmarkPartnerDraftAsSent(sessionId: sid, messageContent: trimmed)
            objectWillChange.send()
        }
    }

    func regenerateResponse(for assistantMessageId: UUID) {
        guard !streamingController.isStreaming else { return }

        guard let assistantIndex = messages.firstIndex(where: { $0.id == assistantMessageId }) else { return }

        // Find the preceding user message
        var userMessageText = ""
        var userMessageIndex: Int? = nil
        for i in stride(from: assistantIndex - 1, through: 0, by: -1) {
            if messages[i].isFromUser {
                userMessageText = messages[i].segments.compactMap { seg -> String? in
                    if case .text(let t) = seg { return t }
                    return nil
                }.joined()
                userMessageIndex = i
                break
            }
        }

        let trimmed = userMessageText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, let userIdx = userMessageIndex else { return }

        Haptics.impact(.medium)

        // Increment regeneration count keyed by user message text
        regenerationCounts[trimmed, default: 0] += 1

        // Truncate: remove user message and everything after it
        messages = Array(messages[..<userIdx])
        chatMessagesVM.updateCacheForCurrentSession(currentMessages: messages)

        // Clear stale response ID so backend doesn't reference deleted context
        if let sid = sessionId {
            streamingController.clearResponseId(for: sid)
        }

        // Re-send the user message (sendMessage recreates it + streams new response)
        streamingController.sendMessage(overrideText: trimmed)
    }

    func regenerationCount(forUserMessageText text: String) -> Int {
        regenerationCounts[text.trimmingCharacters(in: .whitespacesAndNewlines)] ?? 0
    }

    func presentSession(_ id: UUID) async {
        await MainActor.run {
            self.sessionId = id
            self.selectedFriendUserId = self.loadPersistedFriendUserId(for: id)
            self.streamingController.handleSessionPresented(id)
            self.isLoading = self.streamingController.isCurrentlyStreaming(sessionId: id)
            self.isAssistantTyping = false
            self.thinkingText = ""
            self.thinkingTextDone = false
            self.pendingInitialJump = true
        }

        await chatMessagesVM.presentSession(id)
        await MainActor.run {
            self.messages = chatMessagesVM.messages
            self.isLoadingHistory = chatMessagesVM.isLoadingHistory
            if self.pendingInitialJump, !self.messages.isEmpty {
                self.pendingInitialJump = false
                self.initialJumpToken &+= 1
            }
        }
    }
}


extension ChatViewModel: ChatVoiceModeDelegate {
    func getAccessToken() async -> String? {
        await authService.getAccessToken()
    }
}


extension ChatViewModel: ChatStreamingDelegate {
    func getCachedMessages(for sessionId: UUID) -> [ChatMessage]? {
        chatMessagesVM.getCachedMessages(for: sessionId)
    }

    func setCachedMessages(_ messages: [ChatMessage], for sessionId: UUID) {
        chatMessagesVM.setCachedMessages(messages, for: sessionId)
    }

    func setChatMessagesVMSessionId(_ id: UUID) {
        chatMessagesVM.sessionId = id
    }

    func streamingWillStart() {
        didReceiveFirstToken = false
        hasSpokenCurrentResponse = false
        hasActiveStreamingCycle = true

        guard isSpeakModeActive else {
            return
        }

        let voiceId = (voiceController.capturedVoiceId ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let voiceName = (voiceController.capturedVoiceName ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !voiceId.isEmpty else { return }

        Task { @MainActor in
            await elevenLabsStreamingTTS.start(voiceId: voiceId, voiceName: voiceName)
        }
    }

    func streamingDidReceiveToken(_ token: String) {
        if !didReceiveFirstToken {
            didReceiveFirstToken = true
            voiceController.notifyStreamingStarted()
        }

        guard isSpeakModeActive else { return }
        // Always buffer tokens — the TTS service's pendingText buffer
        // accumulates text even before the WebSocket is connected,
        // and flushes it once connected. Don't drop tokens.
        elevenLabsStreamingTTS.appendTextDelta(token)
    }

    func streamingDidFinish() {
        voiceController.notifyStreamingFinished()

        guard isSpeakModeActive else { return }
        // Always call finish — it will wait for connection if needed
        elevenLabsStreamingTTS.finish()
    }

}

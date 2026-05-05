import Foundation
import Combine

@MainActor
protocol ChatVoiceModeDelegate: AnyObject {
    var inputText: String { get set }
    var isLoading: Bool { get }
    var sessionId: UUID? { get }
    var selectedFriendUserId: UUID? { get }

    func getAccessToken() async -> String?
    func persistFriendUserId(_ friendUserId: UUID, for sessionId: UUID)
    func ensureSessionId() async -> UUID?
    func appendVoiceMessage(_ message: ChatMessage, for sessionId: UUID)
}

enum SpeakModePhase: Equatable {
    case idle
    case connecting
    case listening
    case processing
    case answering
}

@MainActor
final class ChatVoiceModeController: ObservableObject {

    @Published var isSpeakModeActive: Bool = false
    @Published var isDictationRecording: Bool = false
    @Published var speakModePhase: SpeakModePhase = .idle
    @Published var isSpeakMicMuted: Bool = false

    @Published var micLevel: CGFloat = 0
    @Published var speakerLevel: CGFloat = 0

    /// Live (interim) transcript of the user's current utterance during speak
    /// mode. Empty string when there's nothing to show. Cleared automatically
    /// when the worker commits the turn (the real ChatMessage takes over).
    @Published var liveUserTranscript: String = ""
    /// Live transcript of the agent's current utterance, word-by-word.
    @Published var liveAgentTranscript: String = ""

    /// Used by the dictation push-to-talk path (NOT speak mode). Speak mode
    /// is fully owned by `liveKitVoiceService` and goes over WebRTC to a
    /// LiveKit Agents worker.
    let dictationSTTService = DeepgramStreamingSTTService()
    let liveKitVoiceService = LiveKitVoiceService()

    private var isVoiceModeCapturing: Bool = false
    private var textBeforeRecording: String = ""
    private var pendingSendAfterVoiceStop: Bool = false
    private var sendAfterVoiceStopFallbackTask: Task<Void, Never>?
    private var cancellables: Set<AnyCancellable> = []

    private weak var delegate: ChatVoiceModeDelegate?
    private weak var streamingController: ChatStreamingController?

    init() {}

    func configure(
        delegate: ChatVoiceModeDelegate,
        streamingController: ChatStreamingController
    ) {
        self.delegate = delegate
        self.streamingController = streamingController
        setupSubscriptions()

        // Worker pushes a `bobo.message_committed` event over a LiveKit
        // text stream as soon as it persists each voice turn to the chat
        // DB. Inserting here gets the message into the UI ~50ms after the
        // turn ends, well before the next GET /messages refresh would.
        liveKitVoiceService.onMessageCommitted = { [weak self] committed in
            guard let self else { return }
            let segments: [MessageSegment] = committed.content.isEmpty
                ? [.text("")]
                : [.text(committed.content)]
            let message = ChatMessage(
                id: committed.id ?? UUID(),
                segments: segments,
                isFromUser: committed.role == "user",
                isFromPartnerUser: false,
                timestamp: Date(),
                isToolLoading: false,
                isFromVoiceMode: true,
                regenerationCount: 0
            )
            self.delegate?.appendVoiceMessage(message, for: committed.sessionId)
        }
    }

    private func setupSubscriptions() {
        dictationSTTService.$userTranscript
            .removeDuplicates()
            .throttle(for: .milliseconds(120), scheduler: DispatchQueue.main, latest: true)
            .sink { [weak self] transcript in
                guard let self else { return }
                guard self.isSpeakModeActive == false else { return }
                guard self.isVoiceModeCapturing else { return }
                guard self.dictationSTTService.isRecording else { return }
                let prefix = self.textBeforeRecording
                if prefix.isEmpty {
                    self.delegate?.inputText = transcript
                } else {
                    let separator = transcript.isEmpty ? "" : " "
                    self.delegate?.inputText = prefix + separator + transcript
                }
            }
            .store(in: &cancellables)

        dictationSTTService.$isRecording
            .removeDuplicates()
            .receive(on: DispatchQueue.main)
            .sink { [weak self] recording in
                guard let self else { return }
                self.isDictationRecording = recording

                if recording == false {
                    self.isVoiceModeCapturing = false
                }

                guard recording == false else { return }
                guard self.pendingSendAfterVoiceStop else { return }
                self.pendingSendAfterVoiceStop = false
                self.sendAfterVoiceStopFallbackTask?.cancel()
                self.sendAfterVoiceStopFallbackTask = nil
                self.streamingController?.sendMessage()
            }
            .store(in: &cancellables)

        // Mirror LiveKit room state onto our published phase / levels so the
        // existing UI bindings keep working.
        liveKitVoiceService.$phase
            .receive(on: DispatchQueue.main)
            .sink { [weak self] livekitPhase in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }
                self.speakModePhase = Self.mapLiveKitPhase(livekitPhase)
            }
            .store(in: &cancellables)

        liveKitVoiceService.$micLevel
            .receive(on: DispatchQueue.main)
            .sink { [weak self] level in
                self?.micLevel = CGFloat(level)
            }
            .store(in: &cancellables)

        liveKitVoiceService.$speakerLevel
            .receive(on: DispatchQueue.main)
            .sink { [weak self] level in
                self?.speakerLevel = CGFloat(level)
            }
            .store(in: &cancellables)

        liveKitVoiceService.$liveUserTranscript
            .receive(on: DispatchQueue.main)
            .assign(to: &$liveUserTranscript)

        liveKitVoiceService.$liveAgentTranscript
            .receive(on: DispatchQueue.main)
            .assign(to: &$liveAgentTranscript)
    }

    private static func mapLiveKitPhase(_ phase: LiveKitVoiceService.Phase) -> SpeakModePhase {
        switch phase {
        case .idle: return .idle
        case .connecting: return .connecting
        case .listening: return .listening
        case .speaking: return .answering
        case .error: return .idle
        }
    }

    func preconnectDictationSTTIfNeeded() async {
        guard NetworkMonitor.shared.isOnline else { return }
        guard dictationSTTService.isConnected == false else { return }
        guard await delegate?.getAccessToken() != nil else { return }
        await dictationSTTService.connect()
    }

    /// Kept for ChatView's preconnect warmup. With LiveKit, there is no
    /// session to preconnect — the room is created at `startSpeakMode()`.
    func preconnectSpeakServicesIfNeeded() async {
        // No-op: LiveKit rooms are short-lived and per-session. The agent is
        // dispatched on connect; warming up an empty room would be wasteful.
    }

    func startVoiceModePushToTalk() {
        if isSpeakModeActive {
            stopSpeakMode()
        }
        // Same audio session — never run dictation while a LiveKit room
        // is still in flight (covers the in-flight connect window too).
        if liveKitVoiceService.isConnected {
            Task { @MainActor [weak self] in
                await self?.liveKitVoiceService.disconnect()
            }
        }

        textBeforeRecording = (delegate?.inputText ?? "").trimmingCharacters(in: .whitespacesAndNewlines)

        isVoiceModeCapturing = true
        dictationSTTService.startRecording()

        Task { @MainActor [weak self] in
            guard let self else { return }
            guard NetworkMonitor.shared.isOnline else {
                self.dictationSTTService.lastError = "Voice Mode requires internet."
                self.dictationSTTService.stopRecording()
                self.isVoiceModeCapturing = false
                return
            }

            guard await self.delegate?.getAccessToken() != nil else {
                self.dictationSTTService.lastError = "Sign in to use the microphone."
                self.dictationSTTService.stopRecording()
                self.isVoiceModeCapturing = false
                return
            }

            if let sid = self.delegate?.sessionId, let friendId = self.delegate?.selectedFriendUserId {
                self.delegate?.persistFriendUserId(friendId, for: sid)
            }

            if self.dictationSTTService.isConnected == false {
                self.dictationSTTService.lastError = "Connecting…"
                await self.dictationSTTService.connect()
            }
            if self.dictationSTTService.isConnected {
                self.dictationSTTService.lastError = nil
            }
        }
    }

    func stopVoiceModePushToTalk() {
        dictationSTTService.stopRecording()
        isVoiceModeCapturing = false
        textBeforeRecording = ""
    }

    func startSpeakMode() {
        guard !isSpeakModeActive else { return }
        isSpeakModeActive = true
        speakModePhase = .connecting

        let storedVoiceName = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let voiceAgent = storedVoiceName.lowercased().isEmpty ? "mira" : storedVoiceName.lowercased()

        let customPromptKey = PreferenceKeys.buddyCustomPromptKey(voiceAgent)
        let customPrompt = (UserDefaults.standard.string(forKey: customPromptKey) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)

        // Cancel any in-flight chat SSE stream before voice takes over —
        // otherwise tokens keep mutating the chat while LiveKit drives audio.
        streamingController?.stopGeneration()

        Task { @MainActor [weak self] in
            guard let self else { return }
            guard NetworkMonitor.shared.isOnline else {
                self.isSpeakModeActive = false
                self.speakModePhase = .idle
                return
            }

            // Stop dictation if it was running — same audio session.
            if self.dictationSTTService.isRecording || self.dictationSTTService.isConnected {
                self.dictationSTTService.stopRecording()
                self.dictationSTTService.disconnect()
                self.isVoiceModeCapturing = false
            }

            // Ensure we have a chat session before opening the room so the
            // worker can persist voice turns into it.
            let sessionId = await self.delegate?.ensureSessionId()

            do {
                try await self.liveKitVoiceService.connect(
                    voiceAgent: voiceAgent,
                    ghostName: storedVoiceName.isEmpty ? nil : storedVoiceName,
                    customPrompt: customPrompt.isEmpty ? nil : customPrompt,
                    sessionId: sessionId
                )
            } catch {
                self.isSpeakModeActive = false
                self.speakModePhase = .idle
            }
        }
    }

    func toggleSpeakMicMute() {
        guard isSpeakModeActive else { return }
        isSpeakMicMuted.toggle()
        Task { @MainActor [weak self] in
            await self?.liveKitVoiceService.setMuted(self?.isSpeakMicMuted ?? false)
        }
    }

    func stopSpeakMode() {
        guard isSpeakModeActive else { return }
        isSpeakModeActive = false
        isSpeakMicMuted = false
        speakModePhase = .idle

        Task { @MainActor [weak self] in
            await self?.liveKitVoiceService.disconnect()
        }
    }

    func sendComposerMessage() {
        if dictationSTTService.isRecording {
            pendingSendAfterVoiceStop = true
            sendAfterVoiceStopFallbackTask?.cancel()
            sendAfterVoiceStopFallbackTask = nil

            dictationSTTService.stopRecording()

            sendAfterVoiceStopFallbackTask = Task { @MainActor [weak self] in
                guard let self else { return }
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                guard self.pendingSendAfterVoiceStop else { return }
                self.pendingSendAfterVoiceStop = false
                self.streamingController?.sendMessage()
            }
            return
        }
        streamingController?.sendMessage()
    }
}

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
}

enum SpeakModePhase: Equatable {
    case idle
    case listening
    case processing
    case answering
}

@MainActor
final class ChatVoiceModeController: ObservableObject {

    @Published var isSpeakModeActive: Bool = false
    @Published var isDictationRecording: Bool = false
    @Published var speakModePhase: SpeakModePhase = .idle

    @Published var micLevel: CGFloat = 0
    @Published var speakerLevel: CGFloat = 0

    let dictationSTTService = DeepgramStreamingSTTService()
    let speakSTTService = DeepgramStreamingSTTService()
    let elevenLabsStreamingTTS = ElevenLabsStreamingTTSService()

    private var isVoiceModeCapturing: Bool = false
    private var pendingSendAfterVoiceStop: Bool = false
    private var sendAfterVoiceStopFallbackTask: Task<Void, Never>?
    private var cancellables: Set<AnyCancellable> = []

    private weak var delegate: ChatVoiceModeDelegate?
    private weak var streamingController: ChatStreamingController?
    private var isStreamingCheck: () -> Bool = { false }

    private var ephemeralConversationHistory: [BackendService.ChatHistoryMessage] = []
    private var currentEphemeralStreamTask: Task<Void, Never>?
    private var isEphemeralStreaming: Bool = false
    private let backend = BackendService.shared
    private var speakModeVoiceId: String?
    private var speakModeVoiceName: String?

    init() {}

    func configure(
        delegate: ChatVoiceModeDelegate,
        streamingController: ChatStreamingController,
        isStreamingCheck: @escaping () -> Bool
    ) {
        self.delegate = delegate
        self.streamingController = streamingController
        self.isStreamingCheck = isStreamingCheck
        setupSubscriptions()
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
                self.delegate?.inputText = transcript
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


        speakSTTService.$lastFinalUtterance
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .sink { [weak self] transcript in
                guard let self else { return }
                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }
                if self.isSpeakModeActive {
                    if self.isEphemeralStreaming || self.elevenLabsStreamingTTS.isSpeaking {
                        if self.elevenLabsStreamingTTS.isSpeaking {
                            let mic = self.speakSTTService.spawnLevel
                            let spk = self.elevenLabsStreamingTTS.speakerLevel
                            let likelyEcho = mic <= max(0.28, spk + 0.14)
                            if likelyEcho {
                                return
                            }
                        }

                        self.cancelEphemeralStream()
                        self.elevenLabsStreamingTTS.cancel()
                    }
                    self.updatePhase(.processing)
                    self.sendEphemeralMessage(trimmed)
                }
            }
            .store(in: &cancellables)


        speakSTTService.$isUserSpeaking
            .removeDuplicates()
            .receive(on: DispatchQueue.main)
            .sink { [weak self] speaking in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }

                if speaking {
                    if self.speakModePhase != .processing {
                        self.updatePhase(.listening)
                    }
                } else {
                }
            }
            .store(in: &cancellables)

        elevenLabsStreamingTTS.$isSpeaking
            .removeDuplicates()
            .receive(on: DispatchQueue.main)
            .sink { [weak self] speaking in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }

                if speaking {
                    self.updatePhase(.answering)
                } else if self.speakModePhase == .answering {
                    self.updatePhase(.listening)
                }
            }
            .store(in: &cancellables)

        speakSTTService.$spawnLevel
            .receive(on: DispatchQueue.main)
            .sink { [weak self] level in
                self?.micLevel = level
            }
            .store(in: &cancellables)

        elevenLabsStreamingTTS.$speakerLevel
            .receive(on: DispatchQueue.main)
            .sink { [weak self] level in
                self?.speakerLevel = level
            }
            .store(in: &cancellables)
    }

    private func updatePhase(_ newPhase: SpeakModePhase) {
        guard speakModePhase != newPhase else { return }
        speakModePhase = newPhase
    }

    func notifyStreamingStarted() {
        guard isSpeakModeActive else { return }
        if speakModePhase == .processing {
            updatePhase(.answering)
        }
    }

    func notifyStreamingFinished() {
        guard isSpeakModeActive else { return }
        if !elevenLabsStreamingTTS.isSpeaking && speakModePhase == .answering {
            updatePhase(.listening)
        }
    }


    func preconnectDictationSTTIfNeeded() async {
        guard NetworkMonitor.shared.isOnline else { return }
        await MainActor.run {
            self.dictationSTTService.prewarmAudioForInstantStart()
        }
        guard dictationSTTService.isConnected == false else { return }
        guard await delegate?.getAccessToken() != nil else { return }
        await dictationSTTService.connect()
    }

    func startVoiceModePushToTalk() {
        if isSpeakModeActive {
            stopSpeakMode()
        }

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
    }


    func startSpeakMode() {
        let storedVoiceId = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !storedVoiceId.isEmpty else {
            let msg = "Voice mode is not available. Please select an ElevenLabs voice in Settings."
            elevenLabsStreamingTTS.lastError = msg
            speakSTTService.lastError = msg
            isSpeakModeActive = false
            updatePhase(.idle)
            return
        }

        speakModeVoiceId = storedVoiceId
        var name = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if name.isEmpty, let cached = VoicesCache.shared.cachedVoices {
            name = (cached.first(where: { $0.voice_id == storedVoiceId })?.name ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        }
        speakModeVoiceName = name

        isSpeakModeActive = true
        updatePhase(.listening)

        ephemeralConversationHistory = []

        Task { @MainActor [weak self] in
            guard let self else { return }
            guard NetworkMonitor.shared.isOnline else {
                self.isSpeakModeActive = false
                self.updatePhase(.idle)
                return
            }

            if self.dictationSTTService.isRecording || self.dictationSTTService.isConnected {
                self.dictationSTTService.stopRecording()
                self.dictationSTTService.disconnect()
                self.isVoiceModeCapturing = false
            }

            self.speakSTTService.transcriptAggregation = .perUtterance
            self.speakSTTService.startRecording()

            if self.speakSTTService.isConnected == false {
                await self.speakSTTService.connect(
                    config: .init(language: "en-US", model: "nova-3", endpointingMs: 800, sampleRateHz: 24_000)
                )
            }
        }
    }

    func stopSpeakMode() {
        isSpeakModeActive = false
        updatePhase(.idle)

        cancelEphemeralStream()

        ephemeralConversationHistory = []
        speakModeVoiceId = nil
        speakModeVoiceName = nil

        speakSTTService.stopRecording()
        speakSTTService.disconnect()
        elevenLabsStreamingTTS.cancel()
    }

    private func sendEphemeralMessage(_ message: String) {
        ephemeralConversationHistory.append(BackendService.ChatHistoryMessage(role: "user", content: message))

        currentEphemeralStreamTask?.cancel()
        currentEphemeralStreamTask = Task { @MainActor [weak self] in
            guard let self else { return }
            guard let accessToken = await self.delegate?.getAccessToken() else {
                self.updatePhase(.listening)
                return
            }

            self.isEphemeralStreaming = true
            var assistantResponse = ""
            var receivedFirstToken = false
            var didStartTTS = false

            let voiceId = (self.speakModeVoiceId ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            guard !voiceId.isEmpty else {
                let msg = "Voice mode is not available. Please select an ElevenLabs voice in Settings."
                self.elevenLabsStreamingTTS.lastError = msg
                self.speakSTTService.lastError = msg
                self.isEphemeralStreaming = false
                self.isSpeakModeActive = false
                self.updatePhase(.idle)
                return
            }

            await self.elevenLabsStreamingTTS.start(voiceId: voiceId)
            didStartTTS = self.elevenLabsStreamingTTS.isConnected
            if !didStartTTS {
                let msg = self.elevenLabsStreamingTTS.lastError ?? "Voice mode is not available right now."
                self.elevenLabsStreamingTTS.lastError = msg
                self.speakSTTService.lastError = msg
                self.isEphemeralStreaming = false
                self.isSpeakModeActive = false
                self.updatePhase(.idle)
                return
            }

            let stream = self.backend.streamEphemeralMessage(
                message,
                chatHistory: self.ephemeralConversationHistory.dropLast().map { $0 },
                accessToken: accessToken,
                voiceAgent: self.speakModeVoiceName
            )

            for await event in stream {
                guard !Task.isCancelled else {
                    break
                }

                switch event {
                case .token(let token):
                    if !receivedFirstToken {
                        receivedFirstToken = true
                        self.updatePhase(.answering)
                    }
                    assistantResponse += token
                    if didStartTTS {
                        self.elevenLabsStreamingTTS.appendTextDelta(token)
                    }

                case .done:
                    if !assistantResponse.isEmpty {
                        self.ephemeralConversationHistory.append(BackendService.ChatHistoryMessage(role: "assistant", content: assistantResponse))
                    }
                    if didStartTTS {
                        self.elevenLabsStreamingTTS.finish()
                    }

                case .error(let errorMsg):
                    _ = errorMsg

                default:
                    break
                }
            }

            self.isEphemeralStreaming = false

            guard !Task.isCancelled else { return }
            try? await Task.sleep(nanoseconds: 500_000_000)
            guard !Task.isCancelled else { return }

            if !self.elevenLabsStreamingTTS.isSpeaking && self.speakModePhase != .listening {
                self.updatePhase(.listening)
            }
        }
    }

    private func cancelEphemeralStream() {
        currentEphemeralStreamTask?.cancel()
        currentEphemeralStreamTask = nil
        isEphemeralStreaming = false
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

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

    let dictationSTTService = DeepgramStreamingSTTService()
    let speakSTTService = DeepgramStreamingSTTService()
    let elevenLabsStreamingTTS = ElevenLabsStreamingTTSService()

    private var isVoiceModeCapturing: Bool = false
    private var textBeforeRecording: String = ""
    private var pendingSendAfterVoiceStop: Bool = false
    private var sendAfterVoiceStopFallbackTask: Task<Void, Never>?
    private var cancellables: Set<AnyCancellable> = []

    private weak var delegate: ChatVoiceModeDelegate?
    private weak var streamingController: ChatStreamingController?
    private var isStreamingCheck: () -> Bool = { false }

    private var pendingSpeakAutoSendTask: Task<Void, Never>?
    private var isSpeakComposerLocked: Bool = false
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

        // Speak mode: lastFinalUtterance triggers sending a message.
        // STT stays active during TTS playback (SharedAudioEngine AEC removes echo),
        // so transcripts during TTS = real user speech = barge-in interrupt.
        speakSTTService.$lastFinalUtterance
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .sink { [weak self] transcript in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }
                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }

                // During TTS playback, require a longer utterance to confirm
                // it's real speech (safety net in case AEC leaks a short fragment).
                if self.elevenLabsStreamingTTS.isSpeaking {
                    guard trimmed.count >= 8 else { return }
                }

                isSpeakComposerLocked = true

                // If the assistant is currently answering, stop it (barge-in).
                if isStreamingCheck() || elevenLabsStreamingTTS.isSpeaking || (delegate?.isLoading == true) {
                    streamingController?.stopGeneration()
                    elevenLabsStreamingTTS.cancel()
                }

                updatePhase(.processing)

                pendingSpeakAutoSendTask?.cancel()
                pendingSpeakAutoSendTask = Task { @MainActor [weak self] in
                    guard let self else { return }
                    try? await Task.sleep(nanoseconds: 60_000_000) // 60ms
                    guard self.isSpeakModeActive else { return }
                    self.streamingController?.sendMessage(overrideText: trimmed)
                    self.isSpeakComposerLocked = false
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
                    // If TTS is playing and user starts speaking, cancel TTS immediately.
                    // Don't wait for the final transcript — stop the audio now so the
                    // user hears silence while they talk. The message will be sent later
                    // when lastFinalUtterance fires.
                    if self.elevenLabsStreamingTTS.isSpeaking || self.isStreamingCheck() {
                        self.streamingController?.stopGeneration()
                        self.elevenLabsStreamingTTS.cancel()
                    }
                    self.updatePhase(.listening)
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
                    // STT stays active — SharedAudioEngine voice processing
                    // handles echo cancellation at the hardware level.
                    self.updatePhase(.answering)
                } else {
                    if self.speakModePhase == .answering {
                        if !self.isStreamingCheck() {
                            self.updatePhase(.listening)
                        }
                    }
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

        var name = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if name.isEmpty, let cached = VoicesCache.shared.cachedVoices {
            name = (cached.first(where: { $0.voice_id == storedVoiceId })?.name ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        }
        speakModeVoiceName = name
        streamingController?.activeVoiceAgentName = name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : name

        isSpeakModeActive = true
        isSpeakComposerLocked = false
        pendingSpeakAutoSendTask?.cancel()
        pendingSpeakAutoSendTask = nil
        updatePhase(.connecting)
        delegate?.inputText = ""

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

            // Connect STT and preconnect TTS in parallel
            async let sttConnect: Void = {
                if self.speakSTTService.isConnected == false {
                    await self.speakSTTService.connect(
                        config: .init(language: "en-US", model: "nova-3", endpointingMs: 400, sampleRateHz: 24_000)
                    )
                }
            }()
            async let ttsPreconnect: Void = self.elevenLabsStreamingTTS.preconnect(voiceId: storedVoiceId)

            _ = await (sttConnect, ttsPreconnect)

            guard self.isSpeakModeActive else { return }
            self.speakSTTService.startRecording()
            self.updatePhase(.listening)
        }
    }

    func toggleSpeakMicMute() {
        guard isSpeakModeActive else { return }
        isSpeakMicMuted.toggle()
        if isSpeakMicMuted {
            speakSTTService.pauseCapture()
        } else {
            speakSTTService.resumeCapture()
        }
    }

    func stopSpeakMode() {
        isSpeakModeActive = false
        isSpeakMicMuted = false
        isSpeakComposerLocked = false
        pendingSpeakAutoSendTask?.cancel()
        pendingSpeakAutoSendTask = nil
        updatePhase(.idle)

        streamingController?.activeVoiceAgentName = nil
        streamingController?.stopGeneration()
        elevenLabsStreamingTTS.cancel()

        speakModeVoiceName = nil

        speakSTTService.stopRecording()
        speakSTTService.disconnect()

        // Tear down the shared audio engine
        SharedAudioEngine.shared.stop()
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

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

    private var pendingSpeakAutoSendTask: Task<Void, Never>?
    private var isSpeakComposerLocked: Bool = false
    private var speakModeVoiceName: String?

    /// Timestamp when TTS stopped speaking, used for echo cooldown
    private var ttsStoppedSpeakingAt: Date?
    /// Cooldown period after TTS stops to ignore potential echo (in seconds)
    private let echoCooldownSeconds: TimeInterval = 1.0

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

        speakSTTService.$userTranscript
            .removeDuplicates()
            .throttle(for: .milliseconds(120), scheduler: DispatchQueue.main, latest: true)
            .sink { [weak self] transcript in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }
                guard self.isSpeakComposerLocked == false else { return }
                guard self.speakSTTService.isRecording else { return }
                guard self.speakModePhase == .listening else { return }

                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }
                self.delegate?.inputText = transcript
            }
            .store(in: &cancellables)


        speakSTTService.$lastFinalUtterance
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .sink { [weak self] transcript in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }
                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }

                // If TTS is currently speaking, avoid auto-sending echo picked up from playback.
                if self.elevenLabsStreamingTTS.isSpeaking {
                    let mic = self.speakSTTService.spawnLevel
                    let spk = self.elevenLabsStreamingTTS.speakerLevel
                    let likelyEcho = mic <= max(0.30, spk + 0.12)
                    if likelyEcho {
                        return
                    }
                }

                // Also ignore transcriptions during cooldown period after TTS stops
                // (echo can still arrive after TTS finishes due to STT processing delay)
                if let stoppedAt = self.ttsStoppedSpeakingAt {
                    let elapsed = Date().timeIntervalSince(stoppedAt)
                    if elapsed < self.echoCooldownSeconds {
                        return
                    }
                }

                // Lock composer updates so STT resets don't wipe the message before send.
                isSpeakComposerLocked = true
                delegate?.inputText = trimmed

                // If the assistant is currently answering, stop it so the new user message can send.
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
                    self.streamingController?.sendMessage()
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
                    // Ignore speaking detection during echo cooldown period
                    if let stoppedAt = self.ttsStoppedSpeakingAt {
                        let elapsed = Date().timeIntervalSince(stoppedAt)
                        if elapsed < self.echoCooldownSeconds {
                            return
                        }
                    }

                    // Barge-in: stop the assistant as soon as the user starts speaking.
                    if self.isStreamingCheck() || self.elevenLabsStreamingTTS.isSpeaking || (self.delegate?.isLoading == true) {
                        if self.elevenLabsStreamingTTS.isSpeaking {
                            let mic = self.speakSTTService.spawnLevel
                            let spk = self.elevenLabsStreamingTTS.speakerLevel
                            let likelyEcho = mic <= max(0.30, spk + 0.12)
                            if likelyEcho {
                                return
                            }
                        }
                        self.streamingController?.stopGeneration()
                        self.elevenLabsStreamingTTS.cancel()
                    }
                    self.updatePhase(.listening)
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
                    // Clear the cooldown timestamp when TTS starts speaking
                    self.ttsStoppedSpeakingAt = nil
                    self.updatePhase(.answering)
                } else {
                    // Track when TTS stopped to filter out echo during cooldown
                    self.ttsStoppedSpeakingAt = Date()

                    if self.speakModePhase == .answering {
                        // Only transition to listening if the server isn't still streaming tokens.
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
        updatePhase(.listening)
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

            if self.speakSTTService.isConnected == false {
                await self.speakSTTService.connect(
                    config: .init(language: "en-US", model: "nova-3", endpointingMs: 800, sampleRateHz: 24_000)
                )
            }

            self.speakSTTService.startRecording()
        }
    }

    func stopSpeakMode() {
        isSpeakModeActive = false
        isSpeakComposerLocked = false
        pendingSpeakAutoSendTask?.cancel()
        pendingSpeakAutoSendTask = nil
        ttsStoppedSpeakingAt = nil
        updatePhase(.idle)

        streamingController?.activeVoiceAgentName = nil
        streamingController?.stopGeneration()
        elevenLabsStreamingTTS.cancel()

        speakModeVoiceName = nil

        speakSTTService.stopRecording()
        speakSTTService.disconnect()
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

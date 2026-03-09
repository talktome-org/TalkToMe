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
    private var bargeinEnergyCount: Int = 0
    private var speakModeVoiceName: String?
    private var speakModeVoiceId: String?

    /// Incremented each time speak mode is activated. Helps correlate logs across turns.
    private var speakSessionSeq: Int = 0
    /// Incremented for each utterance within a speak session.
    private var turnSeq: Int = 0
    /// Timestamp when the current speak session started.
    private var speakSessionStartTime: Date?

    private func voiceLog(_ msg: String) {
        let elapsed: String
        if let start = speakSessionStartTime {
            elapsed = String(format: "%.1fs", Date().timeIntervalSince(start))
        } else {
            elapsed = "-"
        }
        print("[VoiceAgent][S\(speakSessionSeq)/T\(turnSeq) +\(elapsed)] \(msg)")
    }

    var capturedVoiceId: String? { speakModeVoiceId }
    var capturedVoiceName: String? { speakModeVoiceName }

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
        // During TTS, mic audio is echo-gated (not sent to Deepgram).
        // Energy-based barge-in lifts the gate when real speech is detected,
        // so any transcript arriving here is genuine user speech.
        speakSTTService.$lastFinalUtterance
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .sink { [weak self] transcript in
                guard let self else { return }
                guard self.isSpeakModeActive else {
                    self.voiceLog("lastFinalUtterance received but speak mode INACTIVE — ignoring transcript: \"\(transcript.prefix(60))\"")
                    return
                }
                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }

                self.turnSeq += 1
                self.voiceLog("lastFinalUtterance: \"\(trimmed.prefix(80))\" — isStreaming=\(isStreamingCheck()) ttsIsSpeaking=\(elevenLabsStreamingTTS.isSpeaking) isLoading=\(delegate?.isLoading ?? false) sttConn=\(speakSTTService.isConnected)")

                isSpeakComposerLocked = true

                // If the assistant is currently answering, stop it (barge-in).
                if isStreamingCheck() || elevenLabsStreamingTTS.isSpeaking || (delegate?.isLoading == true) {
                    self.voiceLog("BARGE-IN: stopping generation & TTS — isStreaming=\(isStreamingCheck()) ttsSpeaking=\(elevenLabsStreamingTTS.isSpeaking) loading=\(delegate?.isLoading ?? false)")
                    streamingController?.stopGeneration()
                    elevenLabsStreamingTTS.cancel()
                }

                updatePhase(.processing)

                pendingSpeakAutoSendTask?.cancel()
                pendingSpeakAutoSendTask = Task { @MainActor [weak self] in
                    guard let self else { return }
                    try? await Task.sleep(nanoseconds: 60_000_000) // 60ms
                    guard self.isSpeakModeActive else {
                        self.voiceLog("ABORT auto-send — speak mode became inactive during 60ms delay")
                        return
                    }
                    let voiceAgent = self.streamingController?.activeVoiceAgentName
                    self.voiceLog("auto-sending message: \"\(trimmed.prefix(80))\" voiceAgent=\(voiceAgent ?? "nil") sttConn=\(self.speakSTTService.isConnected) ttsConn=\(self.elevenLabsStreamingTTS.isConnected)")
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
                    self.updatePhase(.listening)
                }
            }
            .store(in: &cancellables)

        elevenLabsStreamingTTS.$isSpeaking
            .removeDuplicates()
            .receive(on: DispatchQueue.main)
            .sink { [weak self] speaking in
                guard let self else { return }
                self.voiceLog("TTS isSpeaking=\(speaking) — active=\(self.isSpeakModeActive) phase=\(self.speakModePhase) streaming=\(self.isStreamingCheck()) sttConn=\(self.speakSTTService.isConnected) sttRec=\(self.speakSTTService.isRecording)")
                guard self.isSpeakModeActive else { return }

                if speaking {
                    // Gate mic audio so Deepgram never receives echo.
                    // Mic levels still update for energy-based barge-in.
                    self.speakSTTService.isEchoGated = true
                    self.bargeinEnergyCount = 0
                    self.updatePhase(.answering)
                } else {
                    self.speakSTTService.isEchoGated = false
                    self.bargeinEnergyCount = 0
                    if self.speakModePhase == .answering {
                        if !self.isStreamingCheck() {
                            self.updatePhase(.listening)
                        } else {
                            self.voiceLog("TTS stopped but still streaming — staying in .answering")
                        }
                    }
                }
            }
            .store(in: &cancellables)

        speakSTTService.$spawnLevel
            .receive(on: DispatchQueue.main)
            .sink { [weak self] level in
                guard let self else { return }
                self.micLevel = level

                // Energy-based barge-in: while echo gate is active (TTS playing),
                // mic audio isn't sent to Deepgram, but spawnLevel still updates
                // from the AEC-processed input. Real speech punches through AEC
                // much louder than residual echo. When we detect sustained energy
                // above threshold, lift the echo gate and cancel TTS so Deepgram
                // picks up the user's voice.
                guard self.speakSTTService.isEchoGated else {
                    self.bargeinEnergyCount = 0
                    return
                }
                if level > 0.25 {
                    self.bargeinEnergyCount += 1
                    if self.bargeinEnergyCount >= 5 {
                        self.bargeinEnergyCount = 0
                        self.speakSTTService.isEchoGated = false
                        self.streamingController?.stopGeneration()
                        self.elevenLabsStreamingTTS.cancel()
                        self.updatePhase(.listening)
                    }
                } else {
                    self.bargeinEnergyCount = 0
                }
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
        let oldPhase = speakModePhase
        speakModePhase = newPhase
        voiceLog("phase: \(oldPhase) → \(newPhase) | sttConn=\(speakSTTService.isConnected) sttRec=\(speakSTTService.isRecording) ttsConn=\(elevenLabsStreamingTTS.isConnected) ttsSpeaking=\(elevenLabsStreamingTTS.isSpeaking)")
    }

    func notifyStreamingStarted() {
        voiceLog("notifyStreamingStarted — active=\(isSpeakModeActive) phase=\(speakModePhase) sttConn=\(speakSTTService.isConnected) ttsConn=\(elevenLabsStreamingTTS.isConnected)")
        guard isSpeakModeActive else {
            voiceLog("notifyStreamingStarted — SKIPPED (speak mode inactive)")
            return
        }
        if speakModePhase == .processing {
            updatePhase(.answering)
        }
    }

    func notifyStreamingFinished() {
        voiceLog("notifyStreamingFinished — active=\(isSpeakModeActive) phase=\(speakModePhase) ttsSpeaking=\(elevenLabsStreamingTTS.isSpeaking) ttsConn=\(elevenLabsStreamingTTS.isConnected) sttConn=\(speakSTTService.isConnected) sttRec=\(speakSTTService.isRecording)")
        guard isSpeakModeActive else {
            voiceLog("notifyStreamingFinished — SKIPPED (speak mode inactive)")
            return
        }
        if !elevenLabsStreamingTTS.isSpeaking && speakModePhase == .answering {
            updatePhase(.listening)
        }
    }

    func preconnectDictationSTTIfNeeded() async {
        guard NetworkMonitor.shared.isOnline else { return }
        guard dictationSTTService.isConnected == false else { return }
        guard await delegate?.getAccessToken() != nil else { return }
        await dictationSTTService.connect()
    }

    func preconnectSpeakServicesIfNeeded() async {
        guard NetworkMonitor.shared.isOnline else { return }
        guard await delegate?.getAccessToken() != nil else { return }

        if speakSTTService.isConnected == false {
            await speakSTTService.connect(
                config: .init(language: "multi", model: "nova-3", endpointingMs: 300, sampleRateHz: 24_000)
            )
        }

        var storedVoiceId = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let storedVoiceName = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        if let resolved = VoicesCache.shared.resolvedVoiceId(storedId: storedVoiceId, storedName: storedVoiceName) {
            storedVoiceId = resolved
        }
        guard !storedVoiceId.isEmpty else { return }
        await elevenLabsStreamingTTS.preconnect(voiceId: storedVoiceId, voiceName: storedVoiceName)
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
        speakSessionSeq += 1
        turnSeq = 0
        speakSessionStartTime = Date()
        voiceLog("startSpeakMode() — beginning session")

        var storedVoiceId = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let storedVoiceName = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)

        // Validate / refresh stale voice ID against cached voice list
        if let resolved = VoicesCache.shared.resolvedVoiceId(storedId: storedVoiceId, storedName: storedVoiceName) {
            storedVoiceId = resolved
        }

        guard !storedVoiceId.isEmpty else {
            let msg = "Voice mode is not available. Please select an ElevenLabs voice in Settings."
            elevenLabsStreamingTTS.lastError = msg
            speakSTTService.lastError = msg
            isSpeakModeActive = false
            updatePhase(.idle)
            return
        }

        var name = storedVoiceName
        if name.isEmpty, let cached = VoicesCache.shared.cachedVoices {
            name = (cached.first(where: { $0.voice_id == storedVoiceId })?.name ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        }
        speakModeVoiceName = name
        speakModeVoiceId = storedVoiceId
        streamingController?.activeVoiceAgentName = name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : name

        isSpeakModeActive = true
        isSpeakComposerLocked = false
        pendingSpeakAutoSendTask?.cancel()
        pendingSpeakAutoSendTask = nil
        updatePhase(.connecting)
        delegate?.inputText = ""

        // Lock the audio session open for the duration of speak mode so that
        // transient idle checks (e.g. between TTS turns, ghost video configure)
        // can't deactivate it and cycle the mic.
        SharedAudioEngine.shared.holdVoiceSession()

        // Kick off audio session + engine setup on a background thread so the
        // main thread isn't blocked (~200-500 ms for voice processing init).
        // installMicTap() will sync-wait for this to finish before installing
        // the tap, so the mic is ready as soon as the engine is.
        SharedAudioEngine.shared.ensureRunningAsync()

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

            // Connect STT and preconnect TTS in parallel.
            // Phase stays .connecting (gray waveform) until both are ready.
            let voiceIdForTTS = storedVoiceId
            let voiceNameForTTS = name
            async let sttConnect: Void = {
                if self.speakSTTService.isConnected == false {
                    await self.speakSTTService.connect(
                        config: .init(language: "multi", model: "nova-3", endpointingMs: 300, sampleRateHz: 24_000)
                    )
                }
            }()
            async let ttsPreconnect: Void = self.elevenLabsStreamingTTS.preconnect(voiceId: voiceIdForTTS, voiceName: voiceNameForTTS)

            _ = await (sttConnect, ttsPreconnect)

            self.voiceLog("parallel connect done — sttConn=\(self.speakSTTService.isConnected) sttErr=\(self.speakSTTService.lastError ?? "nil") ttsConn=\(self.elevenLabsStreamingTTS.isConnected) ttsErr=\(self.elevenLabsStreamingTTS.lastError ?? "nil") stillActive=\(self.isSpeakModeActive)")
            guard self.isSpeakModeActive else {
                self.voiceLog("speak mode deactivated during connect — aborting")
                return
            }
            if self.speakSTTService.isConnected {
                self.speakSTTService.lastError = nil
            } else {
                self.voiceLog("WARNING: STT failed to connect — voice input will not work!")
            }
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
        voiceLog("stopSpeakMode() called — phase=\(speakModePhase) ttsIsSpeaking=\(elevenLabsStreamingTTS.isSpeaking) ttsConn=\(elevenLabsStreamingTTS.isConnected) sttConn=\(speakSTTService.isConnected) sttRec=\(speakSTTService.isRecording) isStreaming=\(isStreamingCheck()) voiceAgent=\(streamingController?.activeVoiceAgentName ?? "nil")")
        // Capture a stack trace to identify who is calling stopSpeakMode
        Thread.callStackSymbols.prefix(10).forEach { voiceLog("stopSpeakMode caller: \($0)") }
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
        speakModeVoiceId = nil

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

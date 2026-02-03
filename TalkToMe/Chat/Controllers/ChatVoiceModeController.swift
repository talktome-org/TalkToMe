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

@MainActor
final class ChatVoiceModeController: ObservableObject {

    @Published var isSpeakModeActive: Bool = false
    @Published var isDictationRecording: Bool = false

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
        dictationSTTService.$userTranscript  // Dictation mic: stream partial transcription into the composer live
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

        dictationSTTService.$isRecording  // Dictation: if the user pressed send while recording, send when the mic stops
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


        speakSTTService.$lastFinalUtterance  // Speak mode: when an utterance completes, barge-in and send it
            .compactMap { $0 }
            .receive(on: DispatchQueue.main)
            .sink { [weak self] transcript in
                guard let self else { return }
                let trimmed = transcript.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !trimmed.isEmpty else { return }
                if self.isSpeakModeActive {
                    if (self.delegate?.isLoading ?? false) || self.isStreamingCheck() || self.elevenLabsStreamingTTS.isSpeaking {
                        self.streamingController?.stopGeneration()
                        self.elevenLabsStreamingTTS.cancel()
                    }
                    self.delegate?.inputText = trimmed
                    self.streamingController?.sendMessage()
                }
            }
            .store(in: &cancellables)


        speakSTTService.$isUserSpeaking  // Speak mode: barge-in immediately when we detect user speech
            .removeDuplicates()
            .receive(on: DispatchQueue.main)
            .sink { [weak self] speaking in
                guard let self else { return }
                guard self.isSpeakModeActive else { return }
                guard speaking else { return }
                if (self.delegate?.isLoading ?? false) || self.isStreamingCheck() || self.elevenLabsStreamingTTS.isSpeaking {
                    self.streamingController?.stopGeneration()
                }
            }
            .store(in: &cancellables)
    }


    // MARK: - Voice Mode (Dictation)

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


    // MARK: - Speak Mode

    func startSpeakMode() {
        Task { @MainActor [weak self] in
            guard let self else { return }
            guard NetworkMonitor.shared.isOnline else { return }

            if self.dictationSTTService.isRecording || self.dictationSTTService.isConnected {
                self.dictationSTTService.stopRecording()
                self.dictationSTTService.disconnect()
                self.isVoiceModeCapturing = false
            }

            let sid = await self.delegate?.ensureSessionId()
            guard let sid else { return }
            if let friendId = self.delegate?.selectedFriendUserId {
                self.delegate?.persistFriendUserId(friendId, for: sid)
            }

            self.speakSTTService.transcriptAggregation = .perUtterance
            self.isSpeakModeActive = true
            self.speakSTTService.startRecording()

            if self.speakSTTService.isConnected == false {
                await self.speakSTTService.connect(
                    config: .init(language: "en-US", model: "nova-3", endpointingMs: 550, sampleRateHz: 24_000)
                )
            }
        }
    }

    func stopSpeakMode() {
        isSpeakModeActive = false
        speakSTTService.stopRecording()
        speakSTTService.disconnect()
        elevenLabsStreamingTTS.cancel()
        streamingController?.stopGeneration()
    }


    // MARK: - Send Handling

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

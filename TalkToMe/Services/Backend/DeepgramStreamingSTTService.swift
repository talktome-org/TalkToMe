import AVFoundation
import Foundation

/// iOS client for TalkToMe's `/speech/stt/stream` WebSocket (backend proxies to Deepgram streaming STT).
///
/// Responsibilities:
/// - Connect/authenticate with Supabase access token
/// - Stream microphone audio as PCM16 @ 24kHz mono (binary frames)
/// - Maintain a continuously updating transcript with interim revisions + finalization on pauses
final class DeepgramStreamingSTTService: ObservableObject, @unchecked Sendable {
    struct Config: Equatable {
        /// Deepgram language code (e.g. "en-US"). Backend defaults to "en-US".
        var language: String = "en-US"
        /// Deepgram model (e.g. "nova-3"). Backend defaults to "nova-3".
        var model: String = "nova-3"
        /// Milliseconds of silence to consider an utterance boundary.
        var endpointingMs: Int = 400
        /// Audio sample rate we stream. Must match what we encode on-device.
        var sampleRateHz: Int = 24_000
    }

    enum TranscriptAggregation {
        /// Keep a running transcript across multiple utterances (dictation).
        case accumulate
        /// Keep only the active utterance (voice agent / speak mode).
        case perUtterance
    }

    @Published var isConnected: Bool = false
    @Published var isRecording: Bool = false
    @Published var lastError: String?

    /// Quick waveform driver for any future mic UI (optional).
    @Published var spawnLevel: CGFloat = 0

    /// The running transcript shown in the composer (committed + current interim).
    @Published var userTranscript: String = ""

    /// Best-effort; derived from interim activity and `speech_final`.
    @Published var isUserSpeaking: Bool = false

    /// Emits the most recently finalized utterance (when Deepgram reports an utterance boundary).
    @Published var lastFinalUtterance: String?

    private let urlSession: URLSession
    private var wsTask: URLSessionWebSocketTask?

    private var config: Config = .init()

    /// Controls how `userTranscript` is presented.
    /// Set this before calling `startRecording()`.
    var transcriptAggregation: TranscriptAggregation = .accumulate

    // Audio (mic capture only)
    private var audioEngine: AVAudioEngine?
    private var inputConverter: AVAudioConverter?

    private let audioQueue = DispatchQueue(label: "talktome.deepgramSTT.audio")
    private let wsSendQueue = DispatchQueue(label: "talktome.deepgramSTT.wsSend")

    // Best-effort "instant mic": prewarm audio session/engine so start/stop are responsive.
    // Mutate only on `audioQueue`.
    private var didPrewarmAudio: Bool = false

    // When the user taps the mic, we want to start capturing instantly even if the websocket
    // handshake hasn't completed yet. Buffer a small amount of audio and flush once connected.
    private var bufferedAudioChunks: [Data] = []
    private var bufferedAudioBytes: Int = 0
    private let maxBufferedAudioBytes: Int = 480_000 // ~10s @ 24kHz PCM16 mono (≈48KB/s)

    // Transcript aggregation state (mutate only on MainActor).
    private var committedParts: [String] = []
    private var currentInterim: String = ""
    private var currentUtteranceFinalParts: [String] = []

    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }

    // MARK: - Public API

    @MainActor
    func connect(config: Config = .init()) async {
        self.config = config
        self.lastError = nil
        self.isUserSpeaking = false
        self.lastFinalUtterance = nil
        self.userTranscript = ""
        self.committedParts = []
        self.currentInterim = ""
        self.currentUtteranceFinalParts = []

        guard let token = await AuthService.shared.getAccessToken() else {
            self.lastError = "Not authenticated."
            return
        }

        do {
            try await openWebSocket(token: token)
            self.isConnected = true
            self.receiveLoop()
            self.flushBufferedAudioIfPossible()
        } catch {
            let msg = "STT connection failed: \(error.localizedDescription)"
            self.lastError = msg
#if DEBUG
            print("🎙️ [DeepgramSTT] \(msg)")
#endif
            self.isConnected = false
        }
    }

    @MainActor
    func disconnect() {
        stopRecordingIfNeeded()
        isConnected = false
        clearBufferedAudio()
        wsTask?.cancel(with: .goingAway, reason: nil)
        wsTask = nil
    }

    @MainActor
    func startRecording() {
        lastError = nil
        // Fast path: if permission is already granted, start immediately (no async hop).
        if #available(iOS 17.0, *) {
            let perm = AVAudioApplication.shared.recordPermission
            if perm == .granted {
                startRecordingAfterPermission()
                return
            }
            if perm == .denied {
                lastError = "Microphone permission denied. Enable it in Settings."
                return
            }
        } else {
            let perm = AVAudioSession.sharedInstance().recordPermission
            if perm == .granted {
                startRecordingAfterPermission()
                return
            }
            if perm == .denied {
                lastError = "Microphone permission denied. Enable it in Settings."
                return
            }
        }

        // Permission is undetermined; request it asynchronously.
        Task { @MainActor [weak self] in
            guard let self else { return }
            let granted = await self.requestMicrophonePermissionIfNeeded()
            guard granted else {
                self.lastError = "Microphone permission denied. Enable it in Settings."
                return
            }
            self.startRecordingAfterPermission()
        }
    }

    /// Prepare audio session + engine ahead of time to minimize start/stop latency.
    /// Does not prompt for permission (silent no-op if not granted).
    @MainActor
    func prewarmAudioForInstantStart() {
        let hasMicPermission: Bool = {
            if #available(iOS 17.0, *) {
                return AVAudioApplication.shared.recordPermission == .granted
            } else {
                return AVAudioSession.sharedInstance().recordPermission == .granted
            }
        }()
        guard hasMicPermission else { return }

        audioQueue.async { [weak self] in
            guard let self else { return }
            guard self.didPrewarmAudio == false else { return }
            // Don't surface errors here; this is best-effort.
            _ = self.configureAudioSessionForVoiceLocked(reportErrors: false)
            _ = self.ensureEngineRunning(reportErrors: false)
            self.didPrewarmAudio = true
        }
    }

    @MainActor
    func stopRecording() {
        // Flip UI state immediately; cleanup happens on the audio queue.
        isRecording = false
        stopRecordingIfNeeded()
        // Try to flush any buffered frames before we finalize.
        flushBufferedAudioIfPossible()
        // Best-effort flush to Deepgram (backend forwards a Finalize control message).
        sendJSONEvent(["type": "finalize"])
        clearBufferedAudio()
    }

    // MARK: - WebSocket

    @MainActor
    private func openWebSocket(token: String) async throws {
        let base = BackendService.shared.baseURL
        guard let url = Self.makeSTTURL(baseURL: base, config: config) else {
            throw NSError(domain: "DeepgramSTT", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid backend URL"])
        }

        var req = URLRequest(url: url)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let task = urlSession.webSocketTask(with: req)
        task.resume()

        // Confirm the connection is viable by waiting for the backend's ready message.
        do {
            let first = try await task.awaitReceive(timeout: 6.0)
            self.handle(message: first)
        } catch {
            task.cancel(with: .goingAway, reason: nil)
            throw error
        }

        wsTask?.cancel(with: .goingAway, reason: nil)
        wsTask = task
    }

    private func receiveLoop() {
        wsTask?.receive { [weak self] result in
            guard let self else { return }
            Task { @MainActor in
                switch result {
                case .failure(let err):
                    let existing = (self.lastError ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
                    if existing.isEmpty {
                        self.lastError = err.localizedDescription
                    }
#if DEBUG
                    print("🎙️ [DeepgramSTT] ws receive failure: \(err.localizedDescription)")
#endif
                    self.isConnected = false
                    self.isUserSpeaking = false
                case .success(let msg):
                    self.handle(message: msg)
                    if self.isConnected {
                        self.receiveLoop()
                    }
                }
            }
        }
    }

    @MainActor
    private func handle(message: URLSessionWebSocketTask.Message) {
        switch message {
        case .string(let str):
            handle(jsonString: str)
        case .data(let data):
            if let str = String(data: data, encoding: .utf8) {
                handle(jsonString: str)
            }
        @unknown default:
            break
        }
    }

    @MainActor
    private func handle(jsonString: String) {
        guard let data = jsonString.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data),
              let dict = obj as? [String: Any]
        else { return }

        // Backend control plane.
        if let t = dict["type"] as? String, t == "talktome.stt.ready" {
            // Ready handshake.
            return
        }
        if let t = dict["type"] as? String, t == "talktome.error" {
            let msg = (dict["message"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
            lastError = msg
            isConnected = false
            isUserSpeaking = false
            return
        }

        // Deepgram results.
        let isFinal = dict["is_final"] as? Bool
        let speechFinal = dict["speech_final"] as? Bool
        let transcript: String? = {
            guard let channel = dict["channel"] as? [String: Any],
                  let alts = channel["alternatives"] as? [[String: Any]],
                  let first = alts.first
            else { return nil }
            return first["transcript"] as? String
        }()

        // Some messages (metadata, etc.) won't include `is_final` / transcript.
        guard let isFinal else { return }

        let trimmed = (transcript ?? "").trimmingCharacters(in: .whitespacesAndNewlines)

        if isFinal {
            if !trimmed.isEmpty {
                committedParts.append(trimmed)
                currentUtteranceFinalParts.append(trimmed)
            }
            currentInterim = ""
            updatePresentedTranscript()
        } else {
            // Interim hypothesis: mutable, may be revised as more audio arrives.
            currentInterim = trimmed
            updatePresentedTranscript()
            isUserSpeaking = !trimmed.isEmpty
        }

        if speechFinal == true {
            isUserSpeaking = false
            let utterance = currentUtteranceFinalParts.joined(separator: " ").trimmingCharacters(in: .whitespacesAndNewlines)
            if !utterance.isEmpty {
                lastFinalUtterance = utterance
            }
            currentUtteranceFinalParts = []

            if transcriptAggregation == .perUtterance {
                // Clear the UI + buffers once an utterance ends (Speak Mode expects turn-by-turn).
                committedParts = []
                currentInterim = ""
                updatePresentedTranscript()
            }
        }
    }

    @MainActor
    private func updatePresentedTranscript() {
        let committed = committedParts
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
            .joined(separator: " ")
        let interim = currentInterim.trimmingCharacters(in: .whitespacesAndNewlines)

        if committed.isEmpty {
            userTranscript = interim
        } else if interim.isEmpty {
            userTranscript = committed
        } else {
            userTranscript = committed + " " + interim
        }
    }

    private func sendAudio(_ data: Data) {
        wsSendQueue.async { [weak self] in
            guard let self else { return }
            guard !data.isEmpty else { return }

            // If we have an active websocket task, send immediately. Otherwise buffer while recording.
            if let task = self.wsTask {
                task.send(.data(data)) { _ in }
                return
            }

            guard self.isRecording else { return }
            self.bufferAudioLocked(data)
        }
    }

    private func sendJSONEvent(_ obj: [String: Any]) {
        wsSendQueue.async { [weak self] in
            guard let self else { return }
            guard let task = self.wsTask else { return }
            guard JSONSerialization.isValidJSONObject(obj),
                  let data = try? JSONSerialization.data(withJSONObject: obj),
                  let str = String(data: data, encoding: .utf8)
            else { return }
            task.send(.string(str)) { _ in }
        }
    }

    @MainActor
    private func flushBufferedAudioIfPossible() {
        guard let task = wsTask else { return }
        wsSendQueue.async { [weak self] in
            guard let self else { return }
            guard !self.bufferedAudioChunks.isEmpty else { return }
            let chunks = self.bufferedAudioChunks
            self.bufferedAudioChunks.removeAll(keepingCapacity: true)
            self.bufferedAudioBytes = 0
            for data in chunks {
                task.send(.data(data)) { _ in }
            }
        }
    }

    @MainActor
    private func clearBufferedAudio() {
        wsSendQueue.async { [weak self] in
            guard let self else { return }
            self.bufferedAudioChunks.removeAll(keepingCapacity: true)
            self.bufferedAudioBytes = 0
        }
    }

    private func bufferAudioLocked(_ data: Data) {
        bufferedAudioChunks.append(data)
        bufferedAudioBytes += data.count
        trimBufferedAudioLocked()
    }

    private func trimBufferedAudioLocked() {
        guard bufferedAudioBytes > maxBufferedAudioBytes else { return }
        while bufferedAudioBytes > maxBufferedAudioBytes, !bufferedAudioChunks.isEmpty {
            let removed = bufferedAudioChunks.removeFirst()
            bufferedAudioBytes -= removed.count
        }
    }

    // MARK: - Audio session / engine

    @MainActor
    private func requestMicrophonePermissionIfNeeded() async -> Bool {
        if #available(iOS 17.0, *) {
            let perm = AVAudioApplication.shared.recordPermission
            if perm == .granted { return true }
            if perm == .denied { return false }
            return await withCheckedContinuation { cont in
                AVAudioApplication.requestRecordPermission { granted in
                    cont.resume(returning: granted)
                }
            }
        } else {
            let session = AVAudioSession.sharedInstance()
            let perm = session.recordPermission
            if perm == .granted { return true }
            if perm == .denied { return false }
            return await withCheckedContinuation { cont in
                session.requestRecordPermission { granted in
                    cont.resume(returning: granted)
                }
            }
        }
    }

    // Not @MainActor: can be slow; don't block UI updates.
    @discardableResult
    private func configureAudioSessionForVoiceLocked(reportErrors: Bool = true) -> Bool {
        let session = AVAudioSession.sharedInstance()
        do {
            try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetoothHFP])
            try session.setActive(true, options: [])
        } catch {
            if reportErrors {
                Task { @MainActor in
                    self.lastError = "Audio session error: \(error.localizedDescription)"
                }
            }
            return false
        }
        if session.isInputAvailable == false {
            if reportErrors {
                Task { @MainActor in
                    self.lastError = "No microphone input available."
                }
            }
            return false
        }
        return true
    }

    private func ensureEngineRunning(reportErrors: Bool = true) -> Bool {
        let session = AVAudioSession.sharedInstance()
        guard session.isInputAvailable else {
            if reportErrors {
                Task { @MainActor in
                    self.lastError = "No microphone input available."
                }
            }
            return false
        }

        if audioEngine == nil {
            audioEngine = AVAudioEngine()
        }
        guard let engine = audioEngine else { return false }

        _ = engine.inputNode
        if engine.isRunning == false {
            engine.prepare()
            do {
                try engine.start()
            } catch {
                if reportErrors {
                    Task { @MainActor in
                        self.lastError = "Audio engine start failed: \(error.localizedDescription)"
                    }
                }
                return false
            }
        }
        return engine.isRunning
    }

    @MainActor
    private func startRecordingAfterPermission() {
        // Flip UI state immediately so the button reacts instantly.
        // Audio session + engine setup happens off the critical path.
        isRecording = true

        // Reset transcript for a new dictation run.
        userTranscript = ""
        isUserSpeaking = false
        lastFinalUtterance = nil
        committedParts = []
        currentInterim = ""
        currentUtteranceFinalParts = []

        audioQueue.async { [weak self] in
            guard let self else { return }
            // The user may have tapped "Stop" before our audio queue work runs.
            guard self.isRecording else { return }
            guard self.configureAudioSessionForVoiceLocked() else {
                Task { @MainActor in self.isRecording = false }
                return
            }
            guard self.isRecording else { return }
            let ok = self.installMicTapIfNeeded()
            guard ok else {
                Task { @MainActor in self.isRecording = false }
                return
            }
        }
    }

    @discardableResult
    private func installMicTapIfNeeded() -> Bool {
        guard isRecording else { return true } // stopped before tap install; nothing to do
        guard ensureEngineRunning() else { return false }
        guard let engine = audioEngine else { return false }

        let input = engine.inputNode
        let inputFormat = input.outputFormat(forBus: 0)

        let target = AVAudioFormat(
            commonFormat: .pcmFormatInt16,
            sampleRate: Double(config.sampleRateHz),
            channels: 1,
            interleaved: false
        )!
        inputConverter = AVAudioConverter(from: inputFormat, to: target)
        if inputConverter == nil { return false }

        input.removeTap(onBus: 0)
        input.installTap(onBus: 0, bufferSize: 1024, format: inputFormat) { [weak self] buffer, _ in
            guard let self else { return }
            self.updateSpawnLevel(from: buffer)
            self.convertAndSend(buffer: buffer, targetFormat: target)
        }
        return true
    }

    private func convertAndSend(buffer: AVAudioPCMBuffer, targetFormat: AVAudioFormat) {
        guard isRecording else { return }
        guard let converter = inputConverter else { return }

        let inputRate = buffer.format.sampleRate
        let ratio = targetFormat.sampleRate / max(1, inputRate)
        let outCapacity = AVAudioFrameCount(Double(buffer.frameLength) * ratio + 8)
        guard let outBuffer = AVAudioPCMBuffer(pcmFormat: targetFormat, frameCapacity: outCapacity) else { return }

        var err: NSError?
        let inputBlock: AVAudioConverterInputBlock = { _, outStatus in
            outStatus.pointee = .haveData
            return buffer
        }
        converter.convert(to: outBuffer, error: &err, withInputFrom: inputBlock)
        if err != nil { return }

        guard outBuffer.frameLength > 0, let ptr = outBuffer.int16ChannelData?.pointee else { return }
        let byteCount = Int(outBuffer.frameLength) * MemoryLayout<Int16>.size
        let data = Data(bytes: ptr, count: byteCount)
        sendAudio(data)
    }

    private func updateSpawnLevel(from buffer: AVAudioPCMBuffer) {
        guard let channel = buffer.floatChannelData?.pointee else { return }
        let frames = Int(buffer.frameLength)
        if frames <= 0 { return }
        var peak: Float = 0
        for i in 0..<frames {
            let v = fabsf(channel[i])
            if v > peak { peak = v }
        }
        let db = 20.0 * log10f(max(peak, 1e-5))
        let normalized = max(0, min(1, (CGFloat(db) + 60) / 60))
        let noiseFloor: CGFloat = 0.10
        let gated = max(0, normalized - noiseFloor) / (1 - noiseFloor)
        let shaped = min(1, sqrt(gated))
        Task { @MainActor in
            self.spawnLevel = shaped
        }
    }

    private func stopRecordingIfNeeded() {
        audioQueue.async { [weak self] in
            guard let self else { return }
            if let engine = self.audioEngine, engine.isRunning {
                engine.inputNode.removeTap(onBus: 0)
            }
        }
    }

    // MARK: - URL helpers

    private static func makeSTTURL(baseURL: URL, config: Config) -> URL? {
        let url = baseURL
            .appendingPathComponent("speech")
            .appendingPathComponent("stt")
            .appendingPathComponent("stream")

        guard var comps = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return nil }
        comps.queryItems = [
            URLQueryItem(name: "model", value: config.model),
            URLQueryItem(name: "language", value: config.language),
            URLQueryItem(name: "endpointing", value: String(config.endpointingMs)),
            URLQueryItem(name: "sample_rate", value: String(config.sampleRateHz)),
        ]

        if comps.scheme == "https" {
            comps.scheme = "wss"
        } else if comps.scheme == "http" {
            comps.scheme = "ws"
        }
        return comps.url
    }
}


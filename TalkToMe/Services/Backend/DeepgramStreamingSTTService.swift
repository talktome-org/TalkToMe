import AVFoundation
import Foundation

final class DeepgramStreamingSTTService: ObservableObject, @unchecked Sendable {
    struct Config: Equatable {
        var language: String = "multi"
        var model: String = "nova-3"
        var endpointingMs: Int = 400
        var sampleRateHz: Int = 24_000
    }

    enum TranscriptAggregation {
        case accumulate
        case perUtterance
    }

    @Published var isConnected: Bool = false
    @Published var isRecording: Bool = false
    @Published var isPaused: Bool = false
    @Published var lastError: String?
    @Published var spawnLevel: CGFloat = 0
    @Published var userTranscript: String = ""
    @Published var isUserSpeaking: Bool = false
    @Published var lastFinalUtterance: String?

    private let urlSession: URLSession
    private var wsTask: URLSessionWebSocketTask?

    private var config: Config = .init()
    var transcriptAggregation: TranscriptAggregation = .accumulate

    private var inputConverter: AVAudioConverter?

    private let audioQueue = DispatchQueue(label: "talktome.deepgramSTT.audio")
    private let wsSendQueue = DispatchQueue(label: "talktome.deepgramSTT.wsSend")
    private var didPrewarmAudio: Bool = false
    private var bufferedAudioChunks: [Data] = []
    private var bufferedAudioBytes: Int = 0
    private let maxBufferedAudioBytes: Int = 480_000
    private var committedParts: [String] = []
    private var currentInterim: String = ""
    private var currentUtteranceFinalParts: [String] = []
    private var keepAliveTask: Task<Void, Never>?
    private let keepAliveIntervalSeconds: TimeInterval = 10.0

    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }

    @MainActor
    func connect(config: Config = .init()) async {
        self.config = config
        self.lastError = nil
        self.isUserSpeaking = false
        self.lastFinalUtterance = nil
        // Don't wipe active transcript state when reconnecting mid-capture.
        if !self.isRecording {
            self.userTranscript = ""
            self.committedParts = []
            self.currentInterim = ""
            self.currentUtteranceFinalParts = []
        }

        guard let token = await AuthService.shared.getAccessToken() else {
            self.lastError = "Not authenticated."
            return
        }

        do {
            try await openWebSocket(token: token)
            self.isConnected = true
            self.receiveLoop()
            self.startKeepAlive()
            self.flushBufferedAudioIfPossible()
        } catch {
            let msg = "STT connection failed: \(error.localizedDescription)"
            self.lastError = msg
            self.isConnected = false
        }
    }

    private func startKeepAlive() {
        keepAliveTask?.cancel()
        keepAliveTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(self?.keepAliveIntervalSeconds ?? 10.0) * 1_000_000_000)
                guard !Task.isCancelled else { break }
                guard let self, self.isConnected, let task = self.wsTask else { break }
                task.sendPing { error in
                    if error != nil {
                        Task { @MainActor in
                            self.isConnected = false
                        }
                    }
                }
            }
        }
    }

    private func stopKeepAlive() {
        keepAliveTask?.cancel()
        keepAliveTask = nil
    }

    @MainActor
    func disconnect() {
        SharedAudioEngine.shared.removeInputTap()
        stopKeepAlive()
        isConnected = false
        isRecording = false
        isPaused = false
        clearBufferedAudio()
        wsTask?.cancel(with: .goingAway, reason: nil)
        wsTask = nil
    }

    @MainActor
    func startRecording() {
        lastError = nil
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
        guard !didPrewarmAudio else { return }
        SharedAudioEngine.shared.ensureRunning()
        didPrewarmAudio = true
    }

    @MainActor
    func stopRecording() {
        isRecording = false
        isPaused = false
        SharedAudioEngine.shared.removeInputTap()
        flushBufferedAudioIfPossible()
        sendJSONEvent(["type": "finalize"])
        clearBufferedAudio()
    }

    @MainActor
    func pauseCapture() {
        guard isRecording, !isPaused else { return }
        isPaused = true
        isUserSpeaking = false
        spawnLevel = 0
        // Flush any buffered audio and ask Deepgram to finalize so an
        // in-progress utterance still produces a final transcript.
        flushBufferedAudioIfPossible()
        sendJSONEvent(["type": "finalize"])
    }

    @MainActor
    func resumeCapture() {
        guard isRecording, isPaused else { return }
        isPaused = false
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

    // MARK: - Transcript handling

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

        if let t = dict["type"] as? String, t == "talktome.stt.ready" {
            return
        }
        if let t = dict["type"] as? String, t == "talktome.error" {
            let msg = (dict["message"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines)
            lastError = msg
            isConnected = false
            isUserSpeaking = false
            return
        }

        let isFinal = dict["is_final"] as? Bool
        let speechFinal = dict["speech_final"] as? Bool
        let transcript: String? = {
            guard let channel = dict["channel"] as? [String: Any],
                  let alts = channel["alternatives"] as? [[String: Any]],
                  let first = alts.first
            else { return nil }
            return first["transcript"] as? String
        }()

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
            // Ignore transient empty interim packets to prevent transcript flicker.
            if !trimmed.isEmpty {
                currentInterim = trimmed
                updatePresentedTranscript()
            }
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
                committedParts = []
                currentInterim = ""
                // Keep the last utterance visible until new speech arrives.
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

    // MARK: - Audio capture (SharedAudioEngine)

    @MainActor
    private func startRecordingAfterPermission() {
        isRecording = true
        userTranscript = ""
        isUserSpeaking = false
        lastFinalUtterance = nil
        committedParts = []
        currentInterim = ""
        currentUtteranceFinalParts = []

        let sampleRate = config.sampleRateHz
        audioQueue.async { [weak self] in
            guard let self, self.isRecording else { return }
            self.installMicTap(sampleRateHz: sampleRate)
        }
    }

    private func installMicTap(sampleRateHz: Int) {
        SharedAudioEngine.shared.ensureRunning()

        guard let inputFmt = SharedAudioEngine.shared.inputFormat else { return }

        let target = AVAudioFormat(
            commonFormat: .pcmFormatInt16,
            sampleRate: Double(sampleRateHz),
            channels: 1,
            interleaved: false
        )!
        inputConverter = AVAudioConverter(from: inputFmt, to: target)
        guard inputConverter != nil else { return }

        SharedAudioEngine.shared.installInputTap { [weak self] buffer, _ in
            guard let self else { return }
            guard !self.isPaused else { return }
            self.updateSpawnLevel(from: buffer)
            self.convertAndSend(buffer: buffer, targetFormat: target)
        }
    }

    // MARK: - Audio sending

    private func sendAudio(_ data: Data) {
        wsSendQueue.async { [weak self] in
            guard let self else { return }
            guard !data.isEmpty else { return }

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

    // MARK: - Helpers

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
            guard !self.isPaused else { return }
            self.spawnLevel = shaped
        }
    }

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

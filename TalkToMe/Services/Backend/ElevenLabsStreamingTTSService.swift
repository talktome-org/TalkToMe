import AVFoundation
import Foundation

final class ElevenLabsStreamingTTSService: ObservableObject, @unchecked Sendable {
    struct Config: Equatable {
        var modelId: String = "eleven_flash_v2_5"
        var outputFormat: String = "pcm_24000"
    }

    @Published var isConnected: Bool = false
    @Published var isSpeaking: Bool = false
    @Published var speakerLevel: CGFloat = 0
    @Published var lastError: String?

    private let urlSession: URLSession
    private var wsTask: URLSessionWebSocketTask?
    private var config: Config = .init()
    private var voiceId: String?
    private var voiceName: String?
    private let audioQueue = DispatchQueue(label: "talktome.elevenlabs.tts.audio")
    private let wsSendQueue = DispatchQueue(label: "talktome.elevenlabs.tts.wsSend")

    // ── Audio-queue only ──
    private var startedPlayback: Bool = false

    // ── Main-thread only ──
    private var finishSent: Bool = false
    private var pendingFinish: Bool = false
    private var pendingText: String = ""
    private var flushWorkItem: DispatchWorkItem?
    private var keepAliveTask: Task<Void, Never>?
    private let keepAliveIntervalSeconds: TimeInterval = 15.0

    private var playbackFormat: AVAudioFormat {
        AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 24_000, channels: 1, interleaved: false)!
    }

    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }

    // MARK: - Public (MainActor)

    @MainActor
    func preconnect(voiceId: String, voiceName: String? = nil, config: Config = .init()) async {
        guard !isConnected else { return }
        self.config = config
        self.voiceId = voiceId
        self.voiceName = voiceName

        guard let token = await AuthService.shared.getAccessToken() else { return }
        do {
            try await openWebSocket(token: token)
            self.isConnected = true
            self.receiveLoop()
            self.startKeepAlive()
        } catch {
            // Silently fail — we'll connect normally when start() is called
        }
    }

    @MainActor
    func start(voiceId: String, voiceName: String? = nil, config: Config = .init()) async {
        self.config = config
        self.voiceId = voiceId
        self.voiceName = voiceName
        self.lastError = nil
        self.isSpeaking = false
        self.speakerLevel = 0
        self.finishSent = false
        self.pendingFinish = false

        audioQueue.async {
            self.startedPlayback = false
        }

        if isConnected {
            self.flushPendingTextImmediately()
            if self.pendingFinish {
                self.pendingFinish = false
                self.flushPendingTextImmediately()
                self.sendEvent(["type": "end"])
            }
            return
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
            self.flushPendingTextImmediately()
            if self.pendingFinish {
                self.pendingFinish = false
                self.flushPendingTextImmediately()
                self.sendEvent(["type": "end"])
            }
        } catch {
            self.lastError = "TTS connection failed: \(error.localizedDescription)"
            self.isConnected = false
        }
    }

    @MainActor
    func appendTextDelta(_ delta: String) {
        guard !finishSent else { return }

        pendingText += delta
        let shouldFlush: Bool = {
            if pendingText.count >= 48 { return true }
            if delta.contains(where: { $0 == " " || $0 == "\n" }) { return true }
            if delta.contains(where: { ".!?,".contains($0) }) { return true }
            return false
        }()
        if shouldFlush {
            flushPendingTextSoon()
        }
    }

    @MainActor
    func finish() {
        finishSent = true
        if isConnected {
            flushPendingTextImmediately()
            sendEvent(["type": "end"])
        } else {
            pendingFinish = true
        }
    }

    @MainActor
    func cancel() {
        flushWorkItem?.cancel()
        flushWorkItem = nil
        pendingText = ""
        finishSent = true
        pendingFinish = false
        stopKeepAlive()
        sendEvent(["type": "cancel"])
        wsTask?.cancel(with: .goingAway, reason: nil)
        wsTask = nil
        isConnected = false
        stopPlayback()
    }

    // MARK: - Keep-alive

    private func startKeepAlive() {
        keepAliveTask?.cancel()
        keepAliveTask = Task { @MainActor [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(self?.keepAliveIntervalSeconds ?? 15.0) * 1_000_000_000)
                guard !Task.isCancelled else { break }
                guard let self, self.isConnected, let task = self.wsTask else { break }
                task.sendPing { error in
                    if error != nil {
                        Task { @MainActor in self.isConnected = false }
                    }
                }
            }
        }
    }

    private func stopKeepAlive() {
        keepAliveTask?.cancel()
        keepAliveTask = nil
    }

    // MARK: - WebSocket

    private func openWebSocket(token: String) async throws {
        guard let voiceId else {
            throw NSError(domain: "ElevenLabsTTS", code: -1, userInfo: [NSLocalizedDescriptionKey: "Missing voice id"])
        }

        let base = BackendService.shared.baseURL
        guard let url = Self.makeTTSStreamURL(
            baseURL: base, voiceId: voiceId, voiceName: voiceName,
            modelId: config.modelId, outputFormat: config.outputFormat
        ) else {
            throw NSError(domain: "ElevenLabsTTS", code: -1, userInfo: [NSLocalizedDescriptionKey: "Invalid backend URL"])
        }

        var req = URLRequest(url: url)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        let task = urlSession.webSocketTask(with: req)
        task.resume()

        do {
            try await task.awaitPing(timeout: 5.0)
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
                    if self.finishSent {
                        self.isConnected = false
                        self.wsTask = nil
                        return
                    }
                    self.lastError = err.localizedDescription
                    self.isConnected = false
                    self.stopPlayback()
                case .success(let msg):
                    self.handle(message: msg)
                    if self.isConnected {
                        self.receiveLoop()
                    }
                }
            }
        }
    }

    private func handle(message: URLSessionWebSocketTask.Message) {
        switch message {
        case .string(let str): handle(jsonString: str)
        case .data(let data):
            if let str = String(data: data, encoding: .utf8) { handle(jsonString: str) }
        @unknown default: break
        }
    }

    private func handle(jsonString: String) {
        guard let data = jsonString.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data),
              let dict = obj as? [String: Any]
        else { return }

        let audioB64 = (dict["audio"] as? String) ?? ""
        let isFinal = (dict["isFinal"] as? Bool) == true

        if !audioB64.isEmpty {
            enqueuePCMChunk(base64PCM: audioB64, isLast: isFinal)
        } else if isFinal {
            scheduleEndSentinel()
        }

        if isFinal {
            finishSent = true
            stopKeepAlive()
            wsTask?.cancel(with: .normalClosure, reason: nil)
            wsTask = nil
            isConnected = false
            if let vid = voiceId {
                let cfg = config
                let vname = voiceName
                Task { @MainActor in
                    await self.preconnect(voiceId: vid, voiceName: vname, config: cfg)
                }
            }
        }
    }

    // MARK: - Text flushing

    private func sendEvent(_ obj: [String: Any]) {
        guard let task = wsTask else { return }
        wsSendQueue.async {
            guard JSONSerialization.isValidJSONObject(obj),
                  let data = try? JSONSerialization.data(withJSONObject: obj),
                  let str = String(data: data, encoding: .utf8)
            else { return }
            task.send(.string(str)) { _ in }
        }
    }

    private func flushPendingTextSoon() {
        flushWorkItem?.cancel()
        let item = DispatchWorkItem { [weak self] in
            Task { @MainActor in self?.flushPendingTextImmediately() }
        }
        flushWorkItem = item
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.015, execute: item)
    }

    private func flushPendingTextImmediately() {
        guard isConnected, wsTask != nil else { return }
        let trimmed = pendingText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        pendingText = ""
        sendEvent(["type": "text", "text": trimmed])
    }

    // MARK: - Audio playback (SharedAudioEngine)

    private func enqueuePCMChunk(base64PCM: String, isLast: Bool = false) {
        guard let rawData = Data(base64Encoded: base64PCM), !rawData.isEmpty else {
            if isLast { scheduleEndSentinel() }
            return
        }
        audioQueue.async { [weak self] in
            guard let self else { return }
            SharedAudioEngine.shared.ensureRunning()

            let frames = AVAudioFrameCount(rawData.count / MemoryLayout<Int16>.size)
            guard frames > 0,
                  let pcm = AVAudioPCMBuffer(pcmFormat: self.playbackFormat, frameCapacity: frames)
            else {
                if isLast { self.scheduleEndSentinelOnQueue() }
                return
            }
            pcm.frameLength = frames
            rawData.withUnsafeBytes { raw in
                guard let base = raw.baseAddress else { return }
                memcpy(pcm.int16ChannelData!.pointee, base, rawData.count)
            }

            SharedAudioEngine.shared.playerNode.scheduleBuffer(pcm)

            if !self.startedPlayback {
                self.startedPlayback = true
                // Install speaker level tap before playing
                SharedAudioEngine.shared.installSpeakerLevelTap { [weak self] level in
                    Task { @MainActor in self?.speakerLevel = level }
                }
                SharedAudioEngine.shared.playerNode.play()
                Task { @MainActor in self.isSpeaking = true }
            }

            if isLast {
                self.scheduleEndSentinelOnQueue()
            }
        }
    }

    private func scheduleEndSentinel() {
        audioQueue.async { [weak self] in
            self?.scheduleEndSentinelOnQueue()
        }
    }

    private func scheduleEndSentinelOnQueue() {
        SharedAudioEngine.shared.ensureRunning()

        guard let silence = AVAudioPCMBuffer(pcmFormat: playbackFormat, frameCapacity: 1) else { return }
        silence.frameLength = 1
        silence.int16ChannelData!.pointee.initialize(to: 0)

        SharedAudioEngine.shared.playerNode.scheduleBuffer(silence) { [weak self] in
            guard let self else { return }
            self.audioQueue.async {
                self.startedPlayback = false
                SharedAudioEngine.shared.playerNode.stop()
                SharedAudioEngine.shared.removeSpeakerLevelTap()
                Task { @MainActor in
                    self.isSpeaking = false
                    self.speakerLevel = 0
                }
            }
        }

        if !startedPlayback {
            startedPlayback = true
            SharedAudioEngine.shared.playerNode.play()
        }
    }

    private func stopPlayback() {
        audioQueue.async { [weak self] in
            guard let self else { return }
            SharedAudioEngine.shared.playerNode.stop()
            SharedAudioEngine.shared.removeSpeakerLevelTap()
            self.startedPlayback = false
            Task { @MainActor in
                self.isSpeaking = false
                self.speakerLevel = 0
            }
        }
    }

    // MARK: - URL

    private static func makeTTSStreamURL(
        baseURL: URL, voiceId: String, voiceName: String?,
        modelId: String, outputFormat: String
    ) -> URL? {
        let url = baseURL
            .appendingPathComponent("speech")
            .appendingPathComponent("tts")
            .appendingPathComponent("stream")
        guard var comps = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return nil }
        var items = [
            URLQueryItem(name: "voice_id", value: voiceId),
            URLQueryItem(name: "model_id", value: modelId),
            URLQueryItem(name: "output_format", value: outputFormat),
        ]
        if let name = voiceName, !name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            items.append(URLQueryItem(name: "voice_name", value: name))
        }
        comps.queryItems = items
        if comps.scheme == "https" { comps.scheme = "wss" }
        else if comps.scheme == "http" { comps.scheme = "ws" }
        return comps.url
    }
}

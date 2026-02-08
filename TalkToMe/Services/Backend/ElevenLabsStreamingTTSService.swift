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
    private var audioEngine: AVAudioEngine?
    private let playerNode = AVAudioPlayerNode()
    private let audioQueue = DispatchQueue(label: "talktome.elevenlabs.tts.audio")
    private let wsSendQueue = DispatchQueue(label: "talktome.elevenlabs.tts.wsSend")

    private var queuedPlaybackBuffers: Int = 0
    private var startedPlayback: Bool = false
    private var endRequested: Bool = false
    private var playerGraphConfigured: Bool = false
    private var pendingText: String = ""
    private var flushWorkItem: DispatchWorkItem?

    private var playbackFormat: AVAudioFormat {
        AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 24_000, channels: 1, interleaved: false)!
    }

    init(urlSession: URLSession = .shared) {
        self.urlSession = urlSession
    }

    @MainActor
    func start(voiceId: String, config: Config = .init()) async {
        self.config = config
        self.voiceId = voiceId
        self.lastError = nil
        self.isConnected = false
        self.isSpeaking = false
        self.speakerLevel = 0
        self.endRequested = false
        self.queuedPlaybackBuffers = 0
        self.startedPlayback = false

        guard let token = await AuthService.shared.getAccessToken() else {
            self.lastError = "Not authenticated."
            return
        }

        do {
            try await openWebSocket(token: token)
            self.isConnected = true
            self.receiveLoop()
            self.flushPendingTextSoon()
        } catch {
            self.lastError = "TTS connection failed: \(error.localizedDescription)"
            self.isConnected = false
        }
    }

    @MainActor
    func appendTextDelta(_ delta: String) {
        guard !endRequested else { return }

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
        guard isConnected else {
            return
        }
        endRequested = true
        flushPendingTextImmediately()
        sendEvent(["type": "end"])
    }

    @MainActor
    func cancel() {
        flushWorkItem?.cancel()
        flushWorkItem = nil
        pendingText = ""
        endRequested = true
        sendEvent(["type": "cancel"])
        wsTask?.cancel(with: .goingAway, reason: nil)
        wsTask = nil
        isConnected = false
        stopPlayback()
    }

    private func openWebSocket(token: String) async throws {
        guard let voiceId else {
            throw NSError(domain: "ElevenLabsTTS", code: -1, userInfo: [NSLocalizedDescriptionKey: "Missing voice id"])
        }

        let base = BackendService.shared.baseURL
        guard let url = Self.makeTTSStreamURL(
            baseURL: base,
            voiceId: voiceId,
            modelId: config.modelId,
            outputFormat: config.outputFormat
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
                    if self.endRequested {
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

    private func handle(jsonString: String) {
        guard let data = jsonString.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data),
              let dict = obj as? [String: Any]
        else { return }
        if let audioB64 = dict["audio"] as? String, !audioB64.isEmpty {
            enqueuePCMChunk(base64PCM: audioB64)
        }
        if let isFinal = dict["isFinal"] as? Bool, isFinal == true {
            endRequested = true
            wsTask?.cancel(with: .normalClosure, reason: nil)
            wsTask = nil
            isConnected = false
        }
    }

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
            Task { @MainActor in
                self?.flushPendingTextImmediately()
            }
        }
        flushWorkItem = item
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.035, execute: item)
    }

    private func flushPendingTextImmediately() {
        guard isConnected, wsTask != nil else { return }
        let trimmed = pendingText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        pendingText = ""
        sendEvent(["type": "text", "text": trimmed])
    }

    private func setupPlayerIfNeeded(engine: AVAudioEngine) {
        guard playerGraphConfigured == false else { return }
        if engine.attachedNodes.contains(playerNode) == false {
            engine.attach(playerNode)
        }
        engine.connect(playerNode, to: engine.mainMixerNode, format: playbackFormat)
        let tapFormat = engine.mainMixerNode.outputFormat(forBus: 0)
        engine.mainMixerNode.installTap(onBus: 0, bufferSize: 1024, format: tapFormat) { [weak self] buffer, _ in
            guard let self else { return }
            guard let channelData = buffer.floatChannelData else { return }
            let frames = buffer.frameLength
            guard frames > 0 else { return }
            var sum: Float = 0
            let data = channelData[0]
            for i in 0..<Int(frames) {
                let sample = data[i]
                sum += sample * sample
            }
            let rms = sqrt(sum / Float(frames))
            let level = CGFloat(min(1, rms * 3))

            Task { @MainActor in
                self.speakerLevel = level
            }
        }

        playerGraphConfigured = true
    }

    private func ensureEngineRunning() {
        if audioEngine?.isRunning == true { return }
        if audioEngine == nil {
            audioEngine = AVAudioEngine()
        }
        guard let engine = audioEngine else { return }
        do {
            setupPlayerIfNeeded(engine: engine)
            engine.prepare()
            try engine.start()
        } catch {
            Task { @MainActor in
                self.lastError = "Audio engine failed: \(error.localizedDescription)"
            }
        }
    }

    private func enqueuePCMChunk(base64PCM: String) {
        guard let data = Data(base64Encoded: base64PCM), !data.isEmpty else { return }
        audioQueue.async { [weak self] in
            guard let self else { return }
            self.ensureEngineRunning()

            let frames = AVAudioFrameCount(data.count / MemoryLayout<Int16>.size)
            guard frames > 0 else { return }
            guard let pcm = AVAudioPCMBuffer(pcmFormat: self.playbackFormat, frameCapacity: frames) else { return }
            pcm.frameLength = frames
            data.withUnsafeBytes { raw in
                guard let base = raw.baseAddress else { return }
                memcpy(pcm.int16ChannelData!.pointee, base, data.count)
            }

            self.queuedPlaybackBuffers += 1
            self.playerNode.scheduleBuffer(pcm, completionHandler: { [weak self] in
                guard let self else { return }
                self.audioQueue.async {
                    self.queuedPlaybackBuffers = max(0, self.queuedPlaybackBuffers - 1)
                    if self.queuedPlaybackBuffers == 0, self.endRequested {
                        self.startedPlayback = false
                        self.playerNode.stop()
                        Task { @MainActor in
                            self.isSpeaking = false
                            self.speakerLevel = 0
                        }
                    }
                }
            })

            if self.startedPlayback == false {
                if self.queuedPlaybackBuffers >= 2 || (self.endRequested && self.queuedPlaybackBuffers >= 1) {
                    self.startedPlayback = true
                    self.playerNode.play()
                    Task { @MainActor in self.isSpeaking = true }
                }
            }
        }
    }

    private func stopPlayback() {
        audioQueue.async { [weak self] in
            guard let self else { return }
            self.playerNode.stop()
            self.queuedPlaybackBuffers = 0
            self.startedPlayback = false
            Task { @MainActor in
                self.isSpeaking = false
                self.speakerLevel = 0
            }
        }
    }

    private func estimateLevel(fromPCM16Data data: Data) -> CGFloat {
        let maxSamples = 1200
        var peak: Int16 = 0
        data.withUnsafeBytes { raw in
            let count = min(raw.count / 2, maxSamples)
            guard count > 0 else { return }
            let ptr = raw.bindMemory(to: Int16.self).baseAddress!
            for i in 0..<count {
                let v = ptr[i]
                let absV = v == Int16.min ? Int16.max : abs(v)
                if absV > peak { peak = absV }
            }
        }
        let normalized = min(1, max(0, CGFloat(peak) / CGFloat(Int16.max)))
        return sqrt(normalized)
    }

    private static func makeTTSStreamURL(
        baseURL: URL,
        voiceId: String,
        modelId: String,
        outputFormat: String
    ) -> URL? {
        var url = baseURL
        url = url
            .appendingPathComponent("speech")
            .appendingPathComponent("tts")
            .appendingPathComponent("stream")

        guard var comps = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return nil }
        comps.queryItems = [
            URLQueryItem(name: "voice_id", value: voiceId),
            URLQueryItem(name: "model_id", value: modelId),
            URLQueryItem(name: "output_format", value: outputFormat),
        ]

        if comps.scheme == "https" {
            comps.scheme = "wss"
        } else if comps.scheme == "http" {
            comps.scheme = "ws"
        }
        return comps.url
    }
}


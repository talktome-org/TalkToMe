import AVFoundation
import Foundation

/// Single shared AVAudioEngine for both STT (input) and TTS (output).
///
/// By running input and output on the same engine with voice processing enabled,
/// iOS hardware acoustic echo cancellation (AEC) automatically removes speaker
/// output from the mic input — enabling reliable barge-in detection during TTS.
final class SharedAudioEngine: @unchecked Sendable {
    static let shared = SharedAudioEngine()

    private(set) var engine: AVAudioEngine?
    let playerNode = AVAudioPlayerNode()

    private let audioQueue = DispatchQueue(label: "bobo.sharedAudioEngine")
    private var isConfigured = false
    private var hasSpeakerLevelTap = false
    private var hasInputTap = false
    private var isVoiceSessionActive = false
    private var isVoiceSessionHeld = false

    private init() {}

    /// Ensure the shared engine is running with voice processing enabled.
    /// Safe to call from any thread — serialized on audioQueue.
    func ensureRunning() {
        audioQueue.sync {
            ensureRunningOnQueue()
        }
    }

    /// Kick off engine setup on the background queue without blocking the caller.
    /// Subsequent `ensureRunning()` calls will wait for this to finish then
    /// hit the fast path (engine already running).
    func ensureRunningAsync() {
        audioQueue.async {
            self.ensureRunningOnQueue()
        }
    }

    /// Prevent automatic audio session deactivation while speak mode is active.
    /// Synchronous so the hold is guaranteed to be in place before any
    /// subsequent async dispatches (e.g. ghost video ensureIdleAudioSession).
    func holdVoiceSession() {
        audioQueue.sync { self.isVoiceSessionHeld = true }
    }

    /// Return the app audio session to a non-blocking idle state.
    /// Safe to call from any thread.
    func ensureIdleAudioSession() {
        audioQueue.async {
            self.configureIdleAudioSessionOnQueue()
        }
    }

    /// Must be called on audioQueue.
    private func ensureRunningOnQueue() {
        // Re-acquire voice session only when needed.
        if !isVoiceSessionActive {
            let session = AVAudioSession.sharedInstance()
            do {
                try session.setCategory(.playAndRecord, mode: .voiceChat, options: [.defaultToSpeaker, .allowBluetoothHFP])
                try session.setActive(true, options: [])
                isVoiceSessionActive = true
            } catch {
                print("[SharedAudioEngine] Audio session error: \(error.localizedDescription)")
                return
            }
        }

        if let e = engine, e.isRunning { return }

        let eng: AVAudioEngine
        if let existing = engine {
            eng = existing
        } else {
            eng = AVAudioEngine()
            engine = eng
            isConfigured = false
            hasSpeakerLevelTap = false
            hasInputTap = false
        }

        if !isConfigured {
            // Enable voice processing for hardware AEC — must be done before start()
            do {
                try eng.inputNode.setVoiceProcessingEnabled(true)
            } catch {
                print("[SharedAudioEngine] Voice processing error: \(error.localizedDescription)")
                // Continue anyway — AEC just won't work
            }

            // Force output node initialization
            _ = eng.outputNode

            // Attach and connect player node for TTS playback
            if !eng.attachedNodes.contains(playerNode) {
                eng.attach(playerNode)
            }

            let playbackFormat = AVAudioFormat(
                commonFormat: .pcmFormatInt16,
                sampleRate: 24_000,
                channels: 1,
                interleaved: false
            )!
            let mixerFormat = eng.mainMixerNode.outputFormat(forBus: 0)
            eng.connect(playerNode, to: eng.mainMixerNode, format: playbackFormat)
            eng.connect(eng.mainMixerNode, to: eng.outputNode, format: mixerFormat)
            eng.mainMixerNode.outputVolume = 1.0

            isConfigured = true
        }

        eng.prepare()
        do {
            try eng.start()
        } catch {
            print("[SharedAudioEngine] Engine start failed: \(error.localizedDescription)")
            engine = nil
            isConfigured = false
        }
    }

    /// Install a tap on the input node for STT mic capture.
    /// The audio received here has hardware AEC applied — speaker output is removed.
    func installInputTap(bufferSize: AVAudioFrameCount = 1024, block: @escaping (AVAudioPCMBuffer, AVAudioTime) -> Void) {
        audioQueue.sync {
            ensureRunningOnQueue()
            guard let eng = engine else { return }
            let input = eng.inputNode
            let format = input.outputFormat(forBus: 0)
            input.removeTap(onBus: 0)
            input.installTap(onBus: 0, bufferSize: bufferSize, format: format, block: block)
            hasInputTap = true
        }
    }

    /// Remove the input tap (when STT stops recording).
    func removeInputTap() {
        audioQueue.async {
            self.engine?.inputNode.removeTap(onBus: 0)
            self.hasInputTap = false
            self.deactivateAudioSessionIfIdleOnQueue()
        }
    }

    /// Install a tap on the mixer to monitor speaker output level.
    func installSpeakerLevelTap(block: @escaping (CGFloat) -> Void) {
        audioQueue.sync {
            guard !hasSpeakerLevelTap else { return }
            ensureRunningOnQueue()
            guard let eng = engine else { return }
            let tapFormat = eng.mainMixerNode.outputFormat(forBus: 0)
            eng.mainMixerNode.installTap(onBus: 0, bufferSize: 1024, format: tapFormat) { buffer, _ in
                guard let ch = buffer.floatChannelData else { return }
                let n = Int(buffer.frameLength)
                guard n > 0 else { return }
                var sum: Float = 0
                let p = ch[0]
                for i in 0..<n { sum += p[i] * p[i] }
                let level = CGFloat(min(1, sqrt(sum / Float(n)) * 3))
                block(level)
            }
            hasSpeakerLevelTap = true
        }
    }

    /// Remove the speaker level tap.
    func removeSpeakerLevelTap() {
        audioQueue.async {
            guard self.hasSpeakerLevelTap else { return }
            self.engine?.mainMixerNode.removeTap(onBus: 0)
            self.hasSpeakerLevelTap = false
            self.deactivateAudioSessionIfIdleOnQueue()
        }
    }

    /// Stop the player node without stopping the engine.
    func stopPlayerNode() {
        audioQueue.async {
            self.playerNode.stop()
            self.deactivateAudioSessionIfIdleOnQueue()
        }
    }

    /// Fully stop the engine and tear down. Called when exiting speak mode.
    /// Runs synchronously so background music resumes before the call returns.
    func stop() {
        audioQueue.sync {
            self.isVoiceSessionHeld = false
            if self.hasSpeakerLevelTap {
                self.engine?.mainMixerNode.removeTap(onBus: 0)
                self.hasSpeakerLevelTap = false
            }
            self.engine?.inputNode.removeTap(onBus: 0)
            self.hasInputTap = false
            self.playerNode.stop()
            self.engine?.stop()
            // Always deactivate on full stop so background music can resume.
            self.deactivateVoiceSessionOnQueue()
        }
    }

    /// The format of the input node (for creating converters). Must call ensureRunning first.
    var inputFormat: AVAudioFormat? {
        audioQueue.sync {
            engine?.inputNode.outputFormat(forBus: 0)
        }
    }

    private func deactivateAudioSessionIfIdleOnQueue() {
        guard !isVoiceSessionHeld else { return }
        guard !hasInputTap else { return }
        guard !hasSpeakerLevelTap else { return }
        guard !playerNode.isPlaying else { return }

        // Stop the running engine so hardware is released, then deactivate the
        // session so iOS can notify interrupted media apps (e.g. Spotify) to resume.
        // The engine must stop first — iOS rejects setActive(false) while audio
        // resources are still held.
        playerNode.stop()
        engine?.stop()
        deactivateVoiceSessionOnQueue()
    }

    private func deactivateVoiceSessionOnQueue() {
        guard isVoiceSessionActive else { return }
        let session = AVAudioSession.sharedInstance()
        do {
            try session.setActive(false, options: [.notifyOthersOnDeactivation])
        } catch {
            print("[SharedAudioEngine] Audio session deactivate error: \(error.localizedDescription)")
        }
        do {
            try session.setCategory(.ambient, mode: .default, options: [.mixWithOthers])
        } catch {
            print("[SharedAudioEngine] Audio session idle category error: \(error.localizedDescription)")
        }
        isVoiceSessionActive = false
    }

    private func configureIdleAudioSessionOnQueue() {
        guard !isVoiceSessionHeld else { return }
        guard !hasInputTap else { return }
        guard !hasSpeakerLevelTap else { return }
        guard !playerNode.isPlaying else { return }

        if isVoiceSessionActive {
            deactivateVoiceSessionOnQueue()
        } else {
            // Even without a prior voice session, explicitly set ambient so
            // the default .soloAmbient category doesn't interrupt other audio
            // (e.g. when decorative video players start).
            let session = AVAudioSession.sharedInstance()
            try? session.setCategory(.ambient, mode: .default, options: [.mixWithOthers])
        }
    }
}

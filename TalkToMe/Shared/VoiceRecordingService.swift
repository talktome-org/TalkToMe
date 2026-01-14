
import AVFoundation
import AVFAudio
import Speech
import SwiftUI

@MainActor
class VoiceRecordingService: NSObject, ObservableObject {

    @Published var isRecording = false
    @Published var isTranscribing = false
    @Published var transcribedText = ""
    @Published var recordingDuration: TimeInterval = 0
    @Published var hasPermission = false
    @Published var isShowingRecordingPlaceholder = false
    @Published var audioLevel: Float = 0.0
    @Published var waveformSamples: [CGFloat] = Array(repeating: 0.05, count: 48)
    @Published var spawnLevel: CGFloat = 0.0

    private var audioRecorder: AVAudioRecorder?
    private var audioSession: AVAudioSession = AVAudioSession.sharedInstance()
    private var recordingTimer: Timer?
    private var speechRecognizer: SFSpeechRecognizer?
    private var recognitionRequest: SFSpeechAudioBufferRecognitionRequest?
    private var recognitionTask: SFSpeechRecognitionTask?
    private var audioEngine: AVAudioEngine?
    private let waveformSampleCount: Int = 48
    private var smoothedLevel: CGFloat = 0
    private var waveformTick: Int = 0

    override init() {
        super.init()
        speechRecognizer = SFSpeechRecognizer(locale: Locale(identifier: "en-US"))
        checkPermissions()
    }

    private func checkPermissions() {
        let speechPermission = SFSpeechRecognizer.authorizationStatus()
        if #available(iOS 17.0, *) {
            let micPermission = AVAudioApplication.shared.recordPermission
            hasPermission = (micPermission == .granted) && (speechPermission == .authorized)
        } else {
            let micPermission = audioSession.recordPermission
            hasPermission = (micPermission == .granted) && (speechPermission == .authorized)
        }
    }

    func requestPermissions() async -> Bool {
        let micPermission: Bool
        if #available(iOS 17.0, *) {
            micPermission = await withCheckedContinuation { continuation in
                AVAudioApplication.requestRecordPermission { granted in
                    continuation.resume(returning: granted)
                }
            }
        } else {
            micPermission = await withCheckedContinuation { continuation in
                audioSession.requestRecordPermission { granted in
                    continuation.resume(returning: granted)
                }
            }
        }

        let speechPermission = await withCheckedContinuation { continuation in
            SFSpeechRecognizer.requestAuthorization { status in
                continuation.resume(returning: status)
            }
        }

        let granted = micPermission && (speechPermission == .authorized)
        hasPermission = granted

        return granted
    }

    func startRecording() async {
        guard await requestPermissions() else {
            print("Voice recording permissions not granted")
            return
        }

        do {
            try audioSession.setCategory(.record, mode: .measurement, options: .duckOthers)
            try audioSession.setActive(true, options: .notifyOthersOnDeactivation)

            let documentsPath = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
            let audioFilename = documentsPath.appendingPathComponent("recording_\(UUID().uuidString).m4a")

            let settings = [
                AVFormatIDKey: Int(kAudioFormatMPEG4AAC),
                AVSampleRateKey: 44100,
                AVNumberOfChannelsKey: 2,
                AVEncoderAudioQualityKey: AVAudioQuality.high.rawValue
            ]

            audioRecorder = try AVAudioRecorder(url: audioFilename, settings: settings)
            audioRecorder?.delegate = self
            audioRecorder?.isMeteringEnabled = true
            audioRecorder?.record()

            isRecording = true
            recordingDuration = 0
            transcribedText = ""
            isShowingRecordingPlaceholder = true

            recordingTimer = Timer.scheduledTimer(withTimeInterval: 0.05, repeats: true) { [weak self] _ in
                Task { @MainActor in
                    guard let strongSelf = self else { return }
                    strongSelf.recordingDuration += 0.05
                    strongSelf.audioRecorder?.updateMeters()
                    let db = strongSelf.audioRecorder?.peakPower(forChannel: 0) ?? -60.0
                    let normalized = max(0, min(1, (CGFloat(db) + 60) / 60))
                    let noiseFloor: CGFloat = 0.11
                    let gated = max(0, normalized - noiseFloor) / (1 - noiseFloor)
                    let midBoost = sqrt(gated)
                    let shapedFast = min(1, (0.55 * gated) + (0.45 * midBoost))
                    let shapedSmooth = min(1, (0.70 * gated) + (0.30 * midBoost))
                    strongSelf.spawnLevel = shapedFast
                    strongSelf.smoothedLevel = (strongSelf.smoothedLevel * 0.85) + (shapedSmooth * 0.15)
                    strongSelf.audioLevel = Float(strongSelf.smoothedLevel)

                    if strongSelf.waveformSamples.count != strongSelf.waveformSampleCount {
                        strongSelf.waveformSamples = Array(repeating: 0.05, count: strongSelf.waveformSampleCount)
                    }
                    strongSelf.waveformTick += 1
                    if strongSelf.waveformTick % 4 == 0 {
                        strongSelf.waveformSamples.removeFirst()
                        strongSelf.waveformSamples.append(max(0.02, min(1.0, strongSelf.smoothedLevel)))
                    } else if let lastIndex = strongSelf.waveformSamples.indices.last {
                        strongSelf.waveformSamples[lastIndex] = max(0.02, min(1.0, strongSelf.smoothedLevel))
                    }
                }
            }

            startSpeechRecognition()

        } catch {
            print("Recording failed: \(error)")
            isRecording = false
        }
    }

    func stopRecording() {
        audioRecorder?.stop()
        audioRecorder = nil
        isRecording = false
        recordingTimer?.invalidate()
        recordingTimer = nil

        // Stop speech recognition
        recognitionRequest?.endAudio()
        recognitionTask?.cancel()
        recognitionRequest = nil
        recognitionTask = nil

        // Stop audio engine
        audioEngine?.stop()
        audioEngine?.inputNode.removeTap(onBus: 0)
        audioEngine = nil

        isTranscribing = false
        isShowingRecordingPlaceholder = false
        audioLevel = 0.0
        smoothedLevel = 0
        waveformSamples = Array(repeating: 0.05, count: waveformSampleCount)
    }

    private func startSpeechRecognition() {
        guard let speechRecognizer = speechRecognizer, speechRecognizer.isAvailable else {
            print("Speech recognizer not available")
            return
        }

        recognitionRequest = SFSpeechAudioBufferRecognitionRequest()
        guard let recognitionRequest = recognitionRequest else { return }

        recognitionRequest.shouldReportPartialResults = true

        recognitionTask = speechRecognizer.recognitionTask(with: recognitionRequest) { [weak self] result, error in
            DispatchQueue.main.async {
                if let result = result {
                    self?.transcribedText = result.bestTranscription.formattedString
                }

                if error != nil {
                    print("Speech recognition error: \(error?.localizedDescription ?? "Unknown error")")
                    self?.stopRecording()
                }
            }
        }

        // Configure audio engine for real-time recognition
        audioEngine = AVAudioEngine()
        guard let audioEngine = audioEngine else { return }

        let inputNode = audioEngine.inputNode
        let recordingFormat = inputNode.outputFormat(forBus: 0)

        inputNode.installTap(onBus: 0, bufferSize: 1024, format: recordingFormat) { [weak self] buffer, _ in
            self?.recognitionRequest?.append(buffer)
            guard let strongSelf = self else { return }

            // Derive instantaneous peak level from the audio buffer for fast bar spawning
            if let channelData = buffer.floatChannelData?[0] {
                let frameCount = Int(buffer.frameLength)
                var peak: Float = 0
                if frameCount > 0 {
                    for i in 0..<frameCount {
                        let v = fabsf(channelData[i])
                        if v > peak { peak = v }
                    }
                    let db = 20.0 * log10f(max(peak, 1e-5)) // [-100, 0]
                    let normalized = max(0, min(1, (CGFloat(db) + 60) / 60))
                    let noiseFloor: CGFloat = 0.11
                    let gated = max(0, normalized - noiseFloor) / (1 - noiseFloor)
                    let midBoost = sqrt(gated)
                    // Slightly stronger mid emphasis so sustained speech reads higher
                    let shapedFast = min(1, (0.50 * gated) + (0.50 * midBoost))
                    let shapedSmooth = min(1, (0.68 * gated) + (0.32 * midBoost))
                    DispatchQueue.main.async {
                        // Asymmetric update: rises with a tiny inertia, drops immediately
                        let prev = strongSelf.spawnLevel
                        if shapedFast > prev {
                            strongSelf.spawnLevel = min(1, 0.8 * shapedFast + 0.2 * prev)
                        } else {
                            strongSelf.spawnLevel = shapedFast
                        }
                        strongSelf.smoothedLevel = (strongSelf.smoothedLevel * 0.85) + (shapedSmooth * 0.15)
                        strongSelf.audioLevel = Float(strongSelf.smoothedLevel)
                    }
                }
            }
        }

        audioEngine.prepare()
        do {
            try audioEngine.start()
            isTranscribing = true
        } catch {
            print("Audio engine start failed: \(error)")
        }
    }

    func reset() {
        transcribedText = ""
        recordingDuration = 0
        isRecording = false
        isTranscribing = false
        isShowingRecordingPlaceholder = false
        audioLevel = 0.0
        smoothedLevel = 0
        waveformSamples = Array(repeating: 0.05, count: waveformSampleCount)
    }
}

extension VoiceRecordingService: AVAudioRecorderDelegate {
    nonisolated func audioRecorderDidFinishRecording(_ recorder: AVAudioRecorder, successfully flag: Bool) {
        Task { @MainActor in
            isTranscribing = false
            if !flag {
                print("Recording finished unsuccessfully")
            }
        }
    }

    nonisolated func audioRecorderEncodeErrorDidOccur(_ recorder: AVAudioRecorder, error: Error?) {
        Task { @MainActor in
            print("Audio recorder error: \(error?.localizedDescription ?? "Unknown error")")
            isRecording = false
            isTranscribing = false
        }
    }
}
import AVFoundation
import Foundation

@MainActor
final class ElevenLabsTTSPlayer: NSObject, ObservableObject, AVAudioPlayerDelegate {
    static let shared = ElevenLabsTTSPlayer()

    @Published var isSpeaking: Bool = false
    @Published var lastError: String?

    private var player: AVAudioPlayer?

    private override init() {
        super.init()
    }

    func stop() {
        player?.stop()
        player?.delegate = nil
        player = nil
        isSpeaking = false
        deactivateAudioSession()
    }

    func speak(_ text: String) {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }

        Task {
            await self._speak(trimmed)
        }
    }

    private func configureAudioSession() {
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(.playback, mode: .spokenAudio, options: [.duckOthers])
        try? session.setActive(true, options: [])
    }

    private func deactivateAudioSession() {
        let session = AVAudioSession.sharedInstance()
        try? session.setActive(false, options: [.notifyOthersOnDeactivation])
        try? session.setCategory(.ambient, mode: .default, options: [.mixWithOthers])
    }

    nonisolated func audioPlayerDidFinishPlaying(_ player: AVAudioPlayer, successfully flag: Bool) {
        Task { @MainActor in
            self.player?.delegate = nil
            self.player = nil
            self.isSpeaking = false
            self.deactivateAudioSession()
        }
    }

    private func resolveVoiceId() -> String? {
        let raw = UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceId)
        let trimmed = raw?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !trimmed.isEmpty else { return nil }
        let name = (UserDefaults.standard.string(forKey: PreferenceKeys.elevenLabsVoiceName) ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return VoicesCache.shared.resolvedVoiceId(storedId: trimmed, storedName: name) ?? trimmed
    }

    private func _speak(_ text: String) async {
        stop()
        lastError = nil
        configureAudioSession()

        guard let token = await AuthService.shared.getAccessToken() else {
            lastError = "Not authenticated."
            return
        }
        guard let voiceId = resolveVoiceId() else {
            lastError = "Pick a voice from Attachments first."
            return
        }

        do {
            let data = try await BackendService.shared.elevenLabsTTS(text: text, voiceId: voiceId, accessToken: token)
            guard !data.isEmpty else {
                lastError = "Empty audio."
                return
            }
            let p = try AVAudioPlayer(data: data)
            self.player = p
            p.delegate = self
            p.prepareToPlay()
            isSpeaking = true
            p.play()
        } catch {
            lastError = error.localizedDescription
            isSpeaking = false
        }
    }
}


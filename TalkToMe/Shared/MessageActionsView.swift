import SwiftUI
import AVFoundation

struct MessageActionsView: View {
    let text: String

    @State private var showCopyCheck: Bool = false

    private static let ttsSynth = AVSpeechSynthesizer()

    private func normalizeLanguageIdentifier(_ raw: String) -> String {
        raw.replacingOccurrences(of: "_", with: "-")
    }

    private func configureAudioSession() {
        let session = AVAudioSession.sharedInstance()
        try? session.setCategory(.playback, mode: .spokenAudio, options: [.duckOthers])
        try? session.setActive(true, options: [])
    }

    private func preferredVoice() -> AVSpeechSynthesisVoice? {
        let currentRaw = Locale.preferredLanguages.first ?? Locale.current.identifier
        let current = normalizeLanguageIdentifier(currentRaw)
        return AVSpeechSynthesisVoice.speechVoices().first(where: { voice in
            voice.name.localizedCaseInsensitiveContains("Siri") && current.hasPrefix(voice.language)
        })
    }

    private func speak(_ string: String) {
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        configureAudioSession()
        if Self.ttsSynth.isSpeaking { Self.ttsSynth.stopSpeaking(at: .immediate) }
        let utterance = AVSpeechUtterance(string: trimmed)
        if let savedId = UserDefaults.standard.string(forKey: PreferenceKeys.ttsVoiceIdentifier), let v = AVSpeechSynthesisVoice(identifier: savedId) {
            utterance.voice = v
        } else {
            utterance.voice = preferredVoice()
        }
        utterance.rate = AVSpeechUtteranceDefaultSpeechRate
        utterance.pitchMultiplier = 1.0
        utterance.volume = 1.0
        Self.ttsSynth.speak(utterance)
    }

    var body: some View {
        HStack(spacing: 12) {
            Button(action: {
                guard !showCopyCheck else { return }
                UIPasteboard.general.string = text
                Haptics.impact(.light)
                showCopyCheck = true
                DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
                    showCopyCheck = false
                }
            }) {
                Image(systemName: showCopyCheck ? "checkmark" : "square.on.square")
                    .font(.system(size: 14))
                    .foregroundColor(Color.secondary)
            }

            Button(action: {
                Haptics.impact(.light)
                speak(text)
            }) {
                Image(systemName: "speaker.wave.2.fill")
                    .font(.system(size: 14))
                    .foregroundColor(Color.secondary)
            }
        }
    }
}



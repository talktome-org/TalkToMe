import SwiftUI

private struct SpeakModePressScaleButtonStyle: ButtonStyle {
    var pressedScale: CGFloat = 0.92
    var pressedOpacity: CGFloat = 0.92

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? pressedScale : 1.0)
            .opacity(configuration.isPressed ? pressedOpacity : 1.0)
            .animation(.spring(response: 0.22, dampingFraction: 0.88, blendDuration: 0.02), value: configuration.isPressed)
    }
}

struct SpeakModePanelView: View {
    @ObservedObject var voiceService: DeepgramStreamingSTTService
    @ObservedObject var ttsService: ElevenLabsStreamingTTSService
    let onStop: () -> Void

    @Environment(\.colorScheme) private var colorScheme

    var body: some View {
        let isSpeaking = ttsService.isSpeaking
        let activityLevel: CGFloat = isSpeaking ? ttsService.speakerLevel : voiceService.spawnLevel

        HStack(spacing: 12) {
            MetalSpeakerOrb(level: activityLevel, isSpeaking: isSpeaking)

            VStack(alignment: .leading, spacing: 2) {
                Text(isSpeaking ? "Speaking" : "Listening")
                    .font(.system(size: 15, weight: .semibold))
                    .foregroundColor(.primary)

                Text(subtitleText(isSpeaking: isSpeaking))
                    .font(.system(size: 13, weight: .medium))
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                    .truncationMode(.tail)
            }

            Spacer(minLength: 0)

            Button(action: {
                Haptics.impact(.medium)
                onStop()
            }) {
                Image(systemName: "xmark")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundColor(.primary)
                    .frame(width: 34, height: 34)
            }
            .buttonStyle(SpeakModePressScaleButtonStyle())
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 12)
        .background(panelBackground)
        .overlay(panelBorder)
        .padding(.horizontal, 16)
        .padding(.bottom, 6)
        .task(id: ttsService.isSpeaking) {
            guard ttsService.isSpeaking else { return }
            // Subtle "metal vibration" haptics while speaking.
            while !Task.isCancelled && ttsService.isSpeaking {
                if ttsService.speakerLevel > 0.16 {
                    Haptics.impact(.light)
                }
                try? await Task.sleep(nanoseconds: 120_000_000)
            }
        }
    }

    private func subtitleText(isSpeaking: Bool) -> String {
        if isSpeaking {
            return "Tap Speak to interrupt"
        }
        let t = voiceService.userTranscript.trimmingCharacters(in: .whitespacesAndNewlines)
        return t.isEmpty ? "Say something…" : t
    }

    @ViewBuilder
    private var panelBackground: some View {
        if #available(iOS 26.0, *) {
            Color.clear
                .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 18, style: .continuous))
        } else {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .fill(.thinMaterial)
        }
    }

    @ViewBuilder
    private var panelBorder: some View {
        if #available(iOS 26.0, *) {
            EmptyView()
        } else {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .strokeBorder(
                    colorScheme == .dark ? Color.white.opacity(0.14) : Color.black.opacity(0.08),
                    lineWidth: 1
                )
        }
    }
}

private struct MetalSpeakerOrb: View {
    let level: CGFloat
    let isSpeaking: Bool

    var body: some View {
        let amp = min(1, max(0, level))
        ZStack {
            MetalOrbView(level: Float(amp), isSpeaking: isSpeaking)
                .frame(width: 44, height: 44)
            Image(systemName: isSpeaking ? "speaker.wave.2.fill" : "mic.fill")
                .font(.system(size: 14, weight: .semibold))
                .foregroundColor(Color.white.opacity(0.86))
                .shadow(color: Color.black.opacity(0.35), radius: 6, x: 0, y: 3)
        }
        .accessibilityHidden(true)
    }
}

#Preview {
    SpeakModePanelView(
        voiceService: DeepgramStreamingSTTService(),
        ttsService: ElevenLabsStreamingTTSService(),
        onStop: {}
    )
}


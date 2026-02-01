import SwiftUI

struct SpeakModeOrbOverlayView: View {
    @ObservedObject var voiceService: DeepgramStreamingSTTService
    @ObservedObject var ttsService: ElevenLabsStreamingTTSService
    let onClose: () -> Void

    var body: some View {
        let isSpeaking = ttsService.isSpeaking
        let activityLevel: CGFloat = isSpeaking ? ttsService.speakerLevel : voiceService.spawnLevel
        let amp = min(1, max(0, activityLevel))

        ZStack {
            Color.black.opacity(0.92)
                .ignoresSafeArea()

            // Orb
            MetalOrbView(level: Float(amp), isSpeaking: isSpeaking)
                .frame(width: 260, height: 260)
                .shadow(color: Color.black.opacity(0.5), radius: 22, x: 0, y: 10)
                .accessibilityHidden(true)

            // Close (top-left)
            VStack {
                HStack {
                    Button(action: {
                        Haptics.impact(.medium)
                        onClose()
                    }) {
                        Image(systemName: "xmark")
                            .font(.system(size: 16, weight: .semibold))
                            .foregroundColor(.white.opacity(0.92))
                            .frame(width: 44, height: 44)
                    }
                    .buttonStyle(.plain)

                    Spacer(minLength: 0)
                }
                .padding(.leading, 8)
                .padding(.top, 6)

                Spacer(minLength: 0)
            }
        }
    }
}

#Preview {
    SpeakModeOrbOverlayView(
        voiceService: DeepgramStreamingSTTService(),
        ttsService: ElevenLabsStreamingTTSService(),
        onClose: {}
    )
}


import SwiftUI
import AVFoundation

struct MessageActionsView: View {
    let text: String

    @State private var showCopyCheck: Bool = false

    @StateObject private var ttsPlayer = ElevenLabsTTSPlayer.shared

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
                ttsPlayer.speak(text)
            }) {
                Image(systemName: "speaker.wave.2.fill")
                    .font(.system(size: 14))
                    .foregroundColor(Color.secondary)
            }
        }
    }
}



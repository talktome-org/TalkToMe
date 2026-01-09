import SwiftUI

struct InputAreaView: View {
    @StateObject private var voiceService = VoiceRecordingService()

    @Binding var inputText: String
    @Binding var isLoading: Bool
    @Binding var focusSnippet: String?

    let isInputFocused: FocusState<Bool>.Binding
    let send: () -> Void
    let stop: () -> Void
    let onVoiceRecordingStart: () -> Void

    @Environment(\.colorScheme) private var colorScheme

    var body: some View {

        let sendSize: CGFloat = 34
        let trimmedInput = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
        let isInputEmpty = trimmedInput.isEmpty

        VStack(alignment: .leading, spacing: 8) {
            if let snippet = focusSnippet, !snippet.isEmpty {
                HStack(alignment: .top, spacing: 8) {
                    Image(systemName: "quote.opening")
                        .foregroundColor(.secondary)
                    Text(snippet)
                        .font(.caption)
                        .foregroundColor(.secondary)
                        .lineLimit(3)
                        .multilineTextAlignment(.leading)
                    Spacer(minLength: 8)
                    Button(action: { withAnimation { focusSnippet = nil } }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(.secondary)
                    }
                    .buttonStyle(.plain)
                }
                .padding(.horizontal, 14)
                .padding(.vertical, 10)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 14, style: .continuous)
                        .strokeBorder(
                            colorScheme == .dark ?
                                Color.white.opacity(0.12) :
                                Color.black.opacity(0.06),
                            lineWidth: 1
                        )
                )
            }

            HStack(alignment: .bottom, spacing: 4) {
                VStack {
                    if voiceService.isShowingRecordingPlaceholder {
                        WaveformPlaceholderView(
                            currentLevel: voiceService.spawnLevel,
                            barWidth: 3,
                            barSpacing: 2,
                            minHeight: 3,
                            maxHeight: 16,
                            color: Color(red: 0.54, green: 0.32, blue: 0.78)
                        )
                        .padding(.horizontal, 4)
                        .padding(.vertical, 10)
                    } else {
                        TextField("Share what's on your mind", text: $inputText, axis: .vertical)
                            .lineLimit(1...5)
                            .lineSpacing(2)
                            .font(.system(size: 17, weight: .regular))
                            .foregroundColor(.primary)
                            .textFieldStyle(.plain)
                            .fixedSize(horizontal: false, vertical: true)
                            .padding(.horizontal, 4)
                            .padding(.vertical, 6)
                            .focused(isInputFocused)
                    }
                }
                .frame(minHeight: 36)

                Button(action: {
                    if isInputEmpty {
                        if voiceService.isRecording {
                            Haptics.impact(.medium)
                            voiceService.stopRecording()
                            inputText = voiceService.transcribedText
                        } else {
                            Haptics.impact(.medium)
                            onVoiceRecordingStart()
                            Task { await voiceService.startRecording() }
                        }
                    } else {
                        Haptics.impact(.light)
                        if isLoading { stop() } else { isInputFocused.wrappedValue = false; send() }
                    }
                }) {
                    ZStack {
                        Circle()
                            .fill(
                                LinearGradient(
                                    colors: [Color(red: 0.5, green: 0.3, blue: 0.7), Color(red: 0.4, green: 0.2, blue: 0.6)],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                            .frame(width: sendSize, height: sendSize)
                            .shadow(color: Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.3), radius: 4, x: 0, y: 2)

                        ZStack {
                            if isInputEmpty {
                                Image(systemName: voiceService.isRecording ? "stop.fill" : "mic.fill")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(.white)
                                    .transition(.asymmetric(
                                        insertion: .move(edge: .leading).combined(with: .opacity),
                                        removal: .move(edge: .leading).combined(with: .opacity)
                                    ))
                            } else {
                                Image(systemName: isLoading ? "stop.fill" : "arrow.up")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(.white)
                                    .transition(.asymmetric(
                                        insertion: .move(edge: .trailing).combined(with: .opacity),
                                        removal: .move(edge: .trailing).combined(with: .opacity)
                                    ))
                            }
                        }
                    }
                }
                .scaleEffect(voiceService.isRecording && isInputEmpty ? 1.1 : 1.0)
                .animation(.spring(response: 0.3, dampingFraction: 0.7), value: voiceService.isRecording)
                .animation(.spring(response: 0.35, dampingFraction: 0.85, blendDuration: 0.2), value: inputText)
            }
        }
        .padding(.leading, 16)
        .padding(.trailing, 8)
        .padding(.vertical, 5)
        .background(
            Group {
                if #available(iOS 26.0, *) {
                    Color.clear
                        .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 25, style: .continuous))
                } else {
                    RoundedRectangle(cornerRadius: 25, style: .continuous)
                        .fill(.ultraThinMaterial)
                }
            }
        )
        .clipShape(RoundedRectangle(cornerRadius: 25, style: .continuous))
        .overlay(
            Group {
                if #available(iOS 26.0, *) {
                    EmptyView()
                } else {
                    RoundedRectangle(cornerRadius: 25, style: .continuous)
                        .strokeBorder(
                            colorScheme == .dark
                                ? Color.white.opacity(0.15)
                                : Color.black.opacity(0.08),
                            lineWidth: 1
                        )
                }
            }
        )
        .shadow(color: Color.black.opacity(0.08), radius: 12, x: 0, y: 4)
        .padding(.horizontal, 16)
        .padding(.vertical, 12)
        .animation(.easeInOut(duration: 0.2), value: isInputFocused.wrappedValue)
    }
}

#Preview {
    @FocusState var isFocused: Bool
    InputAreaView(
        inputText: .constant(""),
        isLoading: .constant(false),
        focusSnippet: .constant(nil),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceRecordingStart: {}
    )
}




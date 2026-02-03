import SwiftUI
import UIKit

struct ChatSendButtonFramePreferenceKey: PreferenceKey {
    static var defaultValue: CGRect = .zero
    static func reduce(value: inout CGRect, nextValue: () -> CGRect) {
        value = nextValue()
    }
}

struct ChatInputBarFramePreferenceKey: PreferenceKey {
    static var defaultValue: CGRect = .zero
    static func reduce(value: inout CGRect, nextValue: () -> CGRect) {
        value = nextValue()
    }
}

private struct InputAreaGlassModifier: ViewModifier {
    let shape: RoundedRectangle

    func body(content: Content) -> some View {
        if #available(iOS 26.0, *) {
            content.glassEffect(.regular.interactive(), in: shape)
        } else {
            content
                .background(.thinMaterial, in: shape)
                .overlay(shape.strokeBorder(Color.primary.opacity(0.10), lineWidth: 1))
        }
    }
}

struct InputAreaView: View {
    let isVoiceRecording: Bool
    let voiceModeEnabled: Bool
    let isSpeakModeActive: Bool
    let onSpeakToggle: () -> Void

    @Binding var inputText: String
    @Binding var isLoading: Bool
    @Binding var focusSnippet: String?
    @Binding var pendingAttachments: [PendingAttachment]
    @Binding var isMediaPanelVisible: Bool

    let isInputFocused: FocusState<Bool>.Binding
    let send: () -> Void
    let stop: () -> Void
    let onVoiceRecordingStart: () -> Void
    let onVoiceModeStart: () -> Void
    let onVoiceModeStop: () -> Void

    private let sendSize: CGFloat = 40
    private let composerMinHeight: CGFloat = 22
    private let composerMaxHeight: CGFloat = 120

    @Environment(\.colorScheme) private var colorScheme


    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        return !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    private static let inputBarShape = RoundedRectangle(cornerRadius: 22, style: .continuous)

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            focusSnippetView
            contentRow
        }
        .padding(.leading, 18)
        .padding(.trailing, 10)
        .padding(.vertical, 10)
        .background(
            GeometryReader { proxy in
                Color.clear.preference(
                    key: ChatInputBarFramePreferenceKey.self,
                    value: proxy.frame(in: .named("ChatScreen"))
                )
            }
        )
        .clipShape(Self.inputBarShape)
        .modifier(InputAreaGlassModifier(shape: Self.inputBarShape))
        .padding(.horizontal, 16)
        .animation(.easeInOut(duration: 0.2), value: isInputFocused.wrappedValue)
    }

    @ViewBuilder
    private var focusSnippetView: some View {
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
                        colorScheme == .dark ? Color.white.opacity(0.12) : Color.black.opacity(0.06),
                        lineWidth: 1
                    )
            )
        }
    }

    private var contentRow: some View {
        inputStack
    }

    private var inputStack: some View {
        VStack(alignment: .leading, spacing: 6) {
            attachmentsStrip
            composerRow
        }
        .frame(minHeight: 56)
    }

    private var composerRow: some View {
        HStack(alignment: .bottom, spacing: 10) {
            attachmentsButton
                .padding(.bottom, 2)

            textFieldOrWaveform
                .frame(maxWidth: .infinity, alignment: .bottom)

            if !canSend && !isVoiceRecording {
                speakButton
            }
            sendButton
        }
    }

    private var speakButton: some View {
        Button(action: {
            Haptics.impact(.medium)
            isInputFocused.wrappedValue = false
            onSpeakToggle()
        }) {
            HStack(spacing: 6) {
                Image(systemName: isSpeakModeActive ? "speaker.wave.2.fill" : "speaker.wave.2")
                    .font(.system(size: 14, weight: .semibold))
                Text("Speak")
                    .font(.system(size: 14, weight: .semibold))
            }
            .foregroundColor(.primary)
            .padding(.horizontal, 12)
            .padding(.vertical, 8)
            .background {
                if isSpeakModeActive {
                    Capsule().fill(Color.primary.opacity(0.12))
                } else {
                    Capsule().fill(.thinMaterial)
                }
            }
            .overlay(
                Capsule()
                    .strokeBorder(
                        colorScheme == .dark ? Color.white.opacity(0.12) : Color.black.opacity(0.06),
                        lineWidth: 1
                    )
            )
        }
        .buttonStyle(.plain)
    }

    private var attachmentsButton: some View {
        Button(action: {
            Haptics.impact(.light)
            isInputFocused.wrappedValue = false
            withAnimation(.easeInOut(duration: 0.22)) {
                isMediaPanelVisible = true
            }
        }) {
            Image(systemName: "paperclip")
                .font(.system(size: 15, weight: .semibold))
                .foregroundColor(.primary)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
                .background(.thinMaterial, in: Capsule())
                .overlay(
                    Capsule()
                        .strokeBorder(
                            colorScheme == .dark ? Color.white.opacity(0.12) : Color.black.opacity(0.06),
                            lineWidth: 1
                        )
                )
        }
        .buttonStyle(.plain)
        .disabled(isVoiceRecording)
        .opacity(isVoiceRecording ? 0.5 : 1.0)
    }

    @ViewBuilder
    private var attachmentsStrip: some View {
        if !pendingAttachments.isEmpty {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    ForEach(pendingAttachments) { att in
                        attachmentThumb(att)
                    }
                }
                .padding(.horizontal, 4)
                .padding(.top, 2)
                .padding(.bottom, 6)
            }
        }
    }

    private func attachmentThumb(_ att: PendingAttachment) -> some View {
        ZStack(alignment: .topTrailing) {
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(.thinMaterial)
                .frame(width: 54, height: 54)
                .overlay(attachmentThumbContent(att))

            Button(action: {
                Haptics.impact(.light)
                withAnimation(.spring(response: 0.35, dampingFraction: 0.9)) {
                    pendingAttachments.removeAll { $0.id == att.id }
                }
            }) {
                Image(systemName: "xmark.circle.fill")
                    .foregroundColor(.secondary)
                    .background(Color.clear)
            }
            .buttonStyle(.plain)
            .padding(4)
        }
    }

    @ViewBuilder
    private func attachmentThumbContent(_ att: PendingAttachment) -> some View {
        if case .image(let data, _) = att.kind, let uiImage = UIImage(data: data) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFill()
                .frame(width: 54, height: 54)
                .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        } else {
            Image(systemName: "doc")
                .font(.system(size: 16, weight: .semibold))
                .foregroundColor(.secondary)
        }
    }

    @ViewBuilder
    private var textFieldOrWaveform: some View {
        let focusBinding = Binding(
            get: { isInputFocused.wrappedValue },
            set: { isInputFocused.wrappedValue = $0 }
        )

        ZStack(alignment: .topLeading) {
            GrowingTextView(
                text: $inputText,
                isFocused: focusBinding,
                minHeight: composerMinHeight,
                maxHeight: composerMaxHeight,
                font: .systemFont(ofSize: 17, weight: .regular),
                textColor: .label,
                // We still write into the field live while recording; disabling avoids the user
                // fighting the transcriber for the cursor.
                isEditable: !isVoiceRecording
            )
            .frame(minHeight: composerMinHeight, maxHeight: composerMaxHeight, alignment: .bottom)

            if inputText.isEmpty {
                Text("Message TalkToMe")
                    .font(.system(size: 17, weight: .regular))
                    .foregroundColor(.secondary)
                    .padding(.top, 1)
                    .allowsHitTesting(false)
            }
        }
        .padding(.horizontal, 4)
        .padding(.top, 6)
        .padding(.bottom, 2)
    }


    private var sendButton: some View {
        Button(action: sendButtonTapped) {
            let shadowOpacity: Double = colorScheme == .dark ? 0.55 : 0.25
            let showCircularSend = canSend && !isVoiceRecording
            ZStack {
                if showCircularSend {
                    Circle()
                        .fill(
                            LinearGradient(
                                colors: [Color(white: 0.20), Color(white: 0.06)],
                                startPoint: .topLeading,
                                endPoint: .bottomTrailing
                            )
                        )
                        .frame(width: sendSize, height: sendSize)
                        .shadow(color: Color.black.opacity(shadowOpacity), radius: 4, x: 0, y: 2)

                    Image(systemName: isLoading ? "stop.fill" : "arrow.up")
                        .font(.system(size: 15, weight: .semibold))
                        .foregroundColor(.white)
                        .transition(.asymmetric(
                            insertion: .move(edge: .trailing).combined(with: .opacity),
                            removal: .move(edge: .trailing).combined(with: .opacity)
                        ))
                } else {
                    // Voice icon with no circular background (match Grok-style minimal mic).
                    Group {
                        if isVoiceRecording {
                            Image(systemName: "stop.fill")
                                .font(.system(size: 18, weight: .semibold))
                                .foregroundColor(.primary)
                        } else {
                            Text("🎙️")
                                .font(.system(size: 18, weight: .semibold))
                        }
                    }
                    .transition(.asymmetric(
                        insertion: .move(edge: .leading).combined(with: .opacity),
                        removal: .move(edge: .leading).combined(with: .opacity)
                    ))
                }
            }
            .frame(width: 44, height: 44)
        }
        .background(
            GeometryReader { proxy in
                Color.clear.preference(
                    key: ChatSendButtonFramePreferenceKey.self,
                    value: proxy.frame(in: .named("ChatScreen"))
                )
            }
        )
        .scaleEffect(isVoiceRecording ? 1.1 : 1.0)
        .animation(.spring(response: 0.3, dampingFraction: 0.7), value: isVoiceRecording)
        .animation(.spring(response: 0.35, dampingFraction: 0.85, blendDuration: 0.2), value: inputText)
        .buttonStyle(.plain)
    }

    private func sendButtonTapped() {
        // While dictating, keep this as a "quit" button (stop only; do not send).
        if isVoiceRecording {
            Haptics.impact(.medium)
            onVoiceModeStop()
            return
        }

        if !canSend {
            if isSpeakModeActive {
                Haptics.impact(.medium)
                onSpeakToggle()
                return
            }
            Haptics.impact(.medium)
            onVoiceRecordingStart()
            onVoiceModeStart()
            return
        }

        Haptics.impact(.light)
        if isLoading {
            stop()
        } else {
            withAnimation(.easeInOut(duration: 0.22)) {
                isMediaPanelVisible = false
            }
            // Keep the keyboard stable on send (helps the "fly-in" animation and feels snappier).
            isInputFocused.wrappedValue = true
            send()
        }
    }

}

#Preview {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: false,
        onSpeakToggle: {},
        inputText: .constant(""),
        isLoading: .constant(false),
        focusSnippet: .constant(nil),
        pendingAttachments: .constant([]),
        isMediaPanelVisible: .constant(false),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceRecordingStart: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}




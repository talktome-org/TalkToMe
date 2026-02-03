import SwiftUI

struct InputAreaView: View {
    let isVoiceRecording: Bool
    let voiceModeEnabled: Bool
    let isSpeakModeActive: Bool
    let onSpeakToggle: () -> Void

    @Binding var inputText: String
    @Binding var isLoading: Bool
    @Binding var pendingAttachments: [PendingAttachment]
    @Binding var isMediaPanelVisible: Bool

    let isInputFocused: FocusState<Bool>.Binding
    let send: () -> Void
    let stop: () -> Void
    let onVoiceModeStart: () -> Void
    let onVoiceModeStop: () -> Void

    @Environment(\.colorScheme) private var colorScheme

    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if !pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(pendingAttachments) { att in
                            ZStack(alignment: .topTrailing) {
                                RoundedRectangle(cornerRadius: 12, style: .continuous)
                                    .fill(.thinMaterial)
                                    .frame(width: 54, height: 54)
                                    .overlay {
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
                    }
                    .padding(.horizontal, 4)
                }
            }

            TextField("Message TalkToMe", text: $inputText, axis: .vertical)
                .font(.system(size: 17, weight: .regular))
                .textFieldStyle(.plain)
                .focused(isInputFocused)
                .disabled(isVoiceRecording)
                .lineLimit(1...5)
                .padding(.horizontal, 4)
                .padding(.vertical, 4)

            HStack {
                if #available(iOS 26.0, *) {
                    Button(action: {
                        isInputFocused.wrappedValue = false
                        isMediaPanelVisible = true
                    }) {
                        Image(systemName: "paperclip")
                            .font(.system(size: 15, weight: .semibold))
                            .foregroundColor(.primary)
                            .padding(.horizontal, 12)
                            .padding(.vertical, 8)
                    }
                    .buttonStyle(.plain)
                    .glassEffect(.regular.interactive(), in: Circle())
                    .disabled(isVoiceRecording)
                }

                Spacer()

                if #available(iOS 26.0, *) {
                    Button(action: {
                        Haptics.impact(.medium)
                        isInputFocused.wrappedValue = false
                        onSpeakToggle()
                    }) {
                        Text("Speak")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundColor(.primary)
                            .padding(.horizontal, 12)
                            .padding(.vertical, 8)
                    }
                    .buttonStyle(.plain)
                    .glassEffect(.regular.interactive(), in: Capsule())
                    .disabled(isVoiceRecording)
                }

                Button(action: {
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
                        isInputFocused.wrappedValue = true
                        send()
                    }
                }) {
                    let showSend = canSend && !isVoiceRecording
                    let showMicEmoji = !showSend && !isVoiceRecording
                    let iconName: String = {
                        if isVoiceRecording { return "stop.fill" }
                        return isLoading ? "stop.fill" : "arrow.up"
                    }()

                    ZStack {
                        if showMicEmoji {
                            Text("🎙️")
                                .font(.system(size: 18, weight: .semibold))
                        } else {
                            Image(systemName: iconName)
                                .font(.system(size: 16, weight: .semibold))
                                .foregroundColor(.white)
                        }
                    }
                    .frame(width: 44, height: 44)
                }
                .scaleEffect(isVoiceRecording ? 1.1 : 1.0)
                .animation(.spring(response: 0.3, dampingFraction: 0.7), value: isVoiceRecording)
                .animation(.spring(response: 0.35, dampingFraction: 0.85), value: canSend)
                .buttonStyle(.plain)
            }
        }
        .frame(minHeight: 88, alignment: .bottomLeading)
        .padding(.leading, 14)
        .padding(.trailing, 8)
        .padding(.vertical, 8)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .padding(.horizontal, 28)
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
        pendingAttachments: .constant([]),
        isMediaPanelVisible: .constant(false),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}
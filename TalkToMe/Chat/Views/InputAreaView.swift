import SwiftUI

struct InputAreaView: View {
    let isVoiceRecording: Bool
    let voiceModeEnabled: Bool
    let isSpeakModeActive: Bool
    let speakModePhase: SpeakModePhase
    let speakerLevel: CGFloat
    let micLevel: CGFloat
    let onSpeakToggle: () -> Void

    @Binding var inputText: String
    @Binding var isLoading: Bool
    @Binding var pendingAttachments: [PendingAttachment]

    let isInputFocused: FocusState<Bool>.Binding
    let send: () -> Void
    let stop: () -> Void
    let onVoiceModeStart: () -> Void
    let onVoiceModeStop: () -> Void

    @State private var isMediaPanelVisible: Bool = false
    @State private var pendingPhotoSelections: [String: PendingAttachment] = [:]
    @State private var attachmentIdToAssetId: [UUID: String] = [:]

    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
    @Environment(\.colorScheme) private var colorScheme

    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    var body: some View {
        if #available(iOS 26.0, *) {
            VStack(spacing: 12) {
                if isSpeakModeActive {
                    SpeechInputDrawerView(
                        phase: speakModePhase,
                        micLevel: micLevel,
                        speakerLevel: speakerLevel,
                        onCancel: {
                            withAnimation(.spring(response: 0.2, dampingFraction: 0.85)) {
                                onSpeakToggle()
                            }
                        }
                    )
                    .transition(.asymmetric(
                        insertion: .push(from: .bottom),
                        removal: .push(from: .top)
                    ))
                }

                VStack(alignment: .leading, spacing: 8) {
                    if !pendingAttachments.isEmpty {
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(spacing: 10) {
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
                                                if let assetId = attachmentIdToAssetId[att.id] {
                                                    pendingPhotoSelections.removeValue(forKey: assetId)
                                                    attachmentIdToAssetId.removeValue(forKey: att.id)
                                                }
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
                                    .transition(.scale.combined(with: .opacity))
                                }
                            }
                            .padding(.horizontal, 4)
                            .padding(.vertical, 2)
                        }
                        .transition(.asymmetric(
                            insertion: .scale(scale: 0.8).combined(with: .opacity),
                            removal: .scale(scale: 0.8).combined(with: .opacity)
                        ))
                    }

                    TextField("Message TalkToMe", text: $inputText, axis: .vertical)
                        .font(.system(size: 17, weight: .regular))
                        .textFieldStyle(.plain)
                        .focused(isInputFocused)
                        .disabled(isVoiceRecording)
                        .lineLimit(1...5)
                        .padding(.horizontal, 4)
                        .padding(.vertical, 4)
                        .padding(.top, !pendingAttachments.isEmpty ? 2 : 0)
                        .padding(.bottom, 2)
                        .offset(y: -4)

                    HStack(spacing: 8) {
                        Button(action: {
                            isInputFocused.wrappedValue = false
                            isMediaPanelVisible = true
                        }) {
                            Image(systemName: "paperclip")
                                .font(.system(size: 15, weight: .semibold))
                                .foregroundColor(.primary)
                                .frame(width: 32, height: 32)
                                .background(.ultraThinMaterial, in: Circle())
                        }
                        .buttonStyle(.plain)
                        .disabled(isVoiceRecording || isSpeakModeActive)

                        Spacer(minLength: 0)

                        if isVoiceRecording || isSpeakModeActive {
                            // Single End button for both modes
                            Button(action: {
                                Haptics.impact(.medium)
                                if isVoiceRecording {
                                    onVoiceModeStop()
                                }
                                if isSpeakModeActive {
                                    withAnimation(.spring(response: 0.2, dampingFraction: 0.85)) {
                                        onSpeakToggle()
                                    }
                                }
                            }) {
                                Text("End")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(.red)
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 8)
                                    .background(.ultraThinMaterial, in: Capsule())
                            }
                            .buttonStyle(.plain)
                            .transition(.opacity)
                        } else if canSend || isLoading {
                            // Send/Stop button - replaces mic and speak when there's content
                            Button(action: {
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
                                Text(isLoading ? "Stop" : "Send")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(isLoading ? .red : .primary)
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 8)
                                    .background(.ultraThinMaterial, in: Capsule())
                            }
                            .buttonStyle(.plain)
                            .transition(.opacity)
                        } else {
                            // Mic button
                            Button(action: {
                                Haptics.impact(.medium)
                                onVoiceModeStart()
                            }) {
                                Image(systemName: "mic")
                                    .font(.system(size: 15, weight: .semibold))
                                    .foregroundColor(.primary)
                                    .frame(width: 32, height: 32)
                                    .background(.ultraThinMaterial, in: Circle())
                            }
                            .buttonStyle(.plain)
                            .transition(.asymmetric(
                                insertion: .move(edge: .trailing).combined(with: .opacity),
                                removal: .move(edge: .trailing).combined(with: .opacity)
                            ))

                            // Speak to buddy button
                            Button(action: {
                                Haptics.impact(.medium)
                                isInputFocused.wrappedValue = false
                                withAnimation(.spring(response: 0.2, dampingFraction: 0.85)) {
                                    onSpeakToggle()
                                }
                            }) {
                                let buddyName = selectedVoiceName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? "Buddy" : selectedVoiceName
                                Text("Speak to \(buddyName)")
                                    .font(.system(size: 14, weight: .semibold))
                                    .foregroundColor(.primary)
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 8)
                                    .background(.ultraThinMaterial, in: Capsule())
                            }
                            .buttonStyle(.plain)
                            .transition(.asymmetric(
                                insertion: .move(edge: .trailing).combined(with: .opacity),
                                removal: .move(edge: .trailing).combined(with: .opacity)
                            ))
                        }
                    }
                    .animation(.smooth(duration: 0.25), value: isVoiceRecording)
                    .animation(.smooth(duration: 0.25), value: isSpeakModeActive)
                    .animation(.smooth(duration: 0.25), value: canSend)
                }
                .frame(minHeight: 78, alignment: .bottomLeading)
                .padding(.leading, 14)
                .padding(.trailing, 8)
                .padding(.vertical, 8)
                .animation(.spring(response: 0.35, dampingFraction: 0.8), value: pendingAttachments.count)
                .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 28, style: .continuous))
                .sheet(isPresented: $isMediaPanelVisible) {
                    MediaPickerPanelView(
                        attachments: $pendingAttachments,
                        pendingPhotoSelections: $pendingPhotoSelections,
                        attachmentIdToAssetId: $attachmentIdToAssetId
                    )
                    .presentationDetents([.medium])
                    .presentationDragIndicator(.visible)
                }
            }
            .padding(.horizontal)
            .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isSpeakModeActive)
        }
    }
}

#Preview("Default") {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: false,
        speakModePhase: .idle,
        speakerLevel: 0,
        micLevel: 0,
        onSpeakToggle: {},
        inputText: .constant(""),
        isLoading: .constant(false),
        pendingAttachments: .constant([]),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}

#Preview("Speak Mode - Listening") {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: true,
        speakModePhase: .listening,
        speakerLevel: 0,
        micLevel: 0.6,
        onSpeakToggle: {},
        inputText: .constant(""),
        isLoading: .constant(false),
        pendingAttachments: .constant([]),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}

#Preview("Speak Mode - Processing") {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: true,
        speakModePhase: .processing,
        speakerLevel: 0,
        micLevel: 0,
        onSpeakToggle: {},
        inputText: .constant(""),
        isLoading: .constant(false),
        pendingAttachments: .constant([]),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}

#Preview("Speak Mode - Answering") {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: true,
        speakModePhase: .answering,
        speakerLevel: 0.7,
        micLevel: 0,
        onSpeakToggle: {},
        inputText: .constant(""),
        isLoading: .constant(false),
        pendingAttachments: .constant([]),
        isInputFocused: $isFocused,
        send: {},
        stop: {},
        onVoiceModeStart: {},
        onVoiceModeStop: {}
    )
}

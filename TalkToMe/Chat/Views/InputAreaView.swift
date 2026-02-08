import SwiftUI
import UIKit

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
    @State private var inputAreaWidth: CGFloat = 0

    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
    @Environment(\.colorScheme) private var colorScheme

    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    /// Hide the ghost as soon as any text is typed; show it again when input is cleared.
    private var shouldHideGhostForText: Bool {
        !trimmedInput.isEmpty
    }

    /// Approximate text width available inside the capsule when the ghost is visible (collapsed layout).
    private var collapsedTextWidthForMeasurement: CGFloat {
        guard inputAreaWidth > 0 else { return 0 }

        // These constants mirror the current layout values (use unfocused padding for measurement).
        let outerHorizontalPadding: CGFloat = 32 * 2
        let outerSpacing: CGFloat = 6 * 2 // attachments<->capsule and capsule<->ghost
        let attachmentsButtonWidth: CGFloat = 42
        let ghostButtonWidth: CGFloat = 44

        // Capsule padding (leading 14 + trailing 10)
        let capsuleHorizontalPadding: CGFloat = 14 + 10

        // Reserve space inside the capsule so text doesn't collide with the trailing control.
        let trailingAccessoryInset: CGFloat = 44

        let reserved = outerHorizontalPadding
            + outerSpacing
            + attachmentsButtonWidth
            + ghostButtonWidth
            + capsuleHorizontalPadding
            + trailingAccessoryInset

        return max(0, inputAreaWidth - reserved)
    }

    private func estimatedLineCount(for text: String, width: CGFloat) -> Int {
        let font = UIFont.systemFont(ofSize: 17, weight: .regular)
        let lineHeight = font.lineHeight
        guard width > 0, lineHeight > 0 else { return 1 }

        // Use a space so empty text counts as one line.
        let raw = text.isEmpty ? " " : text
        let rect = (raw as NSString).boundingRect(
            with: CGSize(width: width, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading],
            attributes: [.font: font],
            context: nil
        )
        return max(1, Int(ceil(rect.height / lineHeight)))
    }

    private var barColor: Color {
        switch speakModePhase {
        case .idle: return .gray.opacity(0.5)
        case .listening: return .red.opacity(0.8)
        case .processing: return .orange.opacity(0.8)
        case .answering: return .blue.opacity(0.8)
        }
    }

    private var activeLevel: CGFloat {
        switch speakModePhase {
        case .listening: return micLevel
        case .answering: return speakerLevel
        default: return 0
        }
    }

    private var ghostVideoName: String {
        selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var hasGhostVideo: Bool {
        guard !ghostVideoName.isEmpty else { return false }
        return Bundle.main.url(forResource: ghostVideoName, withExtension: "mp4") != nil
    }

    @ViewBuilder
    private var ghostButtonContent: some View {
        Group {
            if hasGhostVideo {
                TransparentVideoPlayerView(
                    videoName: ghostVideoName,
                    videoExtension: "mp4"
                )
                .id(ghostVideoName) // Force recreation when voice changes
            } else {
                // Fallback: empty rounded rect with material background
                RoundedRectangle(cornerRadius: 7, style: .continuous)
                    .fill(.ultraThinMaterial)
            }
        }
        .frame(width: 44, height: 44)
    }

    // MARK: - Attachments button (Part 1)
    @available(iOS 26.0, *)
    @ViewBuilder
    private var attachmentsButton: some View {
        Button(action: {
            isInputFocused.wrappedValue = false
            isMediaPanelVisible = true
        }) {
            Image(systemName: "paperclip")
                .font(.system(size: 19, weight: .semibold))
                .foregroundColor(.primary)
                .frame(width: 42, height: 42)
        }
        .buttonStyle(.plain)
        .glassEffect(.regular.interactive(), in: Circle())
        .disabled(isVoiceRecording || isSpeakModeActive)
        .offset(y: -2)
    }

    // MARK: - Message capsule (Part 2)
    @available(iOS 26.0, *)
    @ViewBuilder
    private var messageCapsule: some View {
        let trailingAccessoryInset: CGFloat = (canSend || isLoading) ? 76 : 44

        VStack(alignment: .leading, spacing: 6) {
            // Attachments preview row
            if !pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(pendingAttachments) { att in
                            ZStack(alignment: .topTrailing) {
                                RoundedRectangle(cornerRadius: 10, style: .continuous)
                                    .fill(.ultraThinMaterial)
                                    .frame(width: 48, height: 48)
                                    .overlay {
                                        if case .image(let data, _) = att.kind, let uiImage = UIImage(data: data) {
                                            Image(uiImage: uiImage)
                                                .resizable()
                                                .scaledToFill()
                                                .frame(width: 48, height: 48)
                                                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                                        } else {
                                            Image(systemName: "doc")
                                                .font(.system(size: 14, weight: .semibold))
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
                                        .font(.system(size: 16))
                                        .foregroundColor(.secondary)
                                        .background(Color.clear)
                                }
                                .buttonStyle(.plain)
                                .offset(x: 4, y: -4)
                            }
                            .transition(.scale.combined(with: .opacity))
                        }
                    }
                }
                .transition(.asymmetric(
                    insertion: .scale(scale: 0.8).combined(with: .opacity),
                    removal: .scale(scale: 0.8).combined(with: .opacity)
                ))
            }

            // Text field
            TextField("Message TalkToMe", text: $inputText, axis: .vertical)
                .font(.system(size: 17, weight: .regular))
                .textFieldStyle(.plain)
                .focused(isInputFocused)
                .disabled(isVoiceRecording || isSpeakModeActive)
                .lineLimit(1...5)

        }
        // Reserve room for the trailing control so text never sits under it.
        .padding(.trailing, trailingAccessoryInset)
        .padding(.leading, 14)
        .padding(.trailing, 10)
        .padding(.vertical, 8)
        .frame(minHeight: 46)
        .frame(maxWidth: .infinity, alignment: .leading)
        .layoutPriority(1)
        // Anchor trailing control to the bottom-right of the *final* padded bar,
        // so it doesn't start a few points high and then settle.
        .overlay(alignment: .bottomTrailing) {
            Group {
                if isSpeakModeActive {
                    // Show animated bars when speak mode is active
                    AnimatedBarsView(
                        level: activeLevel,
                        color: barColor,
                        isAnimating: speakModePhase == .listening || speakModePhase == .answering,
                        isPulsing: speakModePhase == .processing
                    )
                    .frame(height: 32)
                    .transition(.scale.combined(with: .opacity))
                } else if isVoiceRecording {
                    // End button for dictation mode
                    Button(action: {
                        Haptics.impact(.medium)
                        onVoiceModeStop()
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
                } else {
                    // Mic + optional Send/Stop button
                    HStack(spacing: 4) {
                        // Mic button — always visible
                        Button(action: {
                            Haptics.impact(.medium)
                            onVoiceModeStart()
                        }) {
                            Image(systemName: "mic")
                                .font(.system(size: 17, weight: .semibold))
                                .foregroundColor(.primary)
                                .frame(width: 32, height: 32)
                        }
                        .buttonStyle(.plain)

                        if canSend || isLoading {
                            // Send/Stop button
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
                                Image(systemName: isLoading ? "stop.fill" : "arrow.up")
                                    .font(.system(size: 17, weight: .semibold))
                                    .foregroundColor(isLoading ? .red : .primary)
                                    .frame(width: 32, height: 32)
                            }
                            .buttonStyle(.plain)
                            .transition(.scale.combined(with: .opacity))
                        }
                    }
                }
            }
            .padding(.trailing, 10)
            .padding(.bottom, 8)
        }
        .animation(.spring(response: 0.35, dampingFraction: 0.8), value: pendingAttachments.count)
        .animation(.smooth(duration: 0.25), value: isVoiceRecording)
        .animation(.smooth(duration: 0.25), value: canSend)
        .animation(.spring(response: 0.25, dampingFraction: 0.8), value: isSpeakModeActive)
        .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
    }

    // MARK: - Ghost button (Part 3)
    @ViewBuilder
    private var ghostButton: some View {
        Button(action: {
            Haptics.impact(.medium)
            isInputFocused.wrappedValue = false
            withAnimation(.spring(response: 0.2, dampingFraction: 0.85)) {
                onSpeakToggle()
            }
        }) {
            ghostButtonContent
        }
        .buttonStyle(.plain)
        .offset(x: -1, y: -1)
    }

    var body: some View {
        if #available(iOS 26.0, *) {
            HStack(alignment: .bottom, spacing: 6) {
                // Part 1: Attachments button with glass circle
                attachmentsButton

                // Part 2: Message capsule
                messageCapsule

                // Part 3: Ghost button (no glass effect) - hidden when typing
                if !shouldHideGhostForText {
                    ghostButton
                        .transition(.scale.combined(with: .opacity))
                }
            }
            .padding(.vertical, 6)
            .padding(.horizontal, isInputFocused.wrappedValue ? 16 : 32)
            .background(
                GeometryReader { geo in
                    Color.clear.onChange(of: geo.size.width, initial: true) { _, newWidth in
                        inputAreaWidth = newWidth
                    }
                }
            )
            .animation(.spring(response: 0.35, dampingFraction: 0.85), value: isInputFocused.wrappedValue)
            .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isSpeakModeActive)
            .animation(.spring(response: 0.3, dampingFraction: 0.8), value: shouldHideGhostForText)
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


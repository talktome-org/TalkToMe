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
    @State private var showMicSlash: Bool = false
    @State private var hideMicForStop: Bool = false

    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""
    @Environment(\.colorScheme) private var colorScheme

    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    /// Hide the ghost when typing or when voice recording is active.
    private var shouldHideGhost: Bool {
        !trimmedInput.isEmpty || isVoiceRecording
    }

    /// Approximate text width available inside the capsule when the ghost is visible (collapsed layout).
    private var collapsedTextWidthForMeasurement: CGFloat {
        guard inputAreaWidth > 0 else { return 0 }

        // These constants mirror the current layout values (use unfocused padding for measurement).
        let outerHorizontalPadding: CGFloat = 32 * 2
        let outerSpacing: CGFloat = 4 * 2 // attachments<->capsule and capsule<->ghost
        let attachmentsButtonWidth: CGFloat = 44
        let ghostButtonWidth: CGFloat = 46

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
        case .listening: return .primary
        case .processing: return .yellow.opacity(0.8)
        case .answering: return .primary
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
        .frame(width: 46, height: 46)
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
                .frame(width: 44, height: 44)
        }
        .buttonStyle(.plain)
        .glassEffect(.regular.interactive(), in: Circle())
        .offset(y: -2)
    }

    // MARK: - Message capsule (Part 2)
    @available(iOS 26.0, *)
    @ViewBuilder
    private var messageCapsule: some View {
        Group {
            if isSpeakModeActive {
                // Speak mode: just animated bars, no glass capsule
                AnimatedBarsView(
                    level: activeLevel,
                    color: barColor,
                    isAnimating: speakModePhase == .listening || speakModePhase == .answering,
                    isPulsing: speakModePhase == .processing
                )
                .frame(height: 32)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
                .frame(minHeight: 46)
                .transition(.blurReplace)
            } else {
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
                    TextField("Message", text: $inputText, axis: .vertical)
                        .font(.system(size: 17, weight: .regular))
                        .textFieldStyle(.plain)
                        .focused(isInputFocused)
                        .lineLimit(1...5)
                }
                // Reserve room for the trailing control so text never sits under it.
                .padding(.trailing, trailingAccessoryInset)
                .padding(.leading, 14)
                .padding(.trailing, 10)
                .padding(.vertical, 8)
                .frame(minHeight: 46)
                .frame(maxWidth: .infinity, alignment: .leading)
                .animation(.spring(response: 0.35, dampingFraction: 0.8), value: pendingAttachments.count)
                .animation(.smooth(duration: 0.25), value: isVoiceRecording)
                .animation(.smooth(duration: 0.25), value: canSend)
                .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
                // Overlay AFTER glass so controls render on top, not tinted by glass
                .overlay(alignment: .bottomTrailing) {
                    HStack(spacing: 4) {
                        // Mic button — appears separately, hidden while recording
                        if !isVoiceRecording && !hideMicForStop {
                            Image(systemName: "mic")
                                .font(.system(size: 19, weight: .semibold))
                                .foregroundColor(.primary)
                                .frame(width: 32, height: 32)
                                .contentShape(Rectangle())
                                .transition(.scale.combined(with: .opacity))
                                .onTapGesture {
                                    Haptics.impact(.medium)
                                    onVoiceModeStart()
                                }
                        }

                        // Trailing slot: square → arrow (same position)
                        if showMicSlash {
                            // Stop button: small square
                            RoundedRectangle(cornerRadius: 3, style: .continuous)
                                .fill(Color.primary)
                                .frame(width: 12, height: 12)
                                .frame(width: 32, height: 32)
                                .contentShape(Circle())
                                .transition(.scale.combined(with: .opacity))
                                .onTapGesture {
                                    Haptics.impact(.medium)
                                    hideMicForStop = true
                                    onVoiceModeStop()
                                    // Transition square → arrow and mic in at the same time
                                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                                        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
                                            showMicSlash = false
                                            hideMicForStop = false
                                        }
                                    }
                                }
                        } else if !isVoiceRecording && (canSend || isLoading) {
                            // Send/Stop button — replaces square in the same slot
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
                                    .font(.system(size: 19, weight: .semibold))
                                    .foregroundColor(isLoading ? .red : .primary)
                                    .frame(width: 32, height: 32)
                            }
                            .buttonStyle(.plain)
                            .transition(.scale.combined(with: .opacity))
                        }
                    }
                    .onChange(of: isVoiceRecording) { _, recording in
                        if recording {
                            DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                                if isVoiceRecording {
                                    withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) {
                                        showMicSlash = true
                                    }
                                }
                            }
                        }
                    }
                    .padding(.trailing, 6)
                    .padding(.bottom, 8)
                }
                .transition(.blurReplace)
            }
        }
        .layoutPriority(isSpeakModeActive ? 0 : 1)
        .animation(.smooth(duration: 0.45), value: isSpeakModeActive)
    }

    // MARK: - Ghost button (Part 3)
    @available(iOS 26.0, *)
    @ViewBuilder
    private var ghostButton: some View {
        Button(action: {
            Haptics.impact(.medium)
            withAnimation(.smooth(duration: 0.45)) {
                onSpeakToggle()
            }
        }) {
            ghostButtonContent
        }
        .buttonStyle(.plain)
    }

    var body: some View {
        if #available(iOS 26.0, *) {
            HStack(alignment: .bottom, spacing: 4) {
                if isSpeakModeActive {
                    // Compact centered glass capsule: waveform + ghost
                    Spacer()
                    HStack(spacing: 10) {
                        messageCapsule
                        ghostButton
                    }
                    .padding(.horizontal, 14)
                    .frame(height: 46)
                    .background {
                        Capsule()
                            .glassEffect(.regular.interactive(), in: Capsule())
                    }
                    Spacer()
                } else {
                    // Normal: attachments + capsule + ghost
                    attachmentsButton

                    messageCapsule

                    if !shouldHideGhost {
                        ghostButton
                            .transition(.blurReplace)
                    }
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
            .animation(.smooth(duration: 0.45), value: isSpeakModeActive)
            .animation(.smooth(duration: 0.35), value: shouldHideGhost)
            .onChange(of: isInputFocused.wrappedValue) { _, focused in
                if !focused {
                    let trimmed = inputText.trimmingCharacters(in: .whitespacesAndNewlines)
                    if trimmed.isEmpty && !inputText.isEmpty {
                        inputText = ""
                    }
                }
            }
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


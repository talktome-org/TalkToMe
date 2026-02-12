import SwiftUI
import UIKit

struct InputAreaView: View {
    let isVoiceRecording: Bool
    let voiceModeEnabled: Bool
    let isSpeakModeActive: Bool
    let isSpeakMicMuted: Bool
    let speakModePhase: SpeakModePhase
    let speakerLevel: CGFloat
    let micLevel: CGFloat
    let onSpeakToggle: () -> Void
    let onMicMuteToggle: () -> Void

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

    @Environment(\.colorScheme) private var colorScheme

    private var trimmedInput: String {
        inputText.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var canSend: Bool {
        !(trimmedInput.isEmpty && pendingAttachments.isEmpty)
    }

    /// Hide the ghost when typing, voice recording, or attachments are present.
    private var shouldHideGhost: Bool {
        !trimmedInput.isEmpty || isVoiceRecording || !pendingAttachments.isEmpty
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
        speakModePhase == .connecting ? .secondary : .primary
    }

    private var activeLevel: CGFloat {
        switch speakModePhase {
        case .idle: return 0
        case .connecting, .listening, .processing:
            return isSpeakMicMuted ? 0 : micLevel
        case .answering:
            // Keep mic activity visible during assistant playback for barge-in confidence.
            return isSpeakMicMuted ? speakerLevel : max(speakerLevel, micLevel * 0.85)
        }
    }

    // Ghost video logic is in GhostVideoContentView below to isolate
    // @AppStorage re-renders from InputAreaView (prevents sheet/photo reload).

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

    // MARK: - Message capsule (Part 2) — always the text field, shrinks in speak mode
    // MARK: - Message capsule (Part 2) — text field + attachment previews inside glass
    @available(iOS 26.0, *)
    @ViewBuilder
    private var messageCapsule: some View {
        let trailingAccessoryInset: CGFloat = 76 // Always reserve full space so text never shifts

        // Attachment preview sizing
        let thumbSize: CGFloat = 100
        let thumbCornerRadius: CGFloat = 16
        let thumbSpacing: CGFloat = 12
        let xButtonOverhang: CGFloat = 6
        let attachmentsTopPadding: CGFloat = 12
        let gapBelowAttachments: CGFloat = 32

        // Reserve vertical space so the TextField never moves when attachments appear.
        // This keeps the placeholder locked in place while the capsule grows upward.
        let reservedAttachmentsHeight: CGFloat = pendingAttachments.isEmpty
            ? 0
            : (attachmentsTopPadding + xButtonOverhang + thumbSize + gapBelowAttachments)

        VStack(alignment: .leading, spacing: 0) {
            // Reserve space for photo thumbnails (rendered via overlay)
            if !pendingAttachments.isEmpty {
                Color.clear
                    .frame(height: reservedAttachmentsHeight)
            }

            // Text field — always at the bottom, grows with multi-line text
            TextField("Message", text: $inputText, axis: .vertical)
                .font(.system(size: 17, weight: .regular))
                .textFieldStyle(.plain)
                .focused(isInputFocused)
                .lineLimit(1...5)
                .padding(.trailing, trailingAccessoryInset)
                .padding(.vertical, 10)
                .padding(.bottom, 2)
        }
        .padding(.leading, 14)
        .padding(.trailing, 10)
        .frame(minHeight: 46, alignment: .bottomLeading)
        .frame(maxWidth: .infinity, alignment: .bottomLeading)
        // Kill ALL inherited animations on capsule content — placeholder must never move
        .transaction { $0.animation = nil }
        .glassEffect(.regular.interactive(), in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        // Attachment thumbnails overlaid at the top inside the capsule
        .overlay(alignment: .topLeading) {
            if !pendingAttachments.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: thumbSpacing) {
                        ForEach(pendingAttachments) { att in
                            ZStack(alignment: .topTrailing) {
                                RoundedRectangle(cornerRadius: thumbCornerRadius, style: .continuous)
                                    .fill(.ultraThinMaterial)
                                    .frame(width: thumbSize, height: thumbSize)
                                    .overlay {
                                        if case .image(let data, _) = att.kind, let uiImage = UIImage(data: data) {
                                            Image(uiImage: uiImage)
                                                .resizable()
                                                .scaledToFill()
                                                .frame(width: thumbSize, height: thumbSize)
                                                .clipShape(RoundedRectangle(cornerRadius: thumbCornerRadius, style: .continuous))
                                        } else {
                                            Image(systemName: "doc")
                                                .font(.system(size: 22, weight: .semibold))
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
                                        .font(.system(size: 22))
                                        .foregroundColor(.secondary)
                                        .background(Color.clear)
                                }
                                .buttonStyle(.plain)
                                .offset(x: xButtonOverhang, y: -xButtonOverhang)
                            }
                            .transition(.opacity)
                        }
                    }
                    // Prevent the offset x-button from getting clipped by the ScrollView
                    .padding(.top, xButtonOverhang)
                    .padding(.trailing, xButtonOverhang)
                }
                .padding(.top, attachmentsTopPadding)
                .padding(.leading, 14)
                .transition(.opacity)
            }
        }
        // Overlay AFTER glass so controls render on top, not tinted by glass
        .overlay(alignment: .bottomTrailing) {
            HStack(spacing: 4) {
                // Mic button — hidden while recording or in speak mode
                if !isVoiceRecording && !hideMicForStop && !isSpeakModeActive {
                    Image(systemName: "mic")
                        .font(.system(size: 19, weight: .semibold))
                        .foregroundColor(.primary)
                        .frame(width: 32, height: 32)
                        .contentShape(Rectangle())
                        .transition(.symbolEffect(.disappear))
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
                } else if !isVoiceRecording && (canSend || (isLoading && !isSpeakModeActive)) {
                    // Send/Stop button — replaces square in the same slot
                    // In speak mode, only show the send arrow (stop is via ghost capsule)
                    Button(action: {
                        Haptics.impact(.light)
                        if isLoading && !isSpeakModeActive {
                            stop()
                        } else if canSend {
                            withAnimation(.easeInOut(duration: 0.22)) {
                                isMediaPanelVisible = false
                            }
                            isInputFocused.wrappedValue = true
                            send()
                        }
                    }) {
                        Image(systemName: (isLoading && !isSpeakModeActive) ? "stop.fill" : "arrow.up")
                            .font(.system(size: 19, weight: .semibold))
                            .foregroundColor((isLoading && !isSpeakModeActive) ? .red : .white)
                            .frame(width: 32, height: 32)
                            .background(
                                RoundedRectangle(cornerRadius: 22, style: .continuous)
                                    .fill((isLoading && !isSpeakModeActive) ? Color.clear : Color.accentColor)
                            )
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
        .layoutPriority(1)
    }

    // MARK: - Ghost button (Part 3)
    @available(iOS 26.0, *)
    @ViewBuilder
    private var ghostButton: some View {
        Button(action: {
            Haptics.impact(.medium)
            // Let the surrounding `.animation(..., value: isSpeakModeActive)` drive the transition.
            // Explicit `withAnimation` here can cause SwiftUI to snapshot the representable,
            // freezing the ghost video during the transition.
            onSpeakToggle()
        }) {
            GhostVideoContentView(isSpeakModeActive: isSpeakModeActive, speakModePhase: speakModePhase)
        }
        .buttonStyle(.plain)
    }

    var body: some View {
        if #available(iOS 26.0, *) {
            HStack(alignment: .bottom, spacing: 4) {
                attachmentsButton

                messageCapsule

                // Mute mic button — separate glass circle, speak mode only (hide when typing)
                if isSpeakModeActive && !shouldHideGhost {
                    Button(action: {
                        Haptics.impact(.light)
                        withAnimation(.smooth(duration: 0.3)) {
                            onMicMuteToggle()
                        }
                    }) {
                        Image(systemName: isSpeakMicMuted ? "mic.slash.fill" : "mic.fill")
                            .font(.system(size: 19, weight: .semibold))
                            .foregroundColor(isSpeakMicMuted ? .red : .primary)
                            .contentTransition(.symbolEffect(.replace))
                            .frame(width: 44, height: 44)
                    }
                    .buttonStyle(.plain)
                    .glassEffect(.regular.interactive(), in: Circle())
                    .transition(.blurReplace)
                }

                // Waveform capsule (speak mode only, hide when typing)
                if isSpeakModeActive && !shouldHideGhost {
                    Button(action: {
                        Haptics.impact(.medium)
                        onSpeakToggle()
                    }) {
                        AnimatedBarsView(
                            level: activeLevel,
                            color: barColor,
                            isAnimating: speakModePhase == .listening || speakModePhase == .answering,
                            isPulsing: speakModePhase == .processing || speakModePhase == .connecting
                        )
                        .frame(height: 28)
                        .padding(.horizontal, 12)
                        .frame(height: 46)
                        .background {
                            Capsule()
                                .glassEffect(.regular.interactive(), in: Capsule())
                        }
                    }
                    .buttonStyle(.plain)
                    .transition(.blurReplace)
                }

                // Ghost — always mounted so the video never restarts
                ghostButton
                    .offset(y: 4)
                    .opacity(!shouldHideGhost ? 1 : 0)
                    .frame(width: !shouldHideGhost ? nil : 0)
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

// MARK: - Ghost Video Content (isolated from InputAreaView to prevent re-renders)

/// Owns the `@AppStorage` for voice selection so that voice changes only
/// re-render this small view — not the entire InputAreaView (which would
/// tear down the media picker sheet and reload photos).
private struct GhostVideoContentView: View {
    let isSpeakModeActive: Bool
    let speakModePhase: SpeakModePhase

    @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = ""

    private var ghostVideoName: String {
        ElevenLabsVoiceSuggestionsView.ghostVideoName(for: selectedVoiceName)
            ?? selectedVoiceName.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var hasGhostVideo: Bool {
        guard !ghostVideoName.isEmpty else { return false }
        return Bundle.main.url(forResource: ghostVideoName, withExtension: "mp4") != nil
    }

    /// Show the animated mp4 whenever speak mode is active.
    private var isSpeechActive: Bool {
        isSpeakModeActive
    }

    var body: some View {
        let size: CGFloat = isSpeakModeActive ? 64 : 76

        Group {
            if hasGhostVideo && isSpeechActive {
                TransparentVideoPlayerView(
                    videoName: ghostVideoName,
                    videoExtension: "mp4",
                    startTime: ElevenLabsVoiceSuggestionsView.ghostStartTimes[ghostVideoName] ?? 0
                )
            } else {
                ghostImage
            }
        }
        .frame(width: size, height: size)
    }

    @ViewBuilder
    private var ghostImage: some View {
        if let uiImage = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: selectedVoiceName) {
            Image(uiImage: uiImage)
                .resizable()
                .scaledToFit()
        } else {
            RoundedRectangle(cornerRadius: 7, style: .continuous)
                .fill(.ultraThinMaterial)
        }
    }
}

#Preview("Default") {
    @FocusState var isFocused: Bool
    InputAreaView(
        isVoiceRecording: false,
        voiceModeEnabled: false,
        isSpeakModeActive: false,
        isSpeakMicMuted: false,
        speakModePhase: .idle,
        speakerLevel: 0,
        micLevel: 0,
        onSpeakToggle: {},
        onMicMuteToggle: {},
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
        isSpeakMicMuted: false,
        speakModePhase: .listening,
        speakerLevel: 0,
        micLevel: 0.6,
        onSpeakToggle: {},
        onMicMuteToggle: {},
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
        isSpeakMicMuted: false,
        speakModePhase: .processing,
        speakerLevel: 0,
        micLevel: 0,
        onSpeakToggle: {},
        onMicMuteToggle: {},
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
        isSpeakMicMuted: false,
        speakModePhase: .answering,
        speakerLevel: 0.7,
        micLevel: 0,
        onSpeakToggle: {},
        onMicMuteToggle: {},
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

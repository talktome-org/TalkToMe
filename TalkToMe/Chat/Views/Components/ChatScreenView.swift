import SwiftUI
import UIKit

struct ChatScreenView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    let onSend: () -> Void

    @AppStorage(PreferenceKeys.voiceModeEnabled) private var voiceModeEnabled: Bool = false

    @State private var isMediaPanelVisible: Bool = false
    @State private var isNearBottom: Bool = true
    @State private var scrollToBottomToken: Int = 0
    @State private var followBottom: Bool = false
    @State private var sendButtonFrame: CGRect = .zero
    @State private var inputBarFrame: CGRect = .zero
    @State private var frozenSendButtonFrame: CGRect? = nil
    @State private var outgoingAnimatingMessageId: UUID? = nil
    @State private var outgoingSourceMessageId: UUID? = nil
    @State private var sendAnimationCleanupTask: Task<Void, Never>? = nil

    @Namespace private var sendAnimationNamespace

    let isInputFocused: FocusState<Bool>.Binding

    init(
        chatViewModel: ChatViewModel,
        onSend: @escaping () -> Void,
        isInputFocused: FocusState<Bool>.Binding
    ) {
        self._chatViewModel = ObservedObject(wrappedValue: chatViewModel)
        self.onSend = onSend
        self.isInputFocused = isInputFocused
    }

    var body: some View {
        let effectiveSendButtonFrame: CGRect = (outgoingAnimatingMessageId != nil) ? (frozenSendButtonFrame ?? sendButtonFrame) : sendButtonFrame
        // Keep the last message + scroll indicator a bit above the composer.
        let chatBottomBreathingRoom: CGFloat = 14

        VStack(spacing: 0) {
            MessagesListView(
                chatViewModel: chatViewModel,
                isNearBottom: $isNearBottom,
                followBottom: $followBottom,
                messages: chatViewModel.messages,
                isInputFocused: isInputFocused.wrappedValue,
                isAssistantTyping: chatViewModel.isAssistantTyping,
                initialJumpToken: chatViewModel.initialJumpToken,
                scrollToBottomToken: scrollToBottomToken,
                sendAnimationNamespace: sendAnimationNamespace,
                outgoingAnimatingMessageId: outgoingAnimatingMessageId,
                outgoingSourceMessageId: outgoingSourceMessageId,
                bottomReservedSpace: chatBottomBreathingRoom
            )
            .contentShape(Rectangle())
            .onTapGesture {
                isInputFocused.wrappedValue = false
            }
        }
        // Outgoing-send animation (bubble glides from send button into chat).
        .overlay {
            if let id = outgoingSourceMessageId,
               let msg = chatViewModel.messages.first(where: { $0.id == id }),
               effectiveSendButtonFrame != .zero {
                UserMessageBubbleView(segments: msg.segments)
                    .matchedGeometryEffect(id: id, in: sendAnimationNamespace, properties: .position, isSource: true)
                    .position(x: effectiveSendButtonFrame.midX, y: effectiveSendButtonFrame.midY)
                    .allowsHitTesting(false)
            }
        }
        .safeAreaInset(edge: .bottom, spacing: 0) {
            if !chatViewModel.isSpeakModeActive {
                InputAreaView(
                    isVoiceRecording: chatViewModel.dictationSTTService.isRecording,
                    voiceModeEnabled: voiceModeEnabled,
                    isSpeakModeActive: chatViewModel.isSpeakModeActive,
                    onSpeakToggle: {
                        if chatViewModel.isSpeakModeActive {
                            chatViewModel.stopSpeakMode()
                        } else {
                            chatViewModel.startSpeakMode()
                        }
                    },
                    inputText: $chatViewModel.inputText,
                    isLoading: $chatViewModel.isLoading,
                    focusSnippet: $chatViewModel.focusSnippet,
                    pendingAttachments: Binding(
                        get: { chatViewModel.pendingAttachments },
                        set: { chatViewModel.pendingAttachments = $0 }
                    ),
                    isMediaPanelVisible: $isMediaPanelVisible,
                    isInputFocused: isInputFocused,
                    send: { onSend() },
                    stop: { chatViewModel.stopGeneration() },
                    onVoiceRecordingStart: { },
                    onVoiceModeStart: { chatViewModel.startVoiceModePushToTalk() },
                    onVoiceModeStop: { chatViewModel.stopVoiceModePushToTalk() }
                )
                // Add a little breathing room above the keyboard.
                .padding(.bottom, isInputFocused.wrappedValue ? 8 : 0)
                .transition(.move(edge: .bottom).combined(with: .opacity))
            }
        }
        // "Scroll to newest" chevron. Put it on the container (not inside the ScrollView overlay),
        // so it can't end up behind the input bar and miss taps.
        .overlay(alignment: .bottomTrailing) {
            let shouldShowScrollButton = !chatViewModel.isSpeakModeActive
                && !chatViewModel.messages.isEmpty
                && !isNearBottom

            // Lift the chevron a bit higher for easier tapping/visibility.
            let extraScrollButtonLift: CGFloat = 40

            // Place it just above the input bar (when present).
            let scrollButtonBottomPadding: CGFloat = {
                guard !chatViewModel.isSpeakModeActive else { return 10 + extraScrollButtonLift }
                // Fallback keeps the button tappable before the preference value arrives.
                let barHeight = (inputBarFrame == .zero) ? 72 : inputBarFrame.height
                return 10 + barHeight + extraScrollButtonLift
            }()

            // Make it easy to tap (minimum 44pt; we use 52pt).
            let scrollButtonSize: CGFloat = 42
            let scrollButtonIconSize: CGFloat = 16

            ZStack(alignment: .bottomTrailing) {
                if shouldShowScrollButton {
                    Group {
                        if #available(iOS 26.0, *) {
                            Button(action: {
                                followBottom = true
                                scrollToBottomToken &+= 1
                            }) {
                                Image(systemName: "chevron.down")
                                    .font(.system(size: scrollButtonIconSize, weight: .semibold))
                                    .foregroundColor(.primary)
                                    .frame(width: scrollButtonSize, height: scrollButtonSize)
                            }
                            .buttonStyle(PressScaleButtonStyle())
                            .glassEffect(.regular.interactive(), in: Circle())
                            .contentShape(Rectangle())
                        } else {
                            Button(action: {
                                followBottom = true
                                scrollToBottomToken &+= 1
                            }) {
                                Image(systemName: "chevron.down")
                                    .font(.system(size: scrollButtonIconSize, weight: .semibold))
                                    .foregroundColor(.primary)
                                    .frame(width: scrollButtonSize, height: scrollButtonSize)
                            }
                            .buttonStyle(PressScaleButtonStyle())
                            .background(.thinMaterial, in: Circle())
                            .overlay(Circle().strokeBorder(Color.primary.opacity(0.10), lineWidth: 1))
                            .contentShape(Rectangle())
                        }
                    }
                    // Scale in/out (instead of a "pop") when it appears/disappears.
                    .transition(
                        .scale(scale: 0.86, anchor: .bottom)
                        .combined(with: .opacity)
                    )
                }
            }
            // Keep a stable layout target so the transition doesn't "drift" sideways.
            .frame(width: scrollButtonSize, height: scrollButtonSize)
            .padding(.bottom, scrollButtonBottomPadding)
            .padding(.trailing, 16)
            .allowsHitTesting(shouldShowScrollButton)
            .animation(.spring(response: 0.30, dampingFraction: 0.86), value: shouldShowScrollButton)
        }
        .overlay {
            if chatViewModel.isLoadingHistory && chatViewModel.messages.isEmpty && !chatViewModel.isSpeakModeActive {
                ZStack {
                    Color(.systemBackground).ignoresSafeArea()
                    ProgressView().progressViewStyle(.circular)
                }
                .transition(.opacity)
            }
        }
        .overlay {
            if chatViewModel.isSpeakModeActive {
                SpeakModeOrbOverlayView(
                    voiceService: chatViewModel.speakSTTService,
                    ttsService: chatViewModel.elevenLabsStreamingTTS,
                    onClose: {
                        isInputFocused.wrappedValue = false
                        chatViewModel.stopSpeakMode()
                    }
                )
                .transition(.opacity)
            }
        }
        .background(Color(.systemBackground))
        .onPreferenceChange(ChatSendButtonFramePreferenceKey.self) { frame in
            sendButtonFrame = frame
        }
        .onPreferenceChange(ChatInputBarFramePreferenceKey.self) { frame in
            inputBarFrame = frame
        }
        .onChange(of: chatViewModel.pendingOutgoingUserMessageId, initial: false) { _, newId in
            guard let id = newId else { return }

            sendAnimationCleanupTask?.cancel()
            frozenSendButtonFrame = sendButtonFrame
            outgoingAnimatingMessageId = id
            outgoingSourceMessageId = id

            // Start the glide on the next runloop so the destination has a stable layout.
            DispatchQueue.main.async {
                withAnimation(.easeInOut(duration: 0.45)) {
                    outgoingSourceMessageId = nil
                }
            }

            sendAnimationCleanupTask = Task { @MainActor in
                try? await Task.sleep(nanoseconds: 520_000_000)
                outgoingAnimatingMessageId = nil
                frozenSendButtonFrame = nil
            }

            // Consume the signal so it only fires once per send.
            chatViewModel.pendingOutgoingUserMessageId = nil
        }
        .onChange(of: isInputFocused.wrappedValue, initial: false) { _, focused in
            if focused && isMediaPanelVisible {
                withAnimation(.spring(response: 0.45, dampingFraction: 0.92)) {
                    isMediaPanelVisible = false
                }
            }
        }
        .sheet(isPresented: $isMediaPanelVisible) {
            // Half-sheet attachments picker (recents + camera + files).
            MediaPickerPanelView(
                attachments: Binding(
                    get: { chatViewModel.pendingAttachments },
                    set: { chatViewModel.pendingAttachments = $0 }
                ),
                height: 360,
                horizontalPadding: 0,
                cornerRadius: 0
            )
            // Keep it medium (single detent, not expandable).
            .presentationDetents([.medium])
            .presentationDragIndicator(.hidden)
        }
        .coordinateSpace(name: "ChatScreen")
    }
}

private struct PressScaleButtonStyle: ButtonStyle {
    var pressedScale: CGFloat = 0.92
    var pressedOpacity: CGFloat = 0.92

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? pressedScale : 1.0)
            .opacity(configuration.isPressed ? pressedOpacity : 1.0)
            .animation(.spring(response: 0.22, dampingFraction: 0.88, blendDuration: 0.02), value: configuration.isPressed)
    }
}

#Preview("Chat Screen") {
    struct PreviewContainer: View {
        @StateObject private var vm = ChatViewModel()
        @FocusState private var isFocused: Bool

        var body: some View {
            ChatScreenView(
                chatViewModel: vm,
                onSend: { vm.sendMessage() },
                isInputFocused: $isFocused
            )
            .environmentObject(SidebarNavigationViewModel())
            .environmentObject(ChatSessionsViewModel())
            .onAppear {
                vm.isLoading = false
                vm.isLoadingHistory = false
                vm.isAssistantTyping = false
                vm.messages = [
                    ChatMessage.text("Hey! Can we talk about yesterday?", isFromUser: true),
                    ChatMessage(segments: [.text("Of course—what's on your mind?")], isFromUser: false),
                    ChatMessage(segments: [.text("You could say:"), .partnerMessage("I felt dismissed during our talk. Can we revisit it?")], isFromUser: false),
                    ChatMessage(segments: [.partnerReceived("Absolutely, I'd like that. When works for you?")], isFromUser: false)
                ]
            }
        }
    }

    return PreviewContainer()
}
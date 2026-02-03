import SwiftUI

struct ChatScreenView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    let onSend: () -> Void

    @AppStorage(PreferenceKeys.voiceModeEnabled) private var voiceModeEnabled: Bool = false

    @State private var isMediaPanelVisible: Bool = false
    @State private var isNearBottom: Bool = true
    @State private var scrollToBottomToken: Int = 0
    @State private var followBottom: Bool = false

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
        let chatBottomBreathingRoom: CGFloat = 14

        MessagesListView(
            chatViewModel: chatViewModel,
            isNearBottom: $isNearBottom,
            followBottom: $followBottom,
            messages: chatViewModel.messages,
            isInputFocused: isInputFocused.wrappedValue,
            isAssistantTyping: chatViewModel.isAssistantTyping,
            initialJumpToken: chatViewModel.initialJumpToken,
            scrollToBottomToken: scrollToBottomToken,
            bottomReservedSpace: chatBottomBreathingRoom
        )
        .contentShape(Rectangle())
        .onTapGesture {
            isInputFocused.wrappedValue = false
        }
        .safeAreaInset(edge: .bottom, spacing: 0) {
            InputAreaView(
                isVoiceRecording: chatViewModel.dictationSTTService.isRecording,
                voiceModeEnabled: voiceModeEnabled,
                isSpeakModeActive: chatViewModel.isSpeakModeActive,
                onSpeakToggle: {
                    if chatViewModel.isSpeakModeActive {
                        chatViewModel.voiceController.stopSpeakMode()
                    } else {
                        chatViewModel.voiceController.startSpeakMode()
                    }
                },
                inputText: $chatViewModel.inputText,
                isLoading: $chatViewModel.isLoading,
                pendingAttachments: Binding(
                    get: { chatViewModel.pendingAttachments },
                    set: { chatViewModel.pendingAttachments = $0 }
                ),
                isMediaPanelVisible: $isMediaPanelVisible,
                isInputFocused: isInputFocused,
                send: { onSend() },
                stop: { chatViewModel.streamingController.stopGeneration() },
                onVoiceModeStart: { chatViewModel.voiceController.startVoiceModePushToTalk() },
                onVoiceModeStop: { chatViewModel.voiceController.stopVoiceModePushToTalk() }
            )
            .padding(.bottom, isInputFocused.wrappedValue ? 8 : 0)
            .transition(.move(edge: .bottom).combined(with: .opacity))
        }
        .background(Color(.systemBackground))
        .overlay(alignment: .bottomTrailing) {
            let shouldShowScrollButton = !chatViewModel.messages.isEmpty && !isNearBottom
            let extraScrollButtonLift: CGFloat = 40
            let baseComposerHeight: CGFloat = chatViewModel.pendingAttachments.isEmpty ? 68 : 128
            let scrollButtonBottomPadding: CGFloat = 10 + baseComposerHeight + extraScrollButtonLift

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
                    .transition(
                        .scale(scale: 0.86, anchor: .bottom)
                        .combined(with: .opacity)
                    )
                }
            }
            .frame(width: scrollButtonSize, height: scrollButtonSize)
            .padding(.bottom, scrollButtonBottomPadding)
            .padding(.trailing, 16)
            .allowsHitTesting(shouldShowScrollButton)
            .animation(.spring(response: 0.30, dampingFraction: 0.86), value: shouldShowScrollButton)
        }
        .overlay {
            if chatViewModel.isLoadingHistory && chatViewModel.messages.isEmpty {
                ZStack {
                    Color(.systemBackground).ignoresSafeArea()
                    ProgressView().progressViewStyle(.circular)
                }
                .transition(.opacity)
            }
        }
        .onChange(of: chatViewModel.pendingOutgoingUserMessageId, initial: false) { _, newId in
            if newId != nil {
                chatViewModel.pendingOutgoingUserMessageId = nil
            }
        }
        .sheet(isPresented: $isMediaPanelVisible) {
            MediaPickerPanelView(
                attachments: Binding(
                    get: { chatViewModel.pendingAttachments },
                    set: { chatViewModel.pendingAttachments = $0 }
                ),
                height: 360,
                horizontalPadding: 0,
                cornerRadius: 0
            )
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
                onSend: { vm.streamingController.sendMessage() },
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
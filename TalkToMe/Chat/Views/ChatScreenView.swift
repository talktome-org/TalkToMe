import SwiftUI

struct ChatScreenView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    @AppStorage(PreferenceKeys.voiceModeEnabled) private var voiceModeEnabled: Bool = false

    let onSend: () -> Void
    let isInputFocused: FocusState<Bool>.Binding

    private var inputAreaHeight: CGFloat {
        chatViewModel.pendingAttachments.isEmpty ? 68 : 128
    }

    var body: some View {
        MessagesListView(
            chatViewModel: chatViewModel,
            isInputFocused: isInputFocused.wrappedValue,
            inputAreaHeight: inputAreaHeight
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
                speakModePhase: chatViewModel.voiceController.speakModePhase,
                speakerLevel: chatViewModel.voiceController.speakerLevel,
                micLevel: chatViewModel.voiceController.micLevel,
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
                isInputFocused: isInputFocused,
                send: { onSend() },
                stop: {
                    chatViewModel.streamingController.stopGeneration()
                    if chatViewModel.isSpeakModeActive {
                        Task { @MainActor in
                            chatViewModel.elevenLabsStreamingTTS.cancel()
                        }
                    }
                },
                onVoiceModeStart: { chatViewModel.voiceController.startVoiceModePushToTalk() },
                onVoiceModeStop: { chatViewModel.voiceController.stopVoiceModePushToTalk() }
            )
            .padding(.bottom, isInputFocused.wrappedValue ? 8 : 0)
            .transition(.move(edge: .bottom).combined(with: .opacity))
        }
        .background(Color(.systemBackground))
        .onChange(of: chatViewModel.pendingOutgoingUserMessageId, initial: false) { _, newId in
            if newId != nil {
                chatViewModel.pendingOutgoingUserMessageId = nil
            }
        }
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
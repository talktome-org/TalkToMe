import SwiftUI

struct ChatScreenView: View {

  @ObservedObject var chatViewModel: ChatViewModel

  @AppStorage(PreferenceKeys.dictationEnabled) private var dictationEnabled: Bool = false

  let onSend: () -> Void
  let isInputFocused: FocusState<Bool>.Binding

  @State private var toastMessage: String? = nil
  @State private var toastWorkItem: DispatchWorkItem? = nil
  private var inputAreaHeight: CGFloat {
    chatViewModel.pendingAttachments.isEmpty ? 68 : 220
  }

  private var inputAreaYOffset: CGFloat {
    isInputFocused.wrappedValue ? 8 : 6
  }

  private var focusedBottomInset: CGFloat {
    isInputFocused.wrappedValue ? 10 : 0
  }

  var body: some View {
    MessagesListView(
      chatViewModel: chatViewModel,
      isInputFocused: isInputFocused.wrappedValue,
      inputAreaHeight: inputAreaHeight,
      onBackgroundTap: {
        isInputFocused.wrappedValue = false
      }
    )
    .safeAreaInset(edge: .bottom, spacing: 0) {
      VStack(spacing: 0) {
        // Ghost buddy circle — speak mode only, pushes messages up
        if chatViewModel.isSpeakModeActive {
          ghostBuddyCircle
            .transition(.scale(scale: 0.3).combined(with: .opacity))
        }

        InputAreaView(
        isVoiceRecording: chatViewModel.dictationSTTService.isRecording,
        isSpeakModeActive: chatViewModel.isSpeakModeActive,
        isSpeakMicMuted: chatViewModel.voiceController.isSpeakMicMuted,
        speakModePhase: chatViewModel.voiceController.speakModePhase,
        speakerLevel: chatViewModel.voiceController.speakerLevel,
        micLevel: chatViewModel.voiceController.micLevel,
        onSpeakToggle: {
          guard dictationEnabled else {
            showDictationToast()
            return
          }
          if chatViewModel.isSpeakModeActive {
            chatViewModel.voiceController.stopSpeakMode()
          } else {
            chatViewModel.voiceController.startSpeakMode()
          }
        },
        onMicMuteToggle: {
          chatViewModel.voiceController.toggleSpeakMicMute()
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
        onVoiceModeStart: {
          guard dictationEnabled else {
            showDictationToast()
            return
          }
          chatViewModel.voiceController.startVoiceModePushToTalk()
        },
        onVoiceModeStop: { chatViewModel.voiceController.stopVoiceModePushToTalk() }
      )
      .frame(
        minHeight: chatViewModel.pendingAttachments.isEmpty ? inputAreaHeight : nil,
        alignment: .bottom
      )
      .offset(y: inputAreaYOffset)
      .padding(.bottom, focusedBottomInset)
      .transition(.move(edge: .bottom).combined(with: .opacity))
      } // VStack (ghost circle + input)
      .animation(.smooth(duration: 0.45), value: chatViewModel.isSpeakModeActive)
    }
    .overlay(alignment: .top) {
      if let toastMessage {
        ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
          .padding(.top, 8)
      }
    }
    .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
    .background {
      AppTheme.background
        .ignoresSafeArea()
    }
  }

  @ViewBuilder
  private var ghostBuddyCircle: some View {
    Button(action: {
      Haptics.impact(.medium)
      chatViewModel.voiceController.stopSpeakMode()
    }) {
      GhostVideoContentView(
        isSpeakModeActive: true,
        speakModePhase: chatViewModel.voiceController.speakModePhase,
        size: 150
      )
    }
    .buttonStyle(.plain)
    .frame(maxWidth: .infinity)
    .padding(.top, 16)
    .padding(.bottom, 8)
  }

  private func showDictationToast() {
    Haptics.notification(.warning)
    toastWorkItem?.cancel()
    toastMessage = "Turn on Dictation in Settings to use voice input."
    let item = DispatchWorkItem { dismissToast() }
    toastWorkItem = item
    DispatchQueue.main.asyncAfter(deadline: .now() + 3, execute: item)
  }

  private func dismissToast() {
    toastWorkItem?.cancel()
    toastWorkItem = nil
    toastMessage = nil
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
          ChatMessage(
            segments: [
              .text("You could say:"),
              .partnerMessage(
                text: "I felt dismissed during our talk. Can we revisit it?", ghostName: nil),
            ], isFromUser: false),
          ChatMessage(
            segments: [.partnerReceived("Absolutely, I'd like that. When works for you?")],
            isFromUser: false),
        ]
      }
    }
  }

  return PreviewContainer()
}

import SwiftUI

struct ChatScreenView: View {

  @ObservedObject var chatViewModel: ChatViewModel

  @AppStorage(PreferenceKeys.chatWallpaperType) private var wallpaperType: String = "default"
  @AppStorage(PreferenceKeys.chatWallpaperValue) private var wallpaperValue: String = ""
  @AppStorage(PreferenceKeys.dictationEnabled) private var dictationEnabled: Bool = false
  @AppStorage(PreferenceKeys.elevenLabsVoiceName) private var selectedVoiceName: String = "mira"

  let onSend: () -> Void
  let isInputFocused: FocusState<Bool>.Binding

  @State private var toastMessage: String? = nil
  @State private var toastWorkItem: DispatchWorkItem? = nil
  @State private var wallpaperPhoto: UIImage? = nil

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
      } // VStack (reply bar + input)
    }
    .overlay(alignment: .top) {
      if let toastMessage {
        ToastOverlayView(message: toastMessage, onDismiss: dismissToast)
          .padding(.top, 8)
      }
    }
    .animation(.spring(response: 0.3, dampingFraction: 0.8), value: toastMessage)
    .task(id: wallpaperLoadKey) {
      await refreshWallpaperPhoto()
    }
    .background {
      ZStack {
        chatBackground
          .ignoresSafeArea()

        if wallpaperType == "default" {
          ChatFloatingGhostBackdrop(ghostName: selectedVoiceName)
            .opacity(0.42)
            .ignoresSafeArea()
            .allowsHitTesting(false)
        }
      }
    }
  }

  private var wallpaperLoadKey: String {
    "\(wallpaperType)::\(wallpaperValue)"
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

  @ViewBuilder
  private var chatBackground: some View {
    switch wallpaperType {
    case "gradient":
      if let preset = wallpaperPresets.first(where: { $0.id == wallpaperValue }) {
        preset.gradient
      } else {
        AppTheme.background
      }
    case "color":
      WallpapersSettingsView.colorFromHex(wallpaperValue)
    case "photo":
      if let image = wallpaperPhoto {
        Image(uiImage: image)
          .resizable()
          .scaledToFill()
      } else {
        AppTheme.background
      }
    default:
      AppTheme.background
    }
  }

  @MainActor
  private func refreshWallpaperPhoto() async {
    guard wallpaperType == "photo", !wallpaperValue.isEmpty else {
      wallpaperPhoto = nil
      return
    }

    let requestedKey = wallpaperLoadKey
    let filename = wallpaperValue
    let data = await Task.detached(priority: .utility) {
      let url = WallpapersSettingsView.wallpaperFileURL(filename: filename)
      return try? Data(contentsOf: url)
    }.value

    guard requestedKey == wallpaperLoadKey else { return }
    wallpaperPhoto = data.flatMap(UIImage.init(data:))
  }

}

private struct ChatFloatingGhostBackdrop: View {
  let ghostName: String

  var body: some View {
    if let ghostImage = resolvedGhostImage {
      GeometryReader { proxy in
        TimelineView(.animation(minimumInterval: 1.0 / 30.0, paused: false)) { context in
          let time = context.date.timeIntervalSinceReferenceDate
          let size = proxy.size

          ZStack {
            ghostLayer(
              image: ghostImage,
              size: size,
              time: time,
              baseX: 0.20,
              baseY: 0.24,
              xAmplitude: size.width * 0.10,
              yAmplitude: size.height * 0.08,
              driftX: 0.16,
              driftY: 0.13,
              phase: 0.3,
              widthFactor: 0.95,
              blur: 42,
              opacity: 0.22
            )

            ghostLayer(
              image: ghostImage,
              size: size,
              time: time,
              baseX: 0.76,
              baseY: 0.72,
              xAmplitude: size.width * 0.12,
              yAmplitude: size.height * 0.09,
              driftX: 0.12,
              driftY: 0.15,
              phase: 2.2,
              widthFactor: 0.82,
              blur: 36,
              opacity: 0.20
            )
          }
        }
      }
      .clipped()
    }
  }

  private var resolvedGhostImage: UIImage? {
    if let image = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: ghostName) {
      return image
    }
    for buddy in ElevenLabsVoiceSuggestionsView.requiredBuddies {
      if let image = ElevenLabsVoiceSuggestionsView.ghostUIImage(for: buddy.key) {
        return image
      }
    }
    return nil
  }

  private func ghostLayer(
    image: UIImage,
    size: CGSize,
    time: TimeInterval,
    baseX: CGFloat,
    baseY: CGFloat,
    xAmplitude: CGFloat,
    yAmplitude: CGFloat,
    driftX: Double,
    driftY: Double,
    phase: Double,
    widthFactor: CGFloat,
    blur: CGFloat,
    opacity: Double
  ) -> some View {
    let x = size.width * baseX + CGFloat(sin(time * driftX + phase)) * xAmplitude
    let y = size.height * baseY + CGFloat(cos(time * driftY + phase)) * yAmplitude
    let pulse = 1 + CGFloat(sin(time * 0.1 + phase)) * 0.04

    return Image(uiImage: image)
      .resizable()
      .scaledToFit()
      .frame(width: max(size.width * widthFactor, 220))
      .scaleEffect(pulse)
      .saturation(0)
      .contrast(1.18)
      .brightness(-0.14)
      .opacity(opacity)
      .blur(radius: blur)
      .position(x: x, y: y)
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

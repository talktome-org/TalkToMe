import SwiftUI
import UIKit

struct ChatScreenView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    // Cache the last known keyboard height across ChatScreenView instances.
    // This fixes the "panel too short" case when opening a chat and tapping + before the keyboard is shown.
    private static var cachedKeyboardHeight: CGFloat = 0

    @State private var inputBarHeight: CGFloat = 0
    @State private var suggestionsHeight: CGFloat = 0
    @State private var bottomSafeInset: CGFloat = 0
    @State private var keyboardOverlap: CGFloat = 0
    @State private var showSuggestionsDelayed: Bool = false
    @State private var suggestionsDelayWorkItem: DispatchWorkItem?
    @State private var isMediaPanelVisible: Bool = false
    @State private var lastNonZeroKeyboardHeight: CGFloat = 0

    let isInputFocused: FocusState<Bool>.Binding

    private var quickSuggestions: [QuickSuggestion] {
        return [
            QuickSuggestion(title: "We argued about chores", subtitle: "I feel like I'm doing most of the housework"),
            QuickSuggestion(title: "They were texting an ex", subtitle: "I feel uneasy and a bit betrayed"),
            QuickSuggestion(title: "They canceled date night", subtitle: "last minute and I felt unimportant"),
            QuickSuggestion(title: "I found hidden purchases", subtitle: "and I'm worried about money trust"),
            QuickSuggestion(title: "They raised their voice", subtitle: "I shut down—how do we repair this?"),
            QuickSuggestion(title: "We disagree on parenting", subtitle: "bedtime rules turned into a fight"),
            QuickSuggestion(title: "They stayed out late", subtitle: "didn't text back and I felt anxious"),
            QuickSuggestion(title: "I felt criticized in public", subtitle: "they made a joke at my expense")
        ]
    }

    private var isNewChatReadyForSuggestions: Bool {
        chatViewModel.messages.isEmpty && !chatViewModel.isLoadingHistory && !chatViewModel.isLoading
    }

    var body: some View {
        let canShowSuggestions = isNewChatReadyForSuggestions && showSuggestionsDelayed
        // MARK: - Media panel sizing (keyboard-exact)
        // There is no fixed keyboard height constant (it varies). We reuse the last measured keyboard overlap height.
        // If the keyboard hasn't been shown yet, we fall back to a reasonable default.
        let mediaPanelFallbackHeight: CGFloat = 290
        let keyboardExactHeight: CGFloat = {
            if lastNonZeroKeyboardHeight > 0 { return lastNonZeroKeyboardHeight }
            if Self.cachedKeyboardHeight > 0 { return Self.cachedKeyboardHeight }
            return mediaPanelFallbackHeight
        }()
        // While the keyboard is still on-screen, SwiftUI already reserves `keyboardOverlap` via safe-area.
        // Only reserve the *additional* height that becomes visible as the keyboard slides away.
        let mediaPanelHeight: CGFloat = isMediaPanelVisible ? max(0, keyboardExactHeight - keyboardOverlap) : 0
        let extraBottomInset: CGFloat = isMediaPanelVisible ? 0 : bottomSafeInset
        // While the keyboard is animating away, SwiftUI moves the whole overlay down to the bottom,
        // then the media panel pushes it back up (causing the "down then up" jump).
        // Offsetting by the current keyboard overlap keeps the overlay visually locked in place.
        let overlayKeyboardOffset: CGFloat = isMediaPanelVisible ? keyboardOverlap : 0

        VStack(spacing: 0) {
            MessagesListView(
                chatViewModel: chatViewModel,
                messages: chatViewModel.messages,
                isInputFocused: isInputFocused.wrappedValue,
                isAssistantTyping: chatViewModel.isAssistantTyping,
                initialJumpToken: chatViewModel.initialJumpToken
            )
            .safeAreaInset(edge: .bottom) {
                Color.clear
                    .frame(height: 50)
            }
            .padding(
                .bottom,
                inputBarHeight
                    + (canShowSuggestions ? suggestionsHeight : 0)
                    + mediaPanelHeight
                    + extraBottomInset
            )
            .contentShape(Rectangle())
            .onTapGesture {
                isInputFocused.wrappedValue = false
            }
        }
        .overlay(alignment: .bottom) {
            VStack(spacing: 0) {
                if canShowSuggestions {
                    QuickSuggestionsView(
                        suggestions: quickSuggestions,
                        onTap: { text in
                            Haptics.impact(.light)
                            chatViewModel.inputText = text
                            chatViewModel.sendMessage()
                        }
                    )
                    .background(
                        GeometryReader { proxy in
                            Color.clear.preference(key: SuggestionsHeightPreferenceKey.self, value: proxy.size.height)
                        }
                    )
                    .transition(
                        .asymmetric(
                            insertion: .move(edge: .bottom).combined(with: .opacity), // flow up on appear
                            removal: .move(edge: .bottom).combined(with: .opacity)
                        )
                    )
                } else {
                    Color.clear.frame(height: 0).onAppear { suggestionsHeight = 0 }
                }

                InputAreaView(
                    inputText: $chatViewModel.inputText,
                    isLoading: $chatViewModel.isLoading,
                    focusSnippet: $chatViewModel.focusSnippet,
                    pendingAttachments: Binding(
                        get: { chatViewModel.pendingAttachments },
                        set: { chatViewModel.pendingAttachments = $0 }
                    ),
                    isMediaPanelVisible: $isMediaPanelVisible,
                    isInputFocused: isInputFocused,
                    send: { chatViewModel.sendMessage() },
                    stop: { chatViewModel.stopGeneration() },
                    onVoiceRecordingStart: { }
                )
                .background(
                    GeometryReader { proxy in
                        Color.clear.preference(key: InputBarHeightPreferenceKey.self, value: proxy.size.height)
                    }
                )
                .transition(.move(edge: .bottom).combined(with: .opacity))

                if isMediaPanelVisible {
                    MediaPickerPanelView(
                        attachments: Binding(
                            get: { chatViewModel.pendingAttachments },
                            set: { chatViewModel.pendingAttachments = $0 }
                        ),
                        height: keyboardExactHeight,
                        horizontalPadding: -1,
                        cornerRadius: 28
                    )
                    // Push the panel down into the home-indicator area WITHOUT changing the visible gap
                    // between the input bar and the panel.
                    .padding(.top, -(bottomSafeInset + 1))
                    .offset(y: bottomSafeInset + 1)
                    .ignoresSafeArea(edges: .horizontal)
                    .transition(AnyTransition.move(edge: .bottom).combined(with: .opacity))
                } else {
                    Color.clear.frame(height: 0)
                }
            }
            .ignoresSafeArea(edges: .bottom)
            .offset(y: overlayKeyboardOffset)
        }
        .overlay {
            if chatViewModel.isLoadingHistory && chatViewModel.messages.isEmpty {
                ZStack {
                    Color(.systemBackground).ignoresSafeArea()
                    ProgressView().progressViewStyle(.circular)
                }
                .transition(.opacity)
            } else if chatViewModel.messages.isEmpty && !chatViewModel.isLoadingHistory && !chatViewModel.isLoading {
                VStack {
                    Text("Hey, I'm here to listen - tell me\n what happened and what's on your mind, and I'll help you write\n the right message")
                        .font(.system(size: 20, weight: .semibold))
                        .foregroundColor(Color.gray)
                        .multilineTextAlignment(.center)
                        .lineLimit(nil)
                        .fixedSize(horizontal: false, vertical: true)
                        .padding(.horizontal, 28)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
                .padding(.bottom, inputBarHeight + (canShowSuggestions ? suggestionsHeight : 0) + bottomSafeInset + 36)
                .transition(.opacity)
            }
        }
        .background(Color(.systemBackground))
        .onPreferenceChange(InputBarHeightPreferenceKey.self) { newHeight in
            inputBarHeight = newHeight
        }
        .onPreferenceChange(SuggestionsHeightPreferenceKey.self) { newHeight in
            suggestionsHeight = newHeight
        }
        .onAppear {
            bottomSafeInset = currentBottomSafeInset()
            if lastNonZeroKeyboardHeight == 0, Self.cachedKeyboardHeight > 0 {
                lastNonZeroKeyboardHeight = Self.cachedKeyboardHeight
            }
            if isNewChatReadyForSuggestions {
                scheduleSuggestionsDelay()
            } else {
                showSuggestionsDelayed = false
            }
        }
        .onChange(of: isInputFocused.wrappedValue, initial: false) { _, focused in
            if focused && isMediaPanelVisible {
                withAnimation(.spring(response: 0.45, dampingFraction: 0.92)) {
                    isMediaPanelVisible = false
                }
            }
        }
        .onChange(of: isNewChatReadyForSuggestions, initial: false) { _, ready in
            if ready {
                scheduleSuggestionsDelay()
            } else {
                suggestionsDelayWorkItem?.cancel()
                withAnimation(.spring(response: 0.4, dampingFraction: 0.95)) {
                    showSuggestionsDelayed = false
                }
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: UIResponder.keyboardWillChangeFrameNotification)) { note in
            guard let frame = note.userInfo?[UIResponder.keyboardFrameEndUserInfoKey] as? CGRect else { return }
            guard let windowScene = UIApplication.shared.connectedScenes.first as? UIWindowScene,
                  let window = windowScene.windows.first(where: { $0.isKeyWindow }) else { return }
            let endFrameInWindow = window.convert(frame, from: nil)
            let overlap = max(0, window.bounds.maxY - endFrameInWindow.minY)

            let duration = (note.userInfo?[UIResponder.keyboardAnimationDurationUserInfoKey] as? Double) ?? 0.25
            let curveRaw = (note.userInfo?[UIResponder.keyboardAnimationCurveUserInfoKey] as? Int) ?? UIView.AnimationCurve.easeInOut.rawValue
            let curve = UIView.AnimationCurve(rawValue: curveRaw) ?? .easeInOut
            let animation: Animation = {
                switch curve {
                case .easeInOut: return .easeInOut(duration: duration)
                case .easeIn: return .easeIn(duration: duration)
                case .easeOut: return .easeOut(duration: duration)
                case .linear: return .linear(duration: duration)
                @unknown default: return .easeInOut(duration: duration)
                }
            }()

            withAnimation(animation) {
                keyboardOverlap = overlap
            }
            if overlap > 0 {
                lastNonZeroKeyboardHeight = overlap
                Self.cachedKeyboardHeight = overlap
            }
        }
    }

    private func scheduleSuggestionsDelay() {
        suggestionsDelayWorkItem?.cancel()
        let work = DispatchWorkItem {
            withAnimation(.spring(response: 0.5, dampingFraction: 0.9)) {
                showSuggestionsDelayed = true
            }
        }
        suggestionsDelayWorkItem = work
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.2, execute: work)
    }
}

private func currentBottomSafeInset() -> CGFloat {
    guard let windowScene = UIApplication.shared.connectedScenes.first as? UIWindowScene,
          let window = windowScene.windows.first(where: { $0.isKeyWindow }) else { return 0 }
    return window.safeAreaInsets.bottom
}

private struct InputBarHeightPreferenceKey: PreferenceKey {
    static var defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}

private struct SuggestionsHeightPreferenceKey: PreferenceKey {
    static var defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}

#Preview("Chat Screen") {
    struct PreviewContainer: View {
        @StateObject private var vm = ChatViewModel()
        @FocusState private var isFocused: Bool

        var body: some View {
            ChatScreenView(
                chatViewModel: vm,
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
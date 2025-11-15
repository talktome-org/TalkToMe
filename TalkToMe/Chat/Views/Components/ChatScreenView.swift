import SwiftUI
import UIKit

struct ChatScreenView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    @State private var inputBarHeight: CGFloat = 0
    @State private var suggestionsHeight: CGFloat = 0
    @State private var bottomSafeInset: CGFloat = 0
    @State private var keyboardScrollToken: Int = 0
    @State private var showSuggestionsDelayed: Bool = false
    @State private var suggestionsDelayWorkItem: DispatchWorkItem?

    let onSendToPartner: () -> Void
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

        VStack(spacing: 0) {

            MessagesListView(
                messages: chatViewModel.messages,
                chatViewModel: chatViewModel,
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
                inputBarHeight + (canShowSuggestions ? suggestionsHeight : 0) + bottomSafeInset
            )
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
                    isInputFocused: isInputFocused,
                    send: { chatViewModel.sendMessage() },
                    stop: { chatViewModel.stopGeneration() },
                    onSendToPartner: onSendToPartner,
                    onVoiceRecordingStart: { },
                    onVoiceRecordingStop: { _ in }
                )
                .background(
                    GeometryReader { proxy in
                        Color.clear.preference(key: InputBarHeightPreferenceKey.self, value: proxy.size.height)
                    }
                )
                .transition(.move(edge: .bottom).combined(with: .opacity))
            }
            .ignoresSafeArea(edges: .bottom)
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
                .padding(.bottom, inputBarHeight + ((isNewChatReadyForSuggestions && showSuggestionsDelayed) ? suggestionsHeight : 0) + bottomSafeInset + 36)
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
            if isNewChatReadyForSuggestions {
                suggestionsDelayWorkItem?.cancel()
                let work = DispatchWorkItem {
                    withAnimation(.spring(response: 0.5, dampingFraction: 0.9)) {
                        showSuggestionsDelayed = true
                    }
                }
                suggestionsDelayWorkItem = work
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.2, execute: work)
            } else {
                showSuggestionsDelayed = false
            }
        }
        .onChange(of: isNewChatReadyForSuggestions, initial: false) { _, ready in
            if ready {
                suggestionsDelayWorkItem?.cancel()
                let work = DispatchWorkItem {
                    withAnimation(.spring(response: 0.5, dampingFraction: 0.9)) {
                        showSuggestionsDelayed = true
                    }
                }
                suggestionsDelayWorkItem = work
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.2, execute: work)
            } else {
                suggestionsDelayWorkItem?.cancel()
                withAnimation(.spring(response: 0.4, dampingFraction: 0.95)) {
                    showSuggestionsDelayed = false
                }
            }
        }
        .onChange(of: isInputFocused.wrappedValue, initial: false) { _, isFocused in
            if isFocused && !chatViewModel.messages.isEmpty {
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                    keyboardScrollToken &+= 1
                }
            }
        }
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
                onSendToPartner: { },
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
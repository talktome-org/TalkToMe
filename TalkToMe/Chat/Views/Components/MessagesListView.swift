import SwiftUI

struct MessagesListView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    @State private var savedScrollPosition: UUID?

    let messages: [ChatMessage]
    let isInputFocused: Bool
    let isAssistantTyping: Bool
    let initialJumpToken: Int

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView {
                VStack(spacing: 18) {
                    ForEach(Array(messages.enumerated()), id: \.element.id) { index, message in
                        MessageBubbleView(chatViewModel: chatViewModel, message: message, onSendToPartner: { text in
                            NotificationCenter.default.post(name: .sendPartnerMessageFromBubble, object: nil, userInfo: ["content": text])
                        })
                            .id(message.id)
                            .padding(.top, index > 0 && (messages[index - 1].isFromUser != message.isFromUser) ? 4 : 0)
                    }
                    if isAssistantTyping {
                        HStack(alignment: .top, spacing: 0) {
                            TypingIndicatorView(showAfter: 0)
                                .padding(.top, -10)
                            Spacer(minLength: 0)
                        }
                        .id("typing-indicator")
                    }
                }
                .padding(.top, 24)
                .padding(.horizontal)
            }
            .scrollBounceBehavior(.always)
            .scrollIndicators(.visible)
            .onChange(of: chatViewModel.streamingScrollToken, initial: false) { _, _ in
                // Keep view pinned to the assistant's streaming message as tokens arrive
                let targetId = chatViewModel.assistantScrollTargetId ?? messages.last?.id
                if let targetId = targetId {
                    withAnimation(nil) { proxy.scrollTo(targetId, anchor: .bottom) }
                }
            }
            .onChange(of: chatViewModel.assistantScrollTargetId, initial: false) { _, newId in
                // When a new assistant placeholder appears, jump to it immediately
                if let id = newId {
                    withAnimation(nil) { proxy.scrollTo(id, anchor: .bottom) }
                }
            }
            .onChange(of: initialJumpToken, initial: false) { _, token in
                guard token > 0 else { return }
                guard let lastId = messages.last?.id else { return }
                withAnimation(nil) { proxy.scrollTo(lastId, anchor: .bottom) }
            }
            .onChange(of: isAssistantTyping, initial: false) { _, typing in
                // If typing indicator appears before any tokens, make sure it's visible
                if typing {
                    withAnimation(nil) { proxy.scrollTo("typing-indicator", anchor: .bottom) }
                }
            }
            .onChange(of: isInputFocused, initial: false) { _, newValue in
                if newValue {
                    guard let lastId = messages.last?.id else { return }

                    savedScrollPosition = lastId
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                        withAnimation(.spring(response: 0.4, dampingFraction: 0.94)) {
                            proxy.scrollTo(lastId, anchor: .bottom)
                        }
                    }
                } else if !newValue, let savedId = savedScrollPosition {
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                        let currentLastId = messages.last?.id
                        withAnimation(.spring(response: 0.4, dampingFraction: 0.9)) {
                            if let currentLastId = currentLastId, currentLastId != savedId {
                                proxy.scrollTo(currentLastId, anchor: .bottom)
                            } else {
                                proxy.scrollTo(savedId, anchor: .bottom)
                            }
                        }
                        savedScrollPosition = nil
                    }
                }
            }
        }
    }
}
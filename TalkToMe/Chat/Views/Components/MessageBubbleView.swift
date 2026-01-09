import SwiftUI

struct MessageBubbleView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    let message: ChatMessage

    var onSendToPartner: ((String) -> Void)? = nil

    var body: some View {
        VStack(alignment: message.isFromUser ? .trailing : .leading, spacing: 4) {
            if message.isFromUser {
                Text(plainText(from: message.segments))
                    .font(.system(size: 17, weight: .regular))
                    .lineSpacing(2)
                    .padding(.horizontal, 16)
                    .padding(.vertical, 14)
                    .background(
                        RoundedRectangle(cornerRadius: 20)
                            .fill(
                                LinearGradient(
                                    colors: [
                                        Color(red: 0.4, green: 0.2, blue: 0.6),
                                        Color(red: 0.35, green: 0.15, blue: 0.55)
                                    ],
                                    startPoint: .topLeading,
                                    endPoint: .bottomTrailing
                                )
                            )
                    )
                    .foregroundColor(.white)
                    .textSelection(.enabled)
                    .frame(maxWidth: 320, alignment: .trailing)
            } else {
                VStack(alignment: .leading, spacing: 8) {
                    if message.isFromPartnerUser {
                        PartnerMessageBlockView(text: plainText(from: message.segments))
                    } else if !message.segments.isEmpty {
                        ForEach(Array(message.segments.enumerated()), id: \.offset) { _, segment in
                            switch segment {
                            case .text(let text):
                                let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
                                if !trimmed.isEmpty {
                                    MarkdownRendererView(markdown: text)
                                        .frame(maxWidth: .infinity, alignment: .leading)
                                        .padding(.horizontal, 4)
                                        .padding(.vertical, 4)
                                }
                            case .partnerMessage(let text):
                                if !text.isEmpty {
                                    let isSent = chatViewModel.partnerDrafts.isPartnerDraftSent(sessionId: chatViewModel.sessionId, messageContent: text)

                                    let isLinked = (UserDefaults.standard.bool(forKey: PreferenceKeys.partnerConnected) == true)
                                    PartnerDraftBlockView(initialText: text, isSent: isSent, isLinked: isLinked) { action in
                                        switch action {
                                        case .send(let edited):
                                            onSendToPartner?(edited)
                                        }
                                    }
                                    .id(text)
                                    .padding(.top, 6)
                                }
                            case .partnerReceived(let text):
                                if !text.isEmpty {
                                    PartnerMessageBlockView(text: text)
                                        .id("partner_received_\(text.hashValue)")
                                        .padding(.top, 6)
                                }
                            }
                        }
                        if message.isToolLoading {
                            HStack {
                                TypingIndicatorView(showAfter: 0.5)
                                Spacer(minLength: 0)
                            }
                            .padding(.top, 2)
                        }
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: message.isFromUser ? .trailing : .leading)
    }

    private func plainText(from segments: [MessageSegment]) -> String {
        return segments.compactMap { segment in
            if case .text(let text) = segment { return text }
            return nil
        }.joined()
    }
}

#Preview {
    VStack(spacing: 20) {
        MessageBubbleView(
            chatViewModel: ChatViewModel(),
            message: ChatMessage.text("Hello! How are you? I'm Stephan, and I'd like to chat with you.", isFromUser: true)
        )
        MessageBubbleView(
            chatViewModel: ChatViewModel(),
            message: ChatMessage.text("I'm doing great, thanks for asking!", isFromUser: false)
        )
        MessageBubbleView(
            chatViewModel: ChatViewModel(),
            message: ChatMessage(
                segments: [.text("Sure—here's a message you could send:")],
                isFromUser: false,
                isToolLoading: false
            ),
            onSendToPartner: { _ in }
        )
    }
    .padding()
}

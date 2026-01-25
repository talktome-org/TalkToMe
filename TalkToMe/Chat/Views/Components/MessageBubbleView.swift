import SwiftUI
import UIKit

struct MessageBubbleView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    let message: ChatMessage

    var onSendToPartner: ((String) -> Void)? = nil

    @MainActor
    var body: some View {
        VStack(alignment: message.isFromUser ? .trailing : .leading, spacing: 4) {
            if message.isFromUser {
                let text = plainText(from: message.segments)
                let hasText = !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                let attachmentSegments = message.segments.filter { isAttachmentSegment($0) }

                VStack(alignment: .trailing, spacing: 8) {
                    if !attachmentSegments.isEmpty {
                        attachmentsView(segments: attachmentSegments, alignment: .trailing)
                    }
                    if hasText {
                        Text(text)
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
                    }
                }
            } else {
                VStack(alignment: .leading, spacing: 8) {
                    if message.isFromPartnerUser {
                        PartnerMessageBlockView(
                            text: plainText(from: message.segments),
                            senderUserId: message.senderUserId
                        )
                    } else
                    if !message.segments.isEmpty {
                        let attachmentSegments = message.segments.filter { isAttachmentSegment($0) }
                        if !attachmentSegments.isEmpty {
                            attachmentsView(segments: attachmentSegments, alignment: .leading)
                        }
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
                            case .imageData(_), .imageURL(_), .fileData(_, _), .fileURL(_, _):
                                EmptyView()
                            case .partnerMessage(let text):
                                if !text.isEmpty {
                                    let isSent = chatViewModel.partnerDrafts.isPartnerDraftSent(sessionId: chatViewModel.sessionId, messageContent: text)
                                    let isLinked = chatViewModel.isConnectedToFriendInThisChat
                                    PartnerDraftBlockView(
                                        initialText: text,
                                        isSent: isSent,
                                        isLinked: isLinked,
                                        recipientUserId: chatViewModel.selectedFriendUserId
                                    ) { action in
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
                                    PartnerMessageBlockView(text: text, senderUserId: message.senderUserId)
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

    private func isAttachmentSegment(_ seg: MessageSegment) -> Bool {
        switch seg {
        case .imageData(_), .imageURL(_), .fileData(_, _), .fileURL(_, _):
            return true
        default:
            return false
        }
    }

    @ViewBuilder
    private func attachmentsView(segments: [MessageSegment], alignment: HorizontalAlignment) -> some View {
        let isTrailing = (alignment == .trailing)

        Group {
            if segments.count == 1, let seg = segments.first {
                // Special-case single attachment so it can be truly right/left aligned (ScrollView tends to "center-ish" single items).
                attachmentView(seg)
                    .frame(maxWidth: 320, alignment: isTrailing ? .trailing : .leading)
            } else {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 10) {
                ForEach(Array(segments.enumerated()), id: \.offset) { _, seg in
                            attachmentView(seg)
                        }
                    }
                    .padding(.vertical, 2)
                }
                .frame(maxWidth: 320, alignment: isTrailing ? .trailing : .leading)
            }
        }
    }

    @ViewBuilder
    private func attachmentView(_ seg: MessageSegment) -> some View {
                    switch seg {
                    case .imageData(let data):
                        if let uiImage = UIImage(data: data) {
                            Image(uiImage: uiImage)
                                .resizable()
                                .scaledToFill()
                                .frame(width: 220, height: 160)
                                .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                        }
                    case .imageURL(let urlString):
                        if let url = URL(string: urlString) {
                            if url.isFileURL, let uiImage = ChatImageCacheManager.shared.image(fileURL: url) {
                                Image(uiImage: uiImage)
                                    .resizable()
                                    .scaledToFill()
                                    .frame(width: 220, height: 160)
                                    .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                            } else {
                                AsyncImage(url: url) { phase in
                                    switch phase {
                                    case .empty:
                                        RoundedRectangle(cornerRadius: 18, style: .continuous)
                                            .fill(.thinMaterial)
                                            .frame(width: 220, height: 160)
                                            .overlay(ProgressView().progressViewStyle(.circular))
                                    case .success(let image):
                                        image
                                            .resizable()
                                            .scaledToFill()
                                            .frame(width: 220, height: 160)
                                            .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
                                    case .failure:
                                        RoundedRectangle(cornerRadius: 18, style: .continuous)
                                            .fill(.thinMaterial)
                                            .frame(width: 220, height: 160)
                                            .overlay(Image(systemName: "photo").foregroundColor(.secondary))
                                    @unknown default:
                                        EmptyView()
                                    }
                                }
                            }
                        }
                    case .fileData(let name, _):
                        fileChip(title: name)
                    case .fileURL(let name, _):
                        fileChip(title: name)
                    default:
                        EmptyView()
                    }
    }

    @ViewBuilder
    private func fileChip(title: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: "doc.fill")
                .font(.system(size: 14, weight: .semibold))
                .foregroundColor(.secondary)
            Text(title)
                .font(.system(size: 15, weight: .semibold))
                .foregroundColor(.primary)
                .lineLimit(1)
            Spacer(minLength: 0)
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 12)
        .frame(width: 220)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
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

import SwiftUI
import UIKit

struct UserMessageBubbleView: View {
    let segments: [MessageSegment]

    @Environment(\.colorScheme) private var colorScheme

    var body: some View {
        let text = plainText(from: segments)
        let hasText = !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let attachmentSegments = segments.filter { isAttachmentSegment($0) }

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
                            .fill(userBubbleGradient)
                    )
                    .foregroundColor(.white)
                    .textSelection(.enabled)
                    .frame(maxWidth: 320, alignment: .trailing)
            }
        }
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

    private var userBubbleGradient: LinearGradient {
        let colors: [Color] = {
            // Neutral gray/black bubble, with a subtle lift in light mode.
            if colorScheme == .dark {
                return [Color(white: 0.22), Color(white: 0.12)]
            } else {
                return [Color(white: 0.25), Color(white: 0.15)]
            }
        }()
        return LinearGradient(colors: colors, startPoint: .topLeading, endPoint: .bottomTrailing)
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

struct MessageBubbleView: View {

    @ObservedObject var chatViewModel: ChatViewModel

    let message: ChatMessage

    var onSendToPartner: ((String) -> Void)? = nil
    var onRegenerate: (() -> Void)? = nil
    var onToast: ((String) -> Void)? = nil

    var sendAnimationNamespace: Namespace.ID? = nil
    var outgoingAnimatingMessageId: UUID? = nil

    @Environment(\.colorScheme) private var colorScheme
    @State private var isThinkingExpanded: Bool = false

    /// True for the streaming placeholder row where generation starts.
    private var shouldShowThinkingIndicator: Bool {
        guard !message.isFromUser else { return false }
        guard !message.isFromPartnerUser else { return false }
        guard chatViewModel.isAssistantTyping else { return false }
        guard chatViewModel.messages.last?.id == message.id else { return false }
        return !hasRenderableAssistantContent
    }

    /// Live thinking text from the current streaming message, if any.
    private var activeStreamingThinkingText: String? {
        guard !message.isFromUser, !message.isFromPartnerUser else { return nil }
        guard chatViewModel.messages.last?.id == message.id else { return nil }
        let text = chatViewModel.thinkingText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty else { return nil }
        return text
    }

    /// The thinking text to display: persisted summary, or live streaming text.
    private var effectiveThinkingText: String? {
        if let ts = message.thinkingSummary, !ts.isEmpty { return ts }
        return activeStreamingThinkingText
    }

    private var hasRenderableAssistantContent: Bool {
        if message.isToolLoading { return true }
        return message.segments.contains { segment in
            switch segment {
            case .text(let text):
                return !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            case .partnerMessage(let text, _):
                return !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            case .partnerReceived(let text):
                return !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            case .imageData(_), .imageURL(_), .fileData(_, _), .fileURL(_, _):
                return true
            }
        }
    }

    /// Whether to show action buttons under assistant messages
    private var shouldShowAssistantActions: Bool {
        // Don't show for user messages
        guard !message.isFromUser else { return false }
        // Don't show for partner messages
        guard !message.isFromPartnerUser else { return false }
        // Don't show while message is still loading
        guard !message.isToolLoading else { return false }
        // Don't show for messages generated during voice mode
        guard !message.isFromVoiceMode else { return false }
        // Don't show while this message is still streaming (check if it's the last message and loading)
        if chatViewModel.isLoading {
            let lastAssistantMessage = chatViewModel.messages.last { !$0.isFromUser }
            if lastAssistantMessage?.id == message.id {
                return false
            }
        }
        // Don't show for empty messages
        let text = plainText(from: message.segments)
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return false }
        // Check if this message only contains partner drafts/received
        let hasRegularContent = message.segments.contains { segment in
            if case .text(let t) = segment, !t.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                return true
            }
            return false
        }
        return hasRegularContent
    }

    @MainActor
    var body: some View {
        VStack(alignment: message.isFromUser ? .trailing : .leading, spacing: 4) {
            if message.isFromUser {
                let isSendAnimationTarget = message.id == outgoingAnimatingMessageId
                if isSendAnimationTarget, let ns = sendAnimationNamespace {
                    UserMessageBubbleView(segments: message.segments)
                        .matchedGeometryEffect(id: message.id, in: ns, properties: .position, isSource: false)
                } else {
                    UserMessageBubbleView(segments: message.segments)
                }
            } else {
                VStack(alignment: .leading, spacing: 8) {
                    if shouldShowThinkingIndicator {
                        if let thinkingText = activeStreamingThinkingText {
                            // Show "Thinking" header with live text (always expanded during streaming)
                            thinkingSectionView(text: thinkingText, alwaysExpanded: true)
                        } else {
                            ThinkingIndicatorView(
                                thinkingText: chatViewModel.thinkingText,
                                thinkingTextDone: chatViewModel.thinkingTextDone
                            )
                            .padding(.vertical, 4)
                        }
                    } else if message.isFromPartnerUser {
                        PartnerMessageBlockView(
                            text: plainText(from: message.segments),
                            senderUserId: message.senderUserId
                        )
                    } else if !message.segments.isEmpty {
                        // Collapsible thinking section
                        if let summary = effectiveThinkingText {
                            thinkingSectionView(text: summary, alwaysExpanded: false)
                        }

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
                            case .partnerMessage(let text, let ghostName):
                                if !text.isEmpty {
                                    let isSent = chatViewModel.partnerDrafts.isPartnerDraftSent(sessionId: chatViewModel.sessionId, messageContent: text)
                                    let isLinked = chatViewModel.isConnectedToFriendInThisChat
                                    PartnerDraftBlockView(
                                        initialText: text,
                                        isSent: isSent,
                                        isLinked: isLinked,
                                        recipientUserId: chatViewModel.selectedFriendUserId,
                                        ghostName: ghostName ?? message.ghostName
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
                        
                        // Action buttons for assistant messages
                        if shouldShowAssistantActions {
                            AssistantMessageActionsView(
                                messageText: plainText(from: message.segments),
                                regenerationCount: message.regenerationCount,
                                onRegenerate: onRegenerate,
                                onToast: onToast,
                                thinkingSummary: message.thinkingSummary,
                                onToggleThinking: {
                                    withAnimation(.easeInOut(duration: 0.2)) {
                                        isThinkingExpanded.toggle()
                                    }
                                }
                            )
                            .transition(.opacity.animation(.easeIn(duration: 0.2)))
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

    @ViewBuilder
    private func thinkingSectionView(text: String, alwaysExpanded: Bool) -> some View {
        let expanded = alwaysExpanded || isThinkingExpanded
        VStack(alignment: .leading, spacing: 8) {
            Button(action: {
                guard !alwaysExpanded else { return }
                withAnimation(.easeInOut(duration: 0.2)) {
                    isThinkingExpanded.toggle()
                }
            }) {
                HStack(spacing: 6) {
                    Text("Thinking")
                        .font(.body)
                        .foregroundColor(.secondary)
                    Image(systemName: expanded ? "chevron.down" : "chevron.right")
                        .font(.system(size: 11, weight: .semibold))
                        .foregroundColor(.secondary)
                }
            }
            .buttonStyle(.plain)

            if expanded {
                let paragraphs: [String] = {
                    let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
                    return trimmed.components(separatedBy: "\n\n").map { block in
                        let stripped = block.trimmingCharacters(in: .whitespacesAndNewlines)
                        if let parsed = try? AttributedString(markdown: stripped) {
                            return String(parsed.characters)
                        }
                        return stripped
                    }.filter { !$0.isEmpty }
                }()
                VStack(alignment: .leading, spacing: 12) {
                    ForEach(Array(paragraphs.enumerated()), id: \.offset) { _, paragraph in
                        Text(paragraph)
                            .font(.system(size: 14))
                            .foregroundStyle(.secondary)
                            .lineSpacing(2)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                }
                .padding(.horizontal, 4)
                .transition(.opacity)
            }
        }
        .padding(.horizontal, 4)
        .padding(.bottom, 6)
    }

    private func isAttachmentSegment(_ seg: MessageSegment) -> Bool {
        switch seg {
        case .imageData(_), .imageURL(_), .fileData(_, _), .fileURL(_, _):
            return true
        default:
            return false
        }
    }

    private var userBubbleGradient: LinearGradient {
        let colors: [Color] = {
            // Neutral gray/black bubble, with a subtle lift in light mode.
            if colorScheme == .dark {
                return [Color(white: 0.22), Color(white: 0.12)]
            } else {
                return [Color(white: 0.25), Color(white: 0.15)]
            }
        }()
        return LinearGradient(colors: colors, startPoint: .topLeading, endPoint: .bottomTrailing)
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

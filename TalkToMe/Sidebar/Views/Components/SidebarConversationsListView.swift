import SwiftUI

struct SidebarConversationsListView: View {
    let availableWidth: CGFloat
    let isSearchActive: Bool
    let searchText: String

    let pendingRequests: [BackendService.PartnerPendingRequest]
    let sessions: [ChatSession]
    let unreadSessionIds: Set<UUID>
    let isLinked: Bool

    let formatLastUsed: (String?) -> String
    let onTapPendingRequest: (BackendService.PartnerPendingRequest) -> Void
    let onTapSession: (ChatSession) -> Void
    let onRenameSession: (ChatSession) -> Void
    let onDeleteSession: (ChatSession) -> Void

    var body: some View {
        VStack(spacing: 10) {
            if !isSearchActive {
                if !pendingRequests.isEmpty {
                    SidebarPendingRequestsView(requests: pendingRequests, onTap: onTapPendingRequest)
                }

                HStack(spacing: 12) {
                    Text("Conversations")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundColor(.secondary)
                    Spacer()
                }
                .padding(.horizontal, 20)
                .padding(.top, 12)
            }

            let term = searchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            let filteredSessions: [ChatSession] = sessions.filter { session in
                term.isEmpty || session.title.lowercased().contains(term)
            }

            LazyVStack(spacing: 6) {
                ForEach(filteredSessions, id: \.id) { session in
                    Button(action: { onTapSession(session) }) {
                        SidebarConversationRowView(
                            title: session.title,
                            dateText: formatLastUsed(session.lastUsedISO8601),
                            previewText: previewText(for: session),
                            showUnreadDot: isLinked && unreadSessionIds.contains(session.id)
                        )
                    }
                    .buttonStyle(.plain)
                    .contextMenu {
                        Button("Rename", systemImage: "pencil") { onRenameSession(session) }
                        Button(role: .destructive) { onDeleteSession(session) } label: {
                            Label("Delete", systemImage: "trash")
                        }
                    }
                }
            }
            .padding(.top, 4)
            .padding(.horizontal, 20)
            .padding(.bottom, 20)
        }
        .padding(.top, 8)
        .padding(.bottom, 80)
    }

    private func previewText(for session: ChatSession) -> String {
        let previewTargetWidth = availableWidth * 0.88
        let rawPreview = shouldShowLastMessage(session.lastMessageContent) ? (session.lastMessageContent ?? "") : "No messages yet"
        let clipped = wordBoundaryTruncated(rawPreview, previewTargetWidth)
        return clipped + (clipped.count < rawPreview.count ? "…" : "")
    }

    private func shouldShowLastMessage(_ content: String?) -> Bool {
        guard let content = content else { return false }
        let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed.uppercased() != "NULL"
    }

    private func wordBoundaryTruncated(_ text: String, _ targetWidth: CGFloat) -> String {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return "" }
        // Approximate average character width for 14pt system font
        let avgCharWidth: CGFloat = 7.0
        let maxChars = max(8, Int((targetWidth / avgCharWidth).rounded(.down)))
        if trimmed.count <= maxChars { return trimmed }
        var result: String = ""
        for word in trimmed.split(separator: " ") {
            if result.isEmpty {
                if word.count > maxChars {
                    return String(word.prefix(maxChars))
                } else {
                    result = String(word)
                }
            } else {
                if result.count + 1 + word.count > maxChars { break }
                result += " " + word
            }
        }
        return result
    }
}



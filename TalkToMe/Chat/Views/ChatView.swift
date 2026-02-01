import SwiftUI

struct ChatView: View {

    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @StateObject private var viewModel: ChatViewModel

    @FocusState private var isInputFocused: Bool

    @State private var showFriendPicker: Bool = false
    @State private var pendingPartnerDraftText: String? = nil
    @State private var isFriendPickerLoading: Bool = false
    @State private var friendPickerErrorMessage: String? = nil
    @State private var showRenameChatPrompt: Bool = false
    @State private var renameChatTitle: String = ""
    @State private var showDeleteChatConfirm: Bool = false
    @State private var showReportChatConfirm: Bool = false
    @State private var showReportChatThanks: Bool = false

    init(sessionId: UUID? = nil) {
        _viewModel = StateObject(wrappedValue: ChatViewModel(sessionId: sessionId))
    }

    private var hasSentPartnerMessageInThisChat: Bool {
        viewModel.messages.contains(where: { msg in
            msg.segments.contains(where: { seg in
                if case .partnerMessage(_) = seg { return true }
                return false
            })
        })
    }

    private var friendForProfilePictureDisplay: FriendSummary? {
        guard hasSentPartnerMessageInThisChat else { return nil }
        guard let friendId = viewModel.selectedFriendUserId else { return nil }
        return friendsViewModel.friends.first(where: { $0.id == friendId })
    }

    private var activeSessionIdForActions: UUID? {
        sessionsViewModel.activeSessionId ?? viewModel.sessionId
    }

    private var activeSessionTitleForActions: String {
        guard let sid = activeSessionIdForActions else { return ChatSession.defaultTitle }
        return sessionsViewModel.sessions.first(where: { $0.id == sid })?.title ?? ChatSession.defaultTitle
    }

    @MainActor
    var body: some View {
        ChatScreenView(
            chatViewModel: viewModel,
            onSend: { handleSendTapped() },
            isInputFocused: $isInputFocused
        )
        .task {
            await viewModel.preconnectDictationSTTIfNeeded()
        }
        .toolbar {
            ToolbarItemGroup(placement: .topBarTrailing) {
                Button("New chat", systemImage: "square.and.pencil") {
                    sessionsViewModel.startNewChat()
                }

                Menu {
                    Button("Rename chat", systemImage: "pencil") {
                        guard activeSessionIdForActions != nil else { return }
                        renameChatTitle = activeSessionTitleForActions
                        showRenameChatPrompt = true
                    }
                    .disabled(activeSessionIdForActions == nil)

                    Button("Report chat", systemImage: "exclamationmark.bubble") {
                        showReportChatConfirm = true
                    }
                    .disabled(activeSessionIdForActions == nil)

                    Button("Delete chat", systemImage: "trash", role: .destructive) {
                        showDeleteChatConfirm = true
                    }
                    .disabled(activeSessionIdForActions == nil)
                } label: {
                    Label("More", systemImage: "ellipsis")
                }
            }

            if #available(iOS 26.0, *) {
                ToolbarSpacer(.fixed, placement: .topBarTrailing)
            }

            ToolbarItem(placement: .topBarTrailing) {
                if let friend = friendForProfilePictureDisplay {
                    SidebarAvatarView(avatarURL: friend.avatarURL)
                        .frame(width: 36, height: 36)
                        .clipShape(Circle())
                }
            }
        }
        .alert("Rename chat", isPresented: $showRenameChatPrompt) {
            TextField("Title", text: $renameChatTitle)
            Button("Cancel", role: .cancel) {}
            Button("Save") {
                let title = renameChatTitle.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !title.isEmpty else { return }
                // UI-only for now (no rename action wired).
                Haptics.impact(.light)
            }
        } message: {
            Text("Give this chat a new title.")
        }
        .confirmationDialog("Delete chat?", isPresented: $showDeleteChatConfirm, titleVisibility: .visible) {
            Button("Delete", role: .destructive) {
                // UI-only for now (no delete action wired).
                Haptics.impact(.light)
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will delete the chat and its messages.")
        }
        .confirmationDialog("Report chat?", isPresented: $showReportChatConfirm, titleVisibility: .visible) {
            Button("Report", role: .destructive) {
                // TODO: Wire this to a backend/reporting endpoint.
                showReportChatThanks = true
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will flag the conversation for review. (Not wired yet)")
        }
        .alert("Thanks", isPresented: $showReportChatThanks) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Report received. (Not wired yet)")
        }
        .sheet(isPresented: $showFriendPicker) {
            FriendPickerSheetView(
                isPresented: $showFriendPicker,
                friends: friendsViewModel.friends,
                isLoading: isFriendPickerLoading,
                errorMessage: friendPickerErrorMessage,
                onRetry: {
                    Task { @MainActor in
                        await refreshFriendsForPicker()
                    }
                },
                onPick: { friendId in
                    Task { @MainActor in
                        viewModel.selectedFriendUserId = friendId
                        showFriendPicker = false
                        if let draft = pendingPartnerDraftText {
                            pendingPartnerDraftText = nil
                            await viewModel.sendPartnerDraftToSelectedFriend(draft)
                        }
                    }
                }
            )
            .presentationDetents([.medium, .large])
            .presentationDragIndicator(.visible)
        }
        .onReceive(NotificationCenter.default.publisher(for: .sendPartnerMessageFromBubble)) { note in
            let content = (note.userInfo?["content"] as? String)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            guard !content.isEmpty else { return }
            Task { @MainActor in
                if viewModel.isConnectedToFriendInThisChat {
                    await viewModel.sendPartnerDraftViaSession(content)
                    return
                }
                // If not connected, we must prompt the user to pick a friend (or show the empty-state).
                pendingPartnerDraftText = content
                showFriendPicker = true
                await refreshFriendsForPicker()
            }
        }
        .animation(nil, value: viewModel.messages.isEmpty)
    }

    private func handleSendTapped() {
        Task { @MainActor in
            // IMPORTANT: The main chat send button always sends to the AI chat.
            // Friend selection is only required for "send to partner" actions (partner draft blocks),
            // which are handled by `.sendPartnerMessageFromBubble`.
            viewModel.sendComposerMessage()
        }
    }

    @MainActor
    private func refreshFriendsForPicker() async {
        // Only block the UI with a loader if we truly have nothing to show yet.
        let hasCachedFriends = !friendsViewModel.friends.isEmpty
        isFriendPickerLoading = !hasCachedFriends
        friendPickerErrorMessage = nil
        do {
            try await friendsViewModel.loadFriends()
        } catch {
            // If we already have friends to show, keep the sheet usable and don't block it with an error state.
            if !hasCachedFriends {
                friendPickerErrorMessage = error.localizedDescription
            }
        }
        isFriendPickerLoading = false
    }
}

#if DEBUG
#Preview {
    ChatView()
        .environmentObject(ChatSessionsViewModel())
        .environmentObject(FriendsViewModel(accessTokenProvider: { "" }))
}
#endif

private struct FriendPickerSheetView: View {
    @Binding var isPresented: Bool
    let friends: [FriendSummary]
    let isLoading: Bool
    let errorMessage: String?
    let onRetry: () -> Void
    let onPick: (UUID) -> Void

    var body: some View {
        NavigationStack {
            VStack(spacing: 12) {
                Text("Pick a friend")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(.secondary)
                    .padding(.top, 6)

                if friends.isEmpty {
                    if let errorMessage, !errorMessage.isEmpty {
                        VStack(spacing: 10) {
                            Text(errorMessage)
                                .font(.system(size: 14))
                                .foregroundStyle(.secondary)
                                .multilineTextAlignment(.center)

                            Button("Retry") {
                                Haptics.impact(.light)
                                onRetry()
                            }
                            .buttonStyle(.bordered)
                        }
                        .padding(.top, 6)
                    } else if isLoading {
                        VStack(spacing: 10) {
                            ProgressView()
                            Text("Loading friends…")
                                .font(.system(size: 14))
                                .foregroundStyle(.secondary)
                        }
                        .padding(.top, 6)
                    } else {
                        Text("No friends yet. Add one with a 4-digit code first.")
                            .font(.system(size: 14))
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                            .padding(.top, 6)
                    }
                } else {
                    // If we already have cached friends, show the list immediately.
                    // We can still refresh in the background without blocking selection.
                    if isLoading {
                        HStack(spacing: 8) {
                            ProgressView().controlSize(.small)
                            Text("Refreshing…")
                                .font(.system(size: 13, weight: .medium))
                                .foregroundStyle(.secondary)
                            Spacer()
                        }
                        .padding(.top, 2)
                    } else if let errorMessage, !errorMessage.isEmpty {
                        HStack(spacing: 10) {
                            Text(errorMessage)
                                .font(.system(size: 13))
                                .foregroundStyle(.secondary)
                                .lineLimit(2)
                            Spacer()
                            Button("Retry") {
                                Haptics.impact(.light)
                                onRetry()
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                        }
                        .padding(.top, 2)
                    }

                    List {
                        ForEach(friends) { friend in
                            Button {
                                Haptics.impact(.light)
                                onPick(friend.id)
                            } label: {
                                HStack {
                                    SidebarAvatarView(avatarURL: friend.avatarURL)
                                        .frame(width: 34, height: 34)
                                        .clipShape(Circle())

                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(friend.fullName.isEmpty ? "Friend" : friend.fullName)
                                            .font(.system(size: 16, weight: .semibold))
                                            .foregroundStyle(.primary)
                                    }
                                    Spacer()
                                    Image(systemName: "chevron.right")
                                        .font(.system(size: 12, weight: .semibold))
                                        .foregroundStyle(.secondary)
                                }
                                .padding(.vertical, 6)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .listStyle(.plain)
                }

                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16)
            .navigationTitle("Send to…")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Cancel") {
                        pendingDismiss()
                    }
                }
            }
        }
    }

    private func pendingDismiss() {
        isPresented = false
    }
}


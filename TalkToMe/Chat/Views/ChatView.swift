import SwiftUI

struct ChatView: View {

    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @StateObject private var viewModel: ChatViewModel

    @State private var showFriendPicker: Bool = false
    @State private var pendingPartnerDraftText: String? = nil
    @State private var isFriendPickerLoading: Bool = false
    @State private var friendPickerErrorMessage: String? = nil
    @State private var showRenameChatPrompt: Bool = false
    @State private var renameChatTitle: String = ""
    @State private var showDeleteChatConfirm: Bool = false
    @State private var showReportChatConfirm: Bool = false
    @State private var showReportChatThanks: Bool = false

    @FocusState private var isInputFocused: Bool

    let onBack: (() -> Void)?

    init(sessionId: UUID? = nil, onBack: (() -> Void)? = nil) {
        _viewModel = StateObject(wrappedValue: ChatViewModel(sessionId: sessionId))
        self.onBack = onBack
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
        NavigationStack {
            ChatScreenView(
                chatViewModel: viewModel,
                onSend: { handleSendTapped() },
                isInputFocused: $isInputFocused
            )
            .task {
                await viewModel.voiceController.preconnectDictationSTTIfNeeded()
            }
            .toolbar {
                if let onBack {
                    ToolbarItem(placement: .topBarLeading) {
                        Button {
                            Haptics.impact(.light)
                            onBack()
                        } label: {
                            HStack(spacing: 4) {
                                Image(systemName: "chevron.left")
                                    .font(.system(size: 17, weight: .semibold))
                                Text("Back")
                            }
                        }
                    }
                }

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
        }
        .alert("Rename chat", isPresented: $showRenameChatPrompt) {
            TextField("Title", text: $renameChatTitle)
            Button("Cancel", role: .cancel) {}
            Button("Save") {
                let title = renameChatTitle.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !title.isEmpty, let sessionId = activeSessionIdForActions else { return }
                Haptics.impact(.light)
                Task { @MainActor in
                    await sessionsViewModel.renameSession(sessionId, to: title)
                }
            }
        } message: {
            Text("Give this chat a new title.")
        }
        .confirmationDialog("Delete chat?", isPresented: $showDeleteChatConfirm, titleVisibility: .visible) {
            Button("Delete", role: .destructive) {
                guard let sessionId = activeSessionIdForActions else { return }
                Haptics.impact(.light)
                Task { @MainActor in
                    await sessionsViewModel.deleteSession(sessionId)
                    // Navigate away after deletion
                    if let onBack {
                        onBack()
                    } else {
                        sessionsViewModel.startNewChat()
                    }
                }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("This will delete the chat and all its messages. If this chat is linked, it will be deleted on their end too.")
        }
        .confirmationDialog("Report chat?", isPresented: $showReportChatConfirm, titleVisibility: .visible) {
            Button("Report", role: .destructive) {
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
            viewModel.voiceController.sendComposerMessage()
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
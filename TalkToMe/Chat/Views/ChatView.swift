import SwiftUI

struct ChatView: View {

    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var friendsViewModel: FriendsViewModel

    @StateObject private var viewModel: ChatViewModel
    @StateObject private var sessionActions = ChatSessionActionsCoordinator()

    @State private var showFriendPicker: Bool = false
    @State private var pendingPartnerDraftText: String? = nil
    @State private var isFriendPickerLoading: Bool = false
    @State private var friendPickerErrorMessage: String? = nil

    @FocusState private var isInputFocused: Bool

    let onBack: (() -> Void)?

    init(sessionId: UUID? = nil, onBack: (() -> Void)? = nil) {
        _viewModel = StateObject(wrappedValue: ChatViewModel(sessionId: sessionId))
        self.onBack = onBack
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
                async let dictationWarmup: Void = viewModel.voiceController.preconnectDictationSTTIfNeeded()
                async let speakWarmup: Void = viewModel.voiceController.preconnectSpeakServicesIfNeeded()
                _ = await (dictationWarmup, speakWarmup)
            }
            .toolbar {
                if let onBack {
                    ToolbarItem(placement: .topBarLeading) {
                        Button {
                            onBack()
                        } label: {
                            Image(systemName: "chevron.left")
                                .font(.system(size: 17, weight: .semibold))
                        }
                    }
                }

                ToolbarItemGroup(placement: .topBarTrailing) {
                    ChatSessionActionsMenu(
                        sessionId: activeSessionIdForActions,
                        currentTitle: activeSessionTitleForActions,
                        onRenameRequest: { sessionActions.requestRename(currentTitle: activeSessionTitleForActions) },
                        onArchiveRequest: { sessionActions.requestArchive() },
                        onDeleteRequest: { sessionActions.requestDelete() },
                        onReportRequest: { sessionActions.requestReport() }
                    )
                }
            }
        }
        .chatSessionActions(
            coordinator: sessionActions,
            sessionId: activeSessionIdForActions,
            onRename: { sessionId, title in
                await sessionsViewModel.renameSession(sessionId, to: title)
            },
            onDelete: { sessionId in
                await sessionsViewModel.deleteSession(sessionId)
            },
            onDeleteNavigateAway: {
                if let onBack {
                    onBack()
                } else {
                    sessionsViewModel.startNewChat()
                }
            }
        )
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
        let hasCachedFriends = !friendsViewModel.friends.isEmpty
        isFriendPickerLoading = !hasCachedFriends
        friendPickerErrorMessage = nil
        do {
            try await friendsViewModel.loadFriends()
        } catch {
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

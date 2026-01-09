import SwiftUI

struct SidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var linkVM: LinkViewModel

    @FocusState private var isSearchFieldFocused: Bool

    @Binding var isOpen: Bool

    @State private var searchText: String = ""

    @State private var showRenameSheet: Bool = false
    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil

    let profileNamespace: Namespace.ID

    private var isSearchActive: Bool {
        if isSearchFieldFocused { return true }
        return !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var shouldShowPartnerBanner: Bool {
        if isSearchActive { return false }
        if !sessionsViewModel.pendingRequests.isEmpty { return false }
        if sessionsViewModel.partnerInfo?.linked == true { return false }
        if case .linked = linkVM.state { return false }
        return true
    }

    var body: some View {
        NavigationStack {
            GeometryReader { geometry in
                VStack(spacing: 0) {
                    SidebarHeaderView(
                        avatarURL: sessionsViewModel.myAvatarURL,
                        profileNamespace: profileNamespace,
                        searchText: $searchText,
                        isSearchActive: isSearchActive,
                        isSearchFieldFocused: $isSearchFieldFocused,
                        onTapProfile: {
                            Haptics.impact(.light)
                            navigationViewModel.showSettingsSheet = true
                        },
                        onCloseSearch: {
                            Haptics.impact(.light)
                            searchText = ""
                            isSearchFieldFocused = false
                        },
                        onOpenSettings: {
                            Haptics.impact(.medium)
                            navigationViewModel.showSettingsSheet = true
                        },
                        onNewChat: {
                            Haptics.impact(.light)
                            withAnimation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0)) {
                                sessionsViewModel.startNewChat()
                                navigationViewModel.selectedTab = .chat
                                isOpen = false
                            }
                        }
                    )

                    ScrollView {
                        SidebarConversationsListView(
                            availableWidth: geometry.size.width,
                            isSearchActive: isSearchActive,
                            searchText: searchText,
                            pendingRequests: sessionsViewModel.pendingRequests,
                            sessions: sessionsViewModel.sessions,
                            unreadSessionIds: sessionsViewModel.unreadPartnerSessionIds,
                            isLinked: (linkVM.state == .linked) || (sessionsViewModel.partnerInfo?.linked == true),
                            formatLastUsed: sessionsViewModel.formatLastUsed,
                            onTapPendingRequest: { request in
                                withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                    sessionsViewModel.openPendingRequest(request)
                                    isOpen = false
                                }
                            },
                            onTapSession: { session in
                                withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                    sessionsViewModel.openSession(session.id)
                                    navigationViewModel.selectedTab = .chat
                                    isOpen = false
                                }
                            },
                            onRenameSession: { session in
                                renameTargetId = session.id
                                renameText = (session.title == ChatSession.defaultTitle) ? "" : session.title
                                showRenameSheet = true
                            },
                            onDeleteSession: { session in
                                Task { await sessionsViewModel.deleteSession(session.id) }
                            }
                        )
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                    .refreshable { await sessionsViewModel.refreshSessions() }
                    .scrollIndicators(.hidden)
                }
                .overlay(alignment: .bottom) {
                    SidebarPartnerInviteOverlayView(isVisible: !isSearchActive && shouldShowPartnerBanner)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar(.hidden, for: .navigationBar)
            .onChange(of: isOpen) { _, open in
                if !open {
                    searchText = ""
                    isSearchFieldFocused = false
                }
            }
            .onAppear {
                Task { await sessionsViewModel.ensureProfilePictureCached() }
            }
            .sheet(isPresented: $showRenameSheet) {
                SidebarRenameSheetView(
                    isPresented: $showRenameSheet,
                    renameText: $renameText,
                    onSave: { text in
                        if let id = renameTargetId {
                            Task { await sessionsViewModel.renameSession(id, to: text) }
                        }
                    }
                )
            }
        }
}
}
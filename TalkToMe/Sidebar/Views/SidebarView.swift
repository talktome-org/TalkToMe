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

// MARK: - Components (inlined)
//
// These are kept in this file on purpose to make iterating on the Sidebar UI easier
// without jumping between many small files.

struct SidebarHeaderView: View {

    @Environment(\.colorScheme) private var colorScheme

    let avatarURL: String?
    let profileNamespace: Namespace.ID

    @Binding var searchText: String
    let isSearchActive: Bool
    let isSearchFieldFocused: FocusState<Bool>.Binding

    let onTapProfile: () -> Void
    let onCloseSearch: () -> Void
    let onOpenSettings: () -> Void
    let onNewChat: () -> Void

    var body: some View {
        let pillShape = RoundedRectangle(cornerRadius: 999, style: .continuous)

        HStack(spacing: 12) {
            Button(action: onTapProfile) {
                SidebarAvatarView(avatarURL: avatarURL)
                    .frame(width: 40, height: 40)
                    .clipShape(Circle())
                    .overlay(
                        Circle()
                            .stroke(Color.white.opacity(0.18), lineWidth: 1)
                    )
            }
            .buttonStyle(.plain)

            HStack(spacing: 10) {
                Image(systemName: "magnifyingglass")
                    .font(.system(size: 16, weight: .semibold))
                    .foregroundStyle(.secondary)

                TextField("Search conversations", text: $searchText)
                    .focused(isSearchFieldFocused)
                    .submitLabel(.search)
                    .textInputAutocapitalization(.never)
                    .disableAutocorrection(true)
                    .font(.system(size: 16, weight: .regular))
                    .foregroundStyle(.primary)
            }
            .padding(.vertical, 10)
            .padding(.horizontal, 14)
            .frame(maxWidth: .infinity)
            .background {
                if #available(iOS 26.0, *) {
                    pillShape
                        .fill(.clear)
                        .glassEffect()
                } else {
                    pillShape
                        .fill(.ultraThinMaterial)
                }
            }
            .clipShape(pillShape)
            .overlay(
                pillShape
                    .stroke(Color.white.opacity(colorScheme == .dark ? 0.14 : 0.18), lineWidth: 1)
            )

            if isSearchActive {
                GlassCircleButton(systemName: "xmark", action: onCloseSearch)
                    .accessibilityLabel("Close search")
            } else {
                GlassCircleButton(systemName: "gearshape", action: onOpenSettings)
                    .matchedGeometryEffect(id: "settingsGearIcon", in: profileNamespace)
                    .accessibilityLabel("Settings")

                GlassCircleButton(systemName: "square.and.pencil", action: onNewChat)
                    .accessibilityLabel("New chat")
            }
        }
        .padding(.horizontal, 16)
        .padding(.top, 10)
        .padding(.bottom, 8)
        .background(Color(.systemBackground).opacity(0.96))
    }
}

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

struct SidebarConversationRowView: View {
    let title: String
    let dateText: String
    let previewText: String
    let showUnreadDot: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(title)
                    .font(.system(size: 18, weight: .regular))
                    .foregroundColor(.primary)
                Spacer()
                Text(dateText)
                    .font(.system(size: 12))
                    .foregroundColor(.secondary)
            }

            HStack(spacing: 6) {
                Text(previewText)
                    .font(.system(size: 14))
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                    .truncationMode(.tail)
                Spacer()
                if showUnreadDot {
                    Circle()
                        .fill(Color(red: 0.4, green: 0.2, blue: 0.6))
                        .frame(width: 14, height: 14)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 2)
        .padding(.vertical, 12)
    }
}

struct SidebarPendingRequestsView: View {
    let requests: [BackendService.PartnerPendingRequest]
    let onTap: (BackendService.PartnerPendingRequest) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            ForEach(requests, id: \.id) { request in
                Button(action: { onTap(request) }) {
                    PendingRequestRowView(request: request)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 16, style: .continuous)
                .fill(Color(.systemGray6))
        )
        .padding(.horizontal, 16)
    }
}

struct SidebarPartnerInviteOverlayView: View {
    let isVisible: Bool

    var body: some View {
        if isVisible {
            PartnerInviteBannerView()
                .padding(.horizontal, 20)
                .padding(.bottom, 20)
                .transition(.opacity.combined(with: .scale(scale: 0.95)))
                .animation(.spring(response: 0.3, dampingFraction: 0.8), value: isVisible)
                .ignoresSafeArea(.keyboard, edges: .bottom)
                .ignoresSafeArea(.container, edges: .bottom)
                .zIndex(10)
        }
    }
}

struct SidebarRenameSheetView: View {
    @Binding var isPresented: Bool
    @Binding var renameText: String
    let onSave: (String) -> Void

    var body: some View {
        VStack(spacing: 16) {
            Text("Rename Conversation")
                .font(.system(size: 20, weight: .semibold))

            TextField("Title", text: $renameText)
                .textInputAutocapitalization(.sentences)
                .disableAutocorrection(true)
                .padding(12)
                .background(Color(.secondarySystemBackground))
                .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))

            HStack {
                Button("Cancel") { isPresented = false }
                Spacer()
                Button("Save") {
                    let text = renameText
                    isPresented = false
                    onSave(text)
                }
                .disabled(renameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
            .padding(.top, 6)
        }
        .padding(20)
        .presentationDetents([.medium])
    }
}

struct GlassCircleButton: View {
    let systemName: String
    let action: () -> Void

    var body: some View {
        Button(action: action) {
            Image(systemName: systemName)
                .font(.system(size: 18, weight: .semibold))
                .foregroundStyle(.primary)
                .frame(width: 44, height: 44)
                .background {
                    if #available(iOS 26.0, *) {
                        Circle()
                            .fill(.clear)
                            .glassEffect()
                    } else {
                        Circle()
                            .fill(.ultraThinMaterial)
                    }
                }
                .clipShape(Circle())
                .overlay(
                    Circle()
                        .stroke(Color.white.opacity(0.16), lineWidth: 1)
                )
        }
        .buttonStyle(.plain)
    }
}
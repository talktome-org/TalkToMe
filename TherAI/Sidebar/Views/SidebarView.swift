import SwiftUI

struct SlidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var linkVM: LinkViewModel

    @Environment(\.colorScheme) private var colorScheme
    @Environment(\.isSearching) private var isSearching

    @Binding var isOpen: Bool

    @State private var searchText: String = ""
    @State private var isSearchPresented: Bool = false

    // Rename sheet state
    @State private var showRenameSheet: Bool = false
    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil

    // Partner accepted banner moved to ChatView

    let profileNamespace: Namespace.ID

    private var isSearchActive: Bool {
        if isSearching { return true }
        if isSearchPresented { return true }
        return !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var shouldShowPartnerBanner: Bool {
        if isSearchActive { return false }
        if !sessionsViewModel.pendingRequests.isEmpty { return false }
        if sessionsViewModel.partnerInfo?.linked == true { return false }
        if case .linked = linkVM.state { return false }
        return true
    }

    private func shouldShowLastMessage(_ content: String?) -> Bool {
        guard let content = content else { return false }
        let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed.uppercased() != "NULL"
    }

    // Approximate truncation to a full word before ellipsis based on target width
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
                    // If a single word is too long, hard cut it
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

    var body: some View {
        NavigationStack {
            GeometryReader { geometry in
                VStack(spacing: 0) {

                ScrollView {
                    sidebarContent(geometry)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                .refreshable {
                    await sessionsViewModel.refreshSessions()
                }
                .scrollIndicators(.hidden)
                }
                .overlay(alignment: .bottom) { partnerInviteOverlay }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .navigationBarTitleDisplayMode(.inline)
            .searchable(text: $searchText, isPresented: $isSearchPresented, prompt: "Search conversations")
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    HStack(spacing: 10) {
                        // Compact avatar only (no name) - opens profile/settings sheet
                        Button(action: {
                            Haptics.impact(.light)
                            navigationViewModel.showSettingsSheet = true
                        }) {
                            SidebarAvatarView(avatarURL: sessionsViewModel.myAvatarURL)
                                .frame(width: 36, height: 36)
                                .clipShape(Circle())
                                .overlay(
                                    Circle()
                                        .stroke(Color.white.opacity(0.2), lineWidth: 1)
                                )
                                .padding(.vertical, 2)
                        }
                        .buttonStyle(PlainButtonStyle())

                        // Settings gear button
                        Button(action: {
                            Haptics.impact(.medium)
                            navigationViewModel.showSettingsSheet = true
                        }) {
                            Image(systemName: "gearshape")
                                .font(.system(size: 22, weight: .semibold))
                                .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                                .frame(width: 40, height: 40)
                                .matchedGeometryEffect(id: "settingsGearIcon", in: profileNamespace)
                        }
                        .buttonStyle(PlainButtonStyle())
                    }
                    .padding(.leading, 4)
                }
                ToolbarItem(placement: .topBarTrailing) {
                    Button(action: {
                        Haptics.impact(.light)
                        withAnimation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0)) {
                            sessionsViewModel.startNewChat()
                            navigationViewModel.selectedTab = .chat
                            isOpen = false
                        }
                    }) {
                        Image(systemName: "square.and.pencil")
                            .font(.system(size: 20, weight: .semibold))
                            .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                            .frame(width: 44, height: 44)
                            .offset(y: -1.5)
                    }
                }
            }
            .onChange(of: isOpen) { _, open in
                if !open {
                    // Clear search when sidebar closes
                    searchText = ""
                }
            }
            .onReceive(NotificationCenter.default.publisher(for: .avatarChanged)) { _ in
                // Force refresh when avatar changes
                sessionsViewModel.objectWillChange.send()
            }
            .onAppear {
                // Ensure profile picture is cached when sidebar appears
                Task {
                    await sessionsViewModel.ensureProfilePictureCached()
                }
            }
            .sheet(isPresented: $showRenameSheet) {
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
                        Button("Cancel") { showRenameSheet = false }
                        Spacer()
                        Button("Save") {
                            let target = renameTargetId
                            let text = renameText
                            showRenameSheet = false
                            if let id = target {
                                Task { await sessionsViewModel.renameSession(id, to: text) }
                            }
                        }
                        .disabled(renameText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                    }
                    .padding(.top, 6)
                }
                .padding(20)
                .presentationDetents([.medium])
            }
            // Partner link banner handled in ChatView
        }
    }
}

// MARK: - Extracted Content to Aid Type-Checking
extension SlidebarView {
    @ViewBuilder
    private func sidebarContent(_ geometry: GeometryProxy) -> some View {
        VStack(spacing: 10) {
            if !isSearchActive {
                let hasPending = !sessionsViewModel.pendingRequests.isEmpty

                if hasPending {
                    VStack(alignment: .leading, spacing: 12) {
                        ForEach(sessionsViewModel.pendingRequests, id: \.id) { request in
                            Button(action: {
                                withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                    sessionsViewModel.openPendingRequest(request)
                                    isOpen = false
                                }
                            }) {
                                HStack(alignment: .top, spacing: 12) {
                                    Image(systemName: "person.crop.circle.badge.plus")
                                        .font(.system(size: 18, weight: .medium))
                                        .foregroundColor(Color(red: 0.4, green: 0.2, blue: 0.6))
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text("Partner Request")
                                            .font(.system(size: 14, weight: .medium))
                                            .foregroundColor(.primary)
                                        Text(request.content)
                                            .font(.system(size: 13))
                                            .foregroundColor(.secondary)
                                            .lineLimit(2)
                                    }
                                    Spacer()
                                }
                                .padding(12)
                                .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                                .overlay(
                                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                                        .stroke(Color.white.opacity(0.25), lineWidth: 0.5)
                                )
                            }
                            .buttonStyle(PlainButtonStyle())
                        }
                    }
                    .padding(12)
                    .background(
                        RoundedRectangle(cornerRadius: 16, style: .continuous)
                            .fill(Color(.systemGray6))
                    )
                    .padding(.horizontal, 16)
                }

                VStack(spacing: 0) {
                    HStack(spacing: 12) {
                        Text("Conversations")
                            .font(.system(size: 14, weight: .semibold))
                            .foregroundColor(.secondary)
                        Spacer()
                    }
                    .padding(.horizontal, 20)
                    .padding(.top, 12)
                }
                .offset(y: isSearchActive ? -120 : 0)
                .opacity(isSearchActive ? 0 : 1)
                .animation(.spring(response: 0.4, dampingFraction: 0.8), value: isSearchActive)
            }

            let term = searchText.trimmingCharacters(in: .whitespacesAndNewlines)
            let lower = term.lowercased()
            let filteredSessions: [ChatSession] = sessionsViewModel.sessions.filter { session in
                if lower.isEmpty { return true }
                return session.displayTitle.lowercased().contains(lower)
            }

            LazyVStack(spacing: 6) {
                if !filteredSessions.isEmpty {
                    ForEach(filteredSessions, id: \.id) { session in
                        Button(action: {
                            withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                sessionsViewModel.openSession(session.id)
                                navigationViewModel.selectedTab = .chat
                                isOpen = false
                            }
                        }) {
                            VStack(alignment: .leading, spacing: 12) {
                                HStack {
                                    Text(session.displayTitle)
                                        .font(.system(size: 18, weight: .regular))
                                        .foregroundColor(.primary)
                                    Spacer()
                                    Text(sessionsViewModel.formatLastUsed(session.lastUsedISO8601))
                                        .font(.system(size: 12))
                                        .foregroundColor(.secondary)
                                }
                                HStack(spacing: 6) {
                                    let previewTargetWidth = geometry.size.width * 0.88
                                    let rawPreview = shouldShowLastMessage(session.lastMessageContent) ? session.lastMessageContent! : "No messages yet"
                                    let clippedPreview = wordBoundaryTruncated(rawPreview, previewTargetWidth)
                                    Text(clippedPreview + (clippedPreview.count < rawPreview.count ? "…" : ""))
                                        .font(.system(size: 14))
                                        .foregroundColor(.secondary)
                                        .lineLimit(1)
                                        .truncationMode(.tail)
                                        .frame(maxWidth: previewTargetWidth, alignment: .leading)
                                    Spacer()
                                    let isLinked = (linkVM.state == .linked) || (sessionsViewModel.partnerInfo?.linked == true)
                                    let hasUnread = sessionsViewModel.unreadPartnerSessionIds.contains(session.id)
                                    if isLinked && hasUnread {
                                        Circle()
                                            .fill(Color(red: 0.4, green: 0.2, blue: 0.6))
                                            .frame(width: 14, height: 14)
                                            .onAppear { }
                                    }
                                }
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.horizontal, 2)
                            .padding(.vertical, 12)
                        }
                        .buttonStyle(PlainButtonStyle())
                        .contextMenu {
                            Button("Rename", systemImage: "pencil") {
                                renameTargetId = session.id
                                renameText = (session.displayTitle == ChatSession.defaultTitle) ? "" : session.displayTitle
                                showRenameSheet = true
                            }
                            Button(role: .destructive) {
                                Task { await sessionsViewModel.deleteSession(session.id) }
                            } label: {
                                Label("Delete", systemImage: "trash")
                            }
                        }
                    }
                }
            }
            .padding(.top, 4)
            .padding(.horizontal, 20)
            .padding(.bottom, 20)
            .offset(y: isSearchActive ? -8 : 0)
            .animation(.spring(response: 0.4, dampingFraction: 0.8), value: isSearchActive)
        }
        .padding(.top, 8)
        .padding(.bottom, 80)
    }

    @ViewBuilder
    private var partnerInviteOverlay: some View {
        if !isSearchActive && shouldShowPartnerBanner {
            PartnerInviteBannerView()
                .padding(.horizontal, 20)
                .padding(.bottom, 20)
                .transition(.opacity.combined(with: .scale(scale: 0.95)))
                .animation(.spring(response: 0.3, dampingFraction: 0.8), value: shouldShowPartnerBanner)
                .ignoresSafeArea(.keyboard, edges: .bottom)
                .ignoresSafeArea(.container, edges: .bottom)
                .zIndex(10)
        } else {
            EmptyView()
        }
    }

    // partnerAddedBannerOverlay moved to ChatView
}

#Preview("Pending Requests") {
    struct PreviewWithPending: View {
        @Namespace var ns
        let navigationViewModel: SidebarNavigationViewModel
        let sessionsViewModel: ChatSessionsViewModel
        init() {
            let navVM = SidebarNavigationViewModel()
            let sessionsVM = ChatSessionsViewModel()
            sessionsVM.sessions = [
                ChatSession(id: UUID(), title: "Chat with Good Rudi", lastUsedISO8601: "2025-08-29T10:12:00Z", lastMessageContent: "Thanks for the help with the project! This was really useful and I learned a lot from our conversation."),
                ChatSession(id: UUID(), title: "Conversation name", lastUsedISO8601: "2025-09-01T08:00:00Z", lastMessageContent: "Can you explain how this works?"),
                ChatSession(id: UUID(), title: "Weekend plans", lastUsedISO8601: "2025-07-15T18:30:00Z", lastMessageContent: "Let's meet at the coffee shop at 2pm on Saturday")
            ]
            sessionsVM.isLoadingSessions = false
            // Seed example pending requests
            sessionsVM.pendingRequests = [
                BackendService.PartnerPendingRequest(
                    id: UUID(),
                    sender_user_id: UUID(),
                    sender_session_id: UUID(),
                    content: "Partner request: Share chat access?",
                    created_at: ISO8601DateFormatter().string(from: Date()),
                    status: "pending",
                    recipient_session_id: nil,
                    created_message_id: nil
                ),
                BackendService.PartnerPendingRequest(
                    id: UUID(),
                    sender_user_id: UUID(),
                    sender_session_id: UUID(),
                    content: "Invite from S: Connect for partner session",
                    created_at: ISO8601DateFormatter().string(from: Date()),
                    status: "pending",
                    recipient_session_id: nil,
                    created_message_id: nil
                )
            ]
            self.navigationViewModel = navVM
            self.sessionsViewModel = sessionsVM
        }
        var body: some View {
            SlidebarView(isOpen: .constant(true), profileNamespace: ns)
                .environmentObject(navigationViewModel)
                .environmentObject(sessionsViewModel)
                .environmentObject(LinkViewModel(accessTokenProvider: { "" }))
        }
    }
    return PreviewWithPending()
}

#Preview("Default") {
    struct PreviewNoPending: View {
        @Namespace var ns
        let navigationViewModel: SidebarNavigationViewModel
        let sessionsViewModel: ChatSessionsViewModel
        init() {
            let navVM = SidebarNavigationViewModel()
            let sessionsVM = ChatSessionsViewModel()
            sessionsVM.sessions = [
                ChatSession(id: UUID(), title: "Chat with Good Rudi", lastUsedISO8601: "2025-08-29T10:12:00Z", lastMessageContent: "Thanks for the help with the project! This was really useful and I learned a lot from our conversation."),
                ChatSession(id: UUID(), title: "Conversation name", lastUsedISO8601: "2025-09-01T08:00:00Z", lastMessageContent: "Can you explain how this works?"),
                ChatSession(id: UUID(), title: "Weekend plans", lastUsedISO8601: "2025-07-15T18:30:00Z", lastMessageContent: "Let's meet at the coffee shop at 2pm on Saturday")
            ]
            sessionsVM.isLoadingSessions = false
            sessionsVM.pendingRequests = []
            self.navigationViewModel = navVM
            self.sessionsViewModel = sessionsVM
        }
        var body: some View {
            SlidebarView(isOpen: .constant(true), profileNamespace: ns)
                .environmentObject(navigationViewModel)
                .environmentObject(sessionsViewModel)
                .environmentObject(LinkViewModel(accessTokenProvider: { "" }))
        }
    }
    return PreviewNoPending()
}
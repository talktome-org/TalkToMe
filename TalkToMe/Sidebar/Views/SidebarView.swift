import SwiftUI
import Contacts
import MessageUI
import UIKit

struct SidebarView: View {

    @EnvironmentObject private var navigationViewModel: SidebarNavigationViewModel
    @EnvironmentObject private var sessionsViewModel: ChatSessionsViewModel
    @EnvironmentObject private var linkVM: LinkViewModel
    @ObservedObject private var authService = AuthService.shared
    @ObservedObject private var networkMonitor = NetworkMonitor.shared
    @ObservedObject private var backendMonitor = BackendConnectionMonitor.shared

    @Environment(\.colorScheme) private var colorScheme

    @Binding var isOpen: Bool

    @FocusState private var isSearchFieldFocused: Bool

    @State private var searchText: String = ""
    @State private var showRenameSheet: Bool = false
    @State private var renameText: String = ""
    @State private var renameTargetId: UUID? = nil
    @State private var showAddFriendSheet: Bool = false
    @State private var lastKnownOnline: Bool? = nil
    @State private var reconnectTask: Task<Void, Never>? = nil
    @State private var reconnectPhase: ReconnectPhase? = nil
    @State private var lastBackendState: BackendConnectionMonitor.State? = nil

    let profileNamespace: Namespace.ID

    private var isSearchActive: Bool {
        if isSearchFieldFocused { return true }
        return !searchText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var body: some View {
        NavigationStack {
            GeometryReader { geometry in
                VStack(spacing: 0) {
                    ScrollView {
                        let availableWidth = geometry.size.width
                        let isLinked = (linkVM.state == .linked) || (sessionsViewModel.partnerInfo?.linked == true)

                        VStack(spacing: 10) {
                            if !isSearchActive {
                                if !sessionsViewModel.pendingRequests.isEmpty {
                                    VStack(alignment: .leading, spacing: 12) {
                                        ForEach(sessionsViewModel.pendingRequests, id: \.id) { request in
                                            Button(action: {
                                                withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                                    sessionsViewModel.openPendingRequest(request)
                                                    isOpen = false
                                                }
                                            }) {
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

                                HStack(spacing: 12) {
                                    Text("Conversations")
                                        .font(.system(size: 14, weight: .semibold))
                                        .foregroundColor(.secondary)
                                    if let status = sidebarStatus {
                                        sidebarStatusView(status)
                                    }
                                    Spacer()
                                    Button(action: {
                                        Haptics.impact(.light)
                                        showAddFriendSheet = true
                                        Task { await linkVM.ensureInviteReady() }
                                    }) {
                                        HStack(spacing: 8) {
                                            Text("ADD FRIEND")
                                                .font(.system(size: 12, weight: .semibold))
                                                .foregroundColor(.primary)
                                            Spacer(minLength: 0)
                                            Image(systemName: "plus")
                                                .font(.system(size: 12, weight: .semibold))
                                                .foregroundStyle(.primary)
                                        }
                                        .padding(.horizontal, 12)
                                        .padding(.vertical, 8)
                                        .frame(width: 132)
                                        .background {
                                            if #available(iOS 26.0, *) {
                                                Color.clear
                                                    .glassEffect(.regular, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                                                    .opacity(0.98)
                                            } else {
                                                RoundedRectangle(cornerRadius: 12, style: .continuous)
                                                    .fill(Color(.systemGray6))
                                            }
                                        }
                                    }
                                    .buttonStyle(.plain)
                                }
                                .padding(.horizontal, 20)
                                .padding(.top, 12)
                            }

                            let term = searchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                            let filteredSessions: [ChatSession] = sessionsViewModel.sessions.filter { session in
                                term.isEmpty || session.title.lowercased().contains(term)
                            }

                            LazyVStack(spacing: 6) {
                                if term.isEmpty,
                                   let err = sessionsViewModel.sessionsLoadError,
                                   !err.isEmpty,
                                   !sessionsViewModel.sessions.isEmpty,
                                   !sessionsViewModel.isLoadingSessions {
                                    HStack(spacing: 10) {
                                        Image(systemName: "exclamationmark.triangle.fill")
                                            .font(.system(size: 12, weight: .semibold))
                                            .foregroundStyle(.secondary)
                                        Text(err)
                                            .font(.system(size: 13))
                                            .foregroundStyle(.secondary)
                                            .lineLimit(2)
                                        Spacer()
                                        Button(action: {
                                            Haptics.impact(.light)
                                            Task { await sessionsViewModel.refreshSessions() }
                                        }) {
                                            Text("Retry")
                                                .font(.system(size: 13, weight: .semibold))
                                        }
                                        .buttonStyle(.bordered)
                                    }
                                    .padding(.vertical, 6)
                                }

                                if term.isEmpty && sessionsViewModel.sessions.isEmpty {
                                    if sessionsViewModel.isLoadingSessions {
                                        VStack(spacing: 10) {
                                            ProgressView()
                                            Text("Loading conversations…")
                                                .font(.system(size: 14))
                                                .foregroundStyle(.secondary)
                                        }
                                        .frame(maxWidth: .infinity)
                                        .padding(.vertical, 24)
                                    } else if let err = sessionsViewModel.sessionsLoadError, !err.isEmpty {
                                        VStack(spacing: 10) {
                                            Text(err)
                                                .font(.system(size: 14))
                                                .foregroundStyle(.secondary)
                                                .multilineTextAlignment(.center)
                                            Button(action: {
                                                Haptics.impact(.light)
                                                Task { await sessionsViewModel.refreshSessions() }
                                            }) {
                                                Text("Retry")
                                                    .font(.system(size: 14, weight: .semibold))
                                            }
                                            .buttonStyle(.bordered)
                                        }
                                        .frame(maxWidth: .infinity)
                                        .padding(.vertical, 24)
                                    } else {
                                        VStack(spacing: 10) {
                                            Text("No conversations yet")
                                                .font(.system(size: 14))
                                                .foregroundStyle(.secondary)
                                        }
                                        .frame(maxWidth: .infinity)
                                        .padding(.vertical, 24)
                                    }
                                }

                                ForEach(filteredSessions, id: \.id) { session in
                                    Button(action: {
                                        withAnimation(.spring(response: 0.3, dampingFraction: 0.8, blendDuration: 0)) {
                                            sessionsViewModel.openSession(session.id)
                                            navigationViewModel.selectedTab = .chat
                                            isOpen = false
                                        }
                                    }) {
                                        let title = session.title
                                        let dateText = sessionsViewModel.formatLastUsed(session.lastUsedISO8601)
                                        let previewText = previewText(for: session, availableWidth: availableWidth)
                                        let showUnreadDot = isLinked && sessionsViewModel.unreadPartnerSessionIds.contains(session.id)

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
                                    .buttonStyle(.plain)
                                    .contextMenu {
                                        Button("Rename", systemImage: "pencil") {
                                            renameTargetId = session.id
                                            renameText = (session.title == ChatSession.defaultTitle) ? "" : session.title
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
                            .padding(.top, 4)
                            .padding(.horizontal, 20)
                            .padding(.bottom, 20)
                        }
                        .padding(.top, 8)
                        .padding(.bottom, 80)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
                    .refreshable { await sessionsViewModel.refreshSessions() }
                    .scrollIndicators(.hidden)

                    let pillShape = RoundedRectangle(cornerRadius: 999, style: .continuous)

                    HStack(spacing: 12) {
                        Button(action: {
                            Haptics.impact(.light)
                            navigationViewModel.showSettingsSheet = true
                        }) {
                            SidebarAvatarView(avatarURL: sessionsViewModel.myAvatarURL)
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
                                .focused($isSearchFieldFocused)
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

                        Button(action: {
                            Haptics.impact(.light)
                            withAnimation(.spring(response: 0.28, dampingFraction: 0.92, blendDuration: 0)) {
                                sessionsViewModel.startNewChat()
                                navigationViewModel.selectedTab = .chat
                                isOpen = false
                            }
                        }) {
                            Image(systemName: "square.and.pencil")
                                .font(.system(size: 18, weight: .semibold))
                                .foregroundStyle(.primary)
                                .frame(width: 44, height: 44)
                                .offset(y: -1.5)
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
                        }
                        .buttonStyle(.plain)
                        .accessibilityLabel("New chat")

                        if isSearchActive {
                            Button(action: {
                                Haptics.impact(.light)
                                searchText = ""
                                isSearchFieldFocused = false
                            }) {
                                Image(systemName: "xmark")
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
                            }
                            .buttonStyle(.plain)
                            .accessibilityLabel("Close search")
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.top, 10)
                    .padding(.bottom, 12)
                    .background(Color(.systemBackground).opacity(0.96))
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
                lastKnownOnline = networkMonitor.isOnline
                lastBackendState = backendMonitor.state
            }
            .onChange(of: networkMonitor.isOnline, initial: false) { _, online in
                let wasOnline = lastKnownOnline
                lastKnownOnline = online

                if online == false {
                    reconnectTask?.cancel()
                    reconnectTask = nil
                    reconnectPhase = nil
                    return
                }

                // Only show Connecting/Updating when we *actually* transition from offline -> online.
                guard wasOnline == false else { return }

                // We only *display* Connecting here. The actual catch-up sync is triggered when the backend
                // is confirmed reachable again (BackendConnectionMonitor: connecting -> connected).
                reconnectTask?.cancel()
                reconnectTask = nil
                reconnectPhase = .connecting

                // If the backend is already reachable immediately, run catch-up right away.
                if backendMonitor.state == .connected {
                    startCatchUpSync()
                }
            }
            .onChange(of: backendMonitor.state, initial: false) { _, newState in
                let old = lastBackendState
                lastBackendState = newState

                // If we're offline, SidebarStatus handles it already.
                guard networkMonitor.isOnline else { return }
                guard AuthService.shared.isAuthenticated else { return }

                // When backend becomes reachable again after a "connecting" period (weak signal),
                // run a catch-up sync, just like Telegram.
                if old == .connecting && newState == .connected {
                    startCatchUpSync()
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
                            let text = renameText
                            showRenameSheet = false
                            if let id = renameTargetId {
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
            .sheet(isPresented: $showAddFriendSheet) {
                AddFriendContactsSheetView(isPresented: $showAddFriendSheet)
                    .environmentObject(linkVM)
            }
        }
    }


    private func previewText(for session: ChatSession, availableWidth: CGFloat) -> String {
        let previewTargetWidth = availableWidth * 0.88
        let rawPreview = shouldShowLastMessage(session.lastMessageContent) ? (session.lastMessageContent ?? "") : "No messages yet"
        let clipped = wordBoundaryTruncated(rawPreview, previewTargetWidth)
        return clipped + (clipped.count < rawPreview.count ? "…" : "")
    }

    private enum SidebarStatus {
        case offline
        case connecting
        case updating
    }

    private enum ReconnectPhase {
        case connecting
        case updating
    }

    private func startCatchUpSync() {
        reconnectTask?.cancel()
        reconnectTask = Task { @MainActor in
            reconnectPhase = .updating
            await AppSyncGate.shared.setSyncing(true)
            await withTaskGroup(of: Void.self) { group in
                group.addTask { await sessionsViewModel.loadPartnerInfo(prefetchAvatars: false) }
                group.addTask { await sessionsViewModel.loadPendingRequests() }
                group.addTask { await sessionsViewModel.loadSessions(ensurePartnerInfo: false) }
            }
            await AppSyncGate.shared.setSyncing(false)
            reconnectPhase = nil

            // After sync finishes, do non-critical prefetch in the background.
            Task { @MainActor in
                await sessionsViewModel.loadPairedAvatars()
                await sessionsViewModel.preloadAvatars()
                await sessionsViewModel.ensureProfilePictureCached()
            }
        }
    }

    private var sidebarStatus: SidebarStatus? {
        if networkMonitor.isOnline == false { return .offline }
        if authService.isCheckingAuth { return .connecting }
        if backendMonitor.state == .connecting { return .connecting }
        if reconnectPhase == .connecting { return .connecting }
        // Only show Updating when we're doing an actual "sync/bootstrap".
        // Background refreshes shouldn't pin the status for a long time.
        if reconnectPhase == .updating { return .updating }
        if sessionsViewModel.isBootstrapping { return .updating }
        if sessionsViewModel.isLoadingSessions && sessionsViewModel.sessions.isEmpty { return .updating }
        return nil
    }

    @ViewBuilder
    private func sidebarStatusView(_ status: SidebarStatus) -> some View {
        let pill = Capsule(style: .continuous)
        HStack(spacing: 6) {
            switch status {
            case .offline:
                Image(systemName: "wifi.slash")
                    .font(.system(size: 11, weight: .semibold))
                    .foregroundStyle(.secondary)
                Text("Offline")
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundStyle(.secondary)
            case .connecting:
                ProgressView()
                    .controlSize(.mini)
                Text("Connecting…")
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundStyle(.secondary)
            case .updating:
                ProgressView()
                    .controlSize(.mini)
                Text("Updating…")
                    .font(.system(size: 12, weight: .semibold))
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .background(.thinMaterial, in: pill)
        .overlay(
            pill.stroke(Color.white.opacity(colorScheme == .dark ? 0.10 : 0.14), lineWidth: 1)
        )
    }

    private func shouldShowLastMessage(_ content: String?) -> Bool {
        guard let content = content else { return false }
        let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed.uppercased() != "NULL"
    }

    private func wordBoundaryTruncated(_ text: String, _ targetWidth: CGFloat) -> String {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty { return "" }
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

// MARK: - Add Friend (Contacts + Messages)

private struct InviteContact: Identifiable, Hashable {
    let id: String
    let givenName: String
    let familyName: String
    let thumbnailImageData: Data?
    let phoneNumbers: [String]

    var displayName: String {
        let full = "\(givenName) \(familyName)".trimmingCharacters(in: .whitespacesAndNewlines)
        return full.isEmpty ? "Unknown" : full
    }

    var primaryPhoneNumber: String? {
        phoneNumbers.first
    }
}

private struct AddFriendContactsSheetView: View {
    @Binding var isPresented: Bool

    @EnvironmentObject private var linkVM: LinkViewModel

    @State private var authorizationStatus: CNAuthorizationStatus = CNContactStore.authorizationStatus(for: .contacts)
    @State private var isLoadingContacts: Bool = false
    @State private var contacts: [InviteContact] = []
    @State private var loadError: String? = nil

    @State private var activeMessageDraft: MessageDraft? = nil
    @State private var showMessagesUnavailableAlert: Bool = false
    @State private var showInviteLinkUnavailableAlert: Bool = false

    private struct MessageDraft: Identifiable, Hashable {
        let id = UUID()
        let recipients: [String]
        let body: String
    }

    private var inviteLinkString: String? {
        if case .shareReady(let url) = linkVM.state {
            return url.absoluteString
        }
        return nil
    }

    var body: some View {
        NavigationStack {
            Group {
                switch authorizationStatus {
                case .authorized:
                    contentList
                case .limited:
                    contentList
                case .notDetermined:
                    loadingView(title: "Requesting Contacts…")
                case .denied, .restricted:
                    deniedView
                @unknown default:
                    deniedView
                }
            }
            .navigationTitle("Add Friend")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { isPresented = false }
                        .font(.system(size: 16, weight: .semibold))
                }
            }
        }
        .task {
            // Kick link generation (this is the same link shown in Settings)
            await linkVM.ensureInviteReady()

            await refreshAuthorizationStatus()
            if authorizationStatus == .notDetermined {
                await requestContactsAccess()
            }
            if authorizationStatus == .authorized {
                await loadContacts()
            }
        }
        .sheet(item: $activeMessageDraft) { draft in
            MessageComposeView(
                recipients: draft.recipients,
                body: draft.body,
                onFinish: { activeMessageDraft = nil }
            )
        }
        .alert("Messages not available", isPresented: $showMessagesUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("This device can’t send text messages.")
        }
        .alert("Invite link not ready", isPresented: $showInviteLinkUnavailableAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text("Open Settings → Link Partner to generate your invite link, then try again.")
        }
    }

    private var contentList: some View {
        List {
            Section {
                switch linkVM.state {
                case .creating, .accepting, .unlinking:
                    HStack {
                        ProgressView()
                        Text("Preparing your link…")
                            .foregroundStyle(.secondary)
                    }
                case .linked:
                    Text("You’re already connected. Unlink in Settings to generate a new invite link.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                case .shareReady:
                    EmptyView()
                case .idle, .unlinked, .error:
                    HStack {
                        ProgressView()
                        Text("Preparing your link…")
                            .foregroundStyle(.secondary)
                    }
                }
            }

            if isLoadingContacts {
                Section {
                    HStack {
                        Spacer()
                        ProgressView("Loading contacts…")
                        Spacer()
                    }
                }
            } else if let loadError {
                Section {
                    Text(loadError)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }
            } else {
                Section {
                    ForEach(contacts) { contact in
                        HStack(spacing: 12) {
                            avatar(for: contact)

                            VStack(alignment: .leading, spacing: 2) {
                                Text(contact.displayName)
                                    .font(.system(size: 16, weight: .regular))
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                                if let phone = contact.primaryPhoneNumber {
                                    Text(phone)
                                        .font(.system(size: 13, weight: .regular))
                                        .foregroundStyle(.secondary)
                                        .lineLimit(1)
                                }
                            }

                            Spacer()

                            Button(action: { invite(contact) }) {
                                HStack(spacing: 8) {
                                    Text("Invite")
                                        .font(.system(size: 14, weight: .semibold))
                                    Spacer(minLength: 0)
                                }
                                .padding(.horizontal, 12)
                                .padding(.vertical, 8)
                                .frame(width: 92)
                                .background(
                                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                                        .fill(Color(red: 0.4, green: 0.2, blue: 0.6).opacity(0.12))
                                )
                            }
                            .buttonStyle(.plain)
                            .disabled(contact.primaryPhoneNumber == nil || inviteLinkString == nil)
                        }
                        .padding(.vertical, 4)
                    }
                }
            }
        }
        .listStyle(.insetGrouped)
    }

    private func avatar(for contact: InviteContact) -> some View {
        Group {
            if let data = contact.thumbnailImageData, let image = UIImage(data: data) {
                Image(uiImage: image)
                    .resizable()
                    .scaledToFill()
            } else {
                ZStack {
                    Circle()
                        .fill(Color(.systemGray5))
                    Image(systemName: "person.fill")
                        .font(.system(size: 14, weight: .semibold))
                        .foregroundStyle(.secondary)
                }
            }
        }
        .frame(width: 40, height: 40)
        .clipShape(Circle())
    }

    private var deniedView: some View {
        VStack(spacing: 12) {
            Text("Contacts access is off.")
                .font(.system(size: 18, weight: .semibold))
            Text("Enable Contacts in Settings to invite friends.")
                .font(.system(size: 14))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            Button("Open Settings") {
                if let url = URL(string: UIApplication.openSettingsURLString) {
                    UIApplication.shared.open(url)
                }
            }
            .font(.system(size: 16, weight: .semibold))
            .padding(.top, 6)
        }
        .padding(20)
    }

    private func loadingView(title: String) -> some View {
        VStack(spacing: 12) {
            ProgressView()
            Text(title)
                .font(.system(size: 14))
                .foregroundStyle(.secondary)
        }
        .padding(20)
    }

    private func invite(_ contact: InviteContact) {
        guard MFMessageComposeViewController.canSendText() else {
            showMessagesUnavailableAlert = true
            return
        }
        guard let phone = contact.primaryPhoneNumber else { return }
        guard let link = inviteLinkString else {
            showInviteLinkUnavailableAlert = true
            return
        }

        let body = """
Hey! I just discovered TalkToMe and I want to connect. It's a new app to foster good-quality communication, one meaningful conversation at a time.

Here's the link to connect with my profile: \(link)
"""

        activeMessageDraft = MessageDraft(recipients: [phone], body: body)
    }

    private func refreshAuthorizationStatus() async {
        let status = CNContactStore.authorizationStatus(for: .contacts)
        await MainActor.run { authorizationStatus = status }
    }

    private func requestContactsAccess() async {
        let store = CNContactStore()
        let granted: Bool = await withCheckedContinuation { continuation in
            store.requestAccess(for: .contacts) { ok, _ in
                continuation.resume(returning: ok)
            }
        }
        await refreshAuthorizationStatus()
        if !granted {
            await MainActor.run {
                loadError = "Contacts permission was not granted."
            }
        }
    }

    private func loadContacts() async {
        await MainActor.run {
            isLoadingContacts = true
            loadError = nil
        }

        let store = CNContactStore()
        let keys: [CNKeyDescriptor] = [
            CNContactIdentifierKey as CNKeyDescriptor,
            CNContactGivenNameKey as CNKeyDescriptor,
            CNContactFamilyNameKey as CNKeyDescriptor,
            CNContactThumbnailImageDataKey as CNKeyDescriptor,
            CNContactPhoneNumbersKey as CNKeyDescriptor
        ]

        let request = CNContactFetchRequest(keysToFetch: keys)
        request.unifyResults = true
        request.sortOrder = .userDefault

        var results: [InviteContact] = []
        do {
            try store.enumerateContacts(with: request) { contact, _ in
                let phones = contact.phoneNumbers
                    .map { $0.value.stringValue }
                    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }

                // Only show contacts that can actually be invited via SMS
                if phones.isEmpty { return }

                results.append(
                    InviteContact(
                        id: contact.identifier,
                        givenName: contact.givenName,
                        familyName: contact.familyName,
                        thumbnailImageData: contact.thumbnailImageData,
                        phoneNumbers: phones
                    )
                )
            }

            await MainActor.run {
                contacts = results
                isLoadingContacts = false
            }
        } catch {
            await MainActor.run {
                isLoadingContacts = false
                loadError = error.localizedDescription
            }
        }
    }
}

private struct MessageComposeView: UIViewControllerRepresentable {
    let recipients: [String]
    let body: String
    let onFinish: () -> Void

    final class Coordinator: NSObject, MFMessageComposeViewControllerDelegate {
        let onFinish: () -> Void

        init(onFinish: @escaping () -> Void) {
            self.onFinish = onFinish
        }

        func messageComposeViewController(_ controller: MFMessageComposeViewController, didFinishWith result: MessageComposeResult) {
            onFinish()
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(onFinish: onFinish)
    }

    func makeUIViewController(context: Context) -> MFMessageComposeViewController {
        let vc = MFMessageComposeViewController()
        vc.messageComposeDelegate = context.coordinator
        vc.recipients = recipients
        vc.body = body
        return vc
    }

    func updateUIViewController(_ uiViewController: MFMessageComposeViewController, context: Context) {
        // Ensure the message is always prefilled even if SwiftUI updates state around presentation timing.
        if uiViewController.recipients != recipients {
            uiViewController.recipients = recipients
        }
        if uiViewController.body != body {
            uiViewController.body = body
        }
    }
}

#if DEBUG
private struct SidebarView_PreviewHost: View {
    @StateObject private var navigationVM = SidebarNavigationViewModel()
    @StateObject private var sessionsVM = ChatSessionsViewModel()
    @StateObject private var linkVM = LinkViewModel.preview(state: .linked)

    @Namespace private var profileNamespace
    @State private var isOpen: Bool = true

    var body: some View {
        SidebarView(isOpen: $isOpen, profileNamespace: profileNamespace)
            .environmentObject(navigationVM)
            .environmentObject(sessionsVM)
            .environmentObject(linkVM)
            .onAppear {
                sessionsVM.setNavigationViewModel(navigationVM)
                sessionsVM.setLinkViewModel(linkVM)
                navigationVM.isOpen = true
            }
            .task {
                if sessionsVM.sessions.isEmpty {
                    sessionsVM.sessions = sampleSessions
                    sessionsVM.unreadPartnerSessionIds = [sampleSessions[0].id]
                }
            }
    }

    private var sampleSessions: [ChatSession] {
        [
            ChatSession(
                id: UUID(),
                title: "Casual conversation",
                lastUsedISO8601: "2025-11-27T12:34:56Z",
                lastMessageContent: "Hey"
            ),
            ChatSession(
                id: UUID(),
                title: "Therapy notes",
                lastUsedISO8601: "2025-11-23T09:15:02Z",
                lastMessageContent: "Can we talk about what happened this week?"
            ),
            ChatSession(
                id: UUID(),
                title: "New Chat",
                lastUsedISO8601: nil,
                lastMessageContent: nil
            )
        ]
    }
}

#Preview("SidebarView") {
    SidebarView_PreviewHost()
}
#endif
